// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gutil/strings/substitute.h"
#include "olap/compaction.h"
#include "olap/rowset/rowset_factory.h"
#include "util/time.h"

using std::vector;

namespace doris {

Compaction::Compaction(TabletSharedPtr tablet)
    : _tablet(tablet),
      _input_rowsets_size(0),
      _input_row_num(0),
      _state(CompactionState::INITED)
{ }

Compaction::~Compaction() {}

OLAPStatus Compaction::do_compaction() {
    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->full_name();

    OlapStopWatch watch;

    // 1. prepare input and output parameters
    int64_t segments_num = 0;
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        segments_num += rowset->num_segments();
    }
    _output_version = Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());
    _tablet->compute_version_hash_from_rowsets(_input_rowsets, &_output_version_hash);

    RETURN_NOT_OK(construct_output_rowset_writer());
    RETURN_NOT_OK(construct_input_rowset_readers());

    // 2. write merged rows to output rowset
    Merger::Statistics stats;
    auto res = Merger::merge_rowsets(_tablet, compaction_type(), _input_rs_readers, _output_rs_writer.get(), &stats);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to do " << compaction_name()
                     << ". res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", output_version=" << _output_version.first
                     << "-" << _output_version.second;
        return res;
    }

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << ", output_version=" << _output_version.first
                     << "-" << _output_version.second;
        return OLAP_ERR_MALLOC_ERROR;
    }

    // 3. check correctness
    RETURN_NOT_OK(check_correctness(stats));

    // 4. modify rowsets in memory
    RETURN_NOT_OK(modify_rowsets());

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else {
        _tablet->set_last_base_compaction_success_time(now);
    } 

    LOG(INFO) << "succeed to do " << compaction_name()
              << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version.first
              << "-" << _output_version.second
              << ", segments=" << segments_num
              << ". elapsed time=" << watch.get_elapse_second() << "s.";

    return OLAP_SUCCESS;
}

OLAPStatus Compaction::construct_output_rowset_writer() {
    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.rowset_type = StorageEngine::instance()->compaction_rowset_type();
    context.rowset_path_prefix = _tablet->tablet_path();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = _output_version;
    context.version_hash = _output_version_hash;
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(context, &_output_rs_writer));
    return OLAP_SUCCESS;
}

OLAPStatus Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_NOT_OK(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return OLAP_SUCCESS;
}

OLAPStatus Compaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    
    WriteLock wrlock(_tablet->get_header_lock_ptr());
    OLAPStatus res = _tablet->modify_rowsets(output_rowsets, _input_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to replace data sources. res" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", compaction__version=" << _output_version.first
                   << "-" << _output_version.second;
        return res;
    }

    res = _tablet->save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet meta. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", compaction_version=" << _output_version.first
                   << "-" << _output_version.second;
        return OLAP_ERR_BE_SAVE_HEADER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus Compaction::gc_unused_rowsets() {
    StorageEngine* storage_engine = StorageEngine::instance();
    if (_state != CompactionState::SUCCESS) {
        storage_engine->add_unused_rowset(_output_rowset);
        return OLAP_SUCCESS;
    }
    for (auto& rowset : _input_rowsets) {
        storage_engine->add_unused_rowset(rowset);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Compaction::check_version_continuity(const vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        RowsetSharedPtr rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                << "prev_rowset verison=" << prev_rowset->start_version()
                << "-" << prev_rowset->end_version()
                << ", rowset version=" << rowset->start_version()
                << "-" << rowset->end_version();
            return OLAP_ERR_CUMULATIVE_MISS_VERSION;
        }
        prev_rowset = rowset;
    }

    return OLAP_SUCCESS;
}

OLAPStatus Compaction::check_correctness(const Merger::Statistics& stats) {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(FATAL) << "row_num does not match between cumulative input and output! "
                   << "input_row_num=" << _input_row_num
                   << ", merged_row_num=" << stats.merged_rows
                   << ", filted_row_num=" << stats.filtered_rows
                   << ", output_row_num=" << _output_rowset->num_rows();
        return OLAP_ERR_CHECK_LINES_ERROR;
    }

    return OLAP_SUCCESS;
}

}  // namespace doris
