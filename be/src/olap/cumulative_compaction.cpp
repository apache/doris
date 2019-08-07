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

#include "olap/cumulative_compaction.h"

#include <algorithm>
#include <list>
#include <vector>

#include "olap/storage_engine.h"
#include "util/doris_metrics.h"
#include "olap/rowset/alpha_rowset_writer.h"

using std::list;
using std::nothrow;
using std::sort;
using std::vector;

namespace doris {

static bool rowset_comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
    return left->end_version() < right->end_version();
}

CumulativeCompaction::CumulativeCompaction(TabletSharedPtr tablet)
    : _tablet(tablet),
      _cumulative_rowset_size_threshold(config::cumulative_compaction_budgeted_bytes),
      _input_rowsets_size(0),
      _input_row_num(0),
      _cumulative_state(CumulativeState::FAILED)
    { }

CumulativeCompaction::~CumulativeCompaction() {
    if (_cumulative_locked) {
        _tablet->release_cumulative_lock();
    }
}

OLAPStatus CumulativeCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    if (!_tablet->try_cumulative_lock()) {
        LOG(INFO) << "The tablet is under cumulative compaction. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    _cumulative_locked = true;

    // 1. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());

    // 2. do cumulative compaction, merge rowsets
    RETURN_NOT_OK(do_cumulative_compaction());

    _cumulative_state = CumulativeState::SUCCESS;
    
    // 3. garbage collect input rowsets after cumulative compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.size() == 0 || candidate_rowsets.size() == 1) {
        LOG(INFO) << "There is no enough rowsets to cumulative compaction."
                  << ", the size of rowsets to compact=" << candidate_rowsets.size()
                  << ", cumulative_point=" << _tablet->cumulative_layer_point();
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), rowset_comparator);
    RETURN_NOT_OK(check_version_continuity(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    for (size_t i = 0; i < candidate_rowsets.size(); ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            if (transient_rowsets.size() > _input_rowsets.size()) {
                _input_rowsets = transient_rowsets;
            }
            transient_rowsets.clear();
            continue;
        }

        transient_rowsets.push_back(rowset); 
    }

    if (transient_rowsets.size() > _input_rowsets.size()) {
        _input_rowsets = transient_rowsets;
    }
		
    if (_input_rowsets.empty()) {
        _tablet->set_cumulative_layer_point(candidate_rowsets.back()->end_version() + 1);
    } else {
        _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);
    }

    if (_input_rowsets.size() == 1 || _input_rowsets.size() == 0) {
        LOG(INFO) << "There is no enough rowsets to cumulative compaction."
                  << ", the size of rowsets to compact=" << candidate_rowsets.size()
                  << ", cumulative_point=" << _tablet->cumulative_layer_point();
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
    }
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::do_cumulative_compaction() {
    LOG(INFO) << "start cumulative compaction. tablet=" << _tablet->full_name();

    DorisMetrics::cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::cumulative_compaction_bytes_total.increment(_input_rowsets_size);
    OlapStopWatch watch;

    // 1. prepare cumulative_version and cumulative_version
    _cumulative_version = Version(_input_rowsets.front()->start_version(),
                                  _input_rowsets.back()->end_version());
    _tablet->compute_version_hash_from_rowsets(_input_rowsets, &_cumulative_version_hash);
    RETURN_NOT_OK(construct_output_rowset_writer());
    RETURN_NOT_OK(construct_input_rowset_readers());

    Merger merger(_tablet, READER_CUMULATIVE_COMPACTION, _output_rs_writer, _input_rs_readers);

    // 2. merge input rowsets to output rowset
    OLAPStatus res = merger.merge();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do cumulative merge."
                     << " tablet=" << _tablet->full_name()
                     << ", cumulative_version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        return res;
    }

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << _output_rs_writer->version().first << "-"
                     << _output_rs_writer->version().second;
        return OLAP_ERR_MALLOC_ERROR;
    }


    // 3. check correctness
    RETURN_NOT_OK(check_correctness(merger));

    // 4. save meta to remote store and local store 
    RETURN_NOT_OK(save_meta());
    
    // 5. modify rowsets in memory
    RETURN_NOT_OK(modify_rowsets());

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::save_meta() {
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    OLAPStatus res = _tablet->modify_rowsets(output_rowsets, _input_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to modify rowsets. res=" << res
                   << ", tablet=" << _tablet->full_name();
        return res;
    }

    res = _tablet->save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to save meta. res=" << res
                   << ", tablet=" << _tablet->full_name();
        return res;
    }
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::gc_unused_rowsets() {
    StorageEngine* storage_engine = StorageEngine::instance();
    if (_cumulative_state == CumulativeState::FAILED) { 
        storage_engine->add_unused_rowset(_output_rowset);
        return OLAP_SUCCESS;
    }
    for (auto& rowset : _input_rowsets) {
        storage_engine->add_unused_rowset(rowset);
    }
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::construct_output_rowset_writer() {
    _output_rs_writer.reset(new (std::nothrow) AlphaRowsetWriter());
    RowsetId rowset_id = 0;
    RETURN_NOT_OK(_tablet->next_rowset_id(&rowset_id));
    RowsetWriterContext context;
    context.rowset_id = rowset_id;
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.rowset_type = ALPHA_ROWSET;
    context.rowset_path_prefix = _tablet->tablet_path();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.data_dir = _tablet->data_dir();
    context.version = _cumulative_version;
    context.version_hash = _cumulative_version_hash;

    RETURN_NOT_OK(_output_rs_writer->init(context));
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (auto& rowset : rowsets) {
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

OLAPStatus CumulativeCompaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader(rowset->create_reader());
        if (rs_reader == nullptr) {
            LOG(WARNING) << "rowset create reader failed. rowset:" << rowset->rowset_id();
            return OLAP_ERR_ROWSET_CREATE_READER;
        }
        _input_rs_readers.push_back(rs_reader);
    }
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::check_correctness(const Merger& merger) {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + merger.merged_rows() + merger.filted_rows()) {
        LOG(FATAL) << "row_num does not match between cumulative input and output! "
                   << "input_row_num=" << _input_row_num
                   << ", merged_row_num=" << merger.merged_rows()
                   << ", filted_row_num=" << merger.filted_rows()
                   << ", output_row_num=" << _output_rowset->num_rows();
        return OLAP_ERR_CHECK_LINES_ERROR;
    }

    return OLAP_SUCCESS;
}

}  // namespace doris

