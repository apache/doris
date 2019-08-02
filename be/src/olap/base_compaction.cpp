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

#include "olap/base_compaction.h"

#include <algorithm>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "olap/delete_handler.h"
#include "olap/rowset/column_data.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/rowset/segment_group.h"
#include "olap/tablet.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"

using std::list;
using std::map;
using std::string;
using std::vector;

namespace doris {

static bool rowset_comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
    return left->end_version() < right->end_version();
}

BaseCompaction::BaseCompaction(TabletSharedPtr tablet)
    : _tablet(tablet),
      _base_locked(false),
      _input_rowsets_size(0),
      _input_row_num(0)
    { }

BaseCompaction::~BaseCompaction() {
    if (_base_locked) {
        _tablet->release_base_compaction_lock();
    }
}

OLAPStatus BaseCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (!_tablet->try_base_compaction_lock()) {
        LOG(WARNING) << "another base compaction is running. tablet=" << _tablet->full_name();
        return OLAP_ERR_BE_TRY_BE_LOCK_ERROR;
    }

    _base_locked = true; 

    // 1. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());

    // 2. do base compaction, merge rowsets
    RETURN_NOT_OK(do_base_compaction());

    // 3. set base state to success
    _base_state = BaseCompactionState::SUCCESS;

    // 4. garbage collect input rowsets after base compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    _tablet->pick_candicate_rowsets_to_base_compaction(&_input_rowsets);
    if (_input_rowsets.size() == 0 || _input_rowsets.size() == 1) {
        LOG(INFO) << "There is no enough rowsets to cumulative compaction."
                  << ", the size of rowsets to compact=" << _input_rowsets.size()
                  << ", cumulative_point=" << _tablet->cumulative_layer_point();
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(_input_rowsets.begin(), _input_rowsets.end(), rowset_comparator);
    RETURN_NOT_OK(check_version_continuity(_input_rowsets));

    // 1. cumulative rowset must reach base_compaction_num_cumulative_deltas threshold
    if (_input_rowsets.size() > config::base_compaction_num_cumulative_deltas) {
        LOG(INFO) << "satisfy the base compaction policy. tablet="<< _tablet->full_name()
                  << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                  << ", base_compaction_num_cumulative_rowsets=" << config::base_compaction_num_cumulative_deltas;
        return OLAP_SUCCESS;
    }

    // 2. the ratio between base rowset and all input cumulative rowsets reachs the threshold
    int64_t base_size = 0;
    int64_t cumulative_total_size = 0;
    for (auto& rowset : _input_rowsets) {
        if (rowset->start_version() != 0) {
            cumulative_total_size += rowset->data_disk_size();
        } else {
            base_size = rowset->data_disk_size();
        }
    }

    double base_cumulative_delta_ratio = config::base_cumulative_delta_ratio;
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;

    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", cumualtive_total_size=" << cumulative_total_size
                  << ", base_size=" << base_size
                  << ", cumulative_base_ratio=" << cumulative_base_ratio
                  << ", policy_ratio=" << base_cumulative_delta_ratio;
        return OLAP_SUCCESS;
    }

    // 3. the interval since last base compaction reachs the threshold
    int64_t base_creation_time = _input_rowsets[0]->creation_time();
    int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_base_compaction = time(NULL) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction 
                   << ", interval_threshold=" << interval_threshold;
        return OLAP_SUCCESS;
    }

    LOG(INFO) << "don't satisfy the base compaction policy. tablet=" << _tablet->full_name()
              << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
              << ", cumulative_base_ratio=" << cumulative_base_ratio
              << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction;
    return OLAP_ERR_BE_NO_SUITABLE_VERSION;
}

OLAPStatus BaseCompaction::do_base_compaction() {
    DorisMetrics::base_compaction_deltas_total.increment(_input_rowsets.size());
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
    }
    DorisMetrics::base_compaction_bytes_total.increment(_input_rowsets_size);

    OlapStopWatch watch;

    // 1. prepare cumulative_version and cumulative_version
    _base_version = Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());
    _tablet->compute_version_hash_from_rowsets(_input_rowsets, &_base_version_hash);

    RETURN_NOT_OK(construct_output_rowset_writer());
    RETURN_NOT_OK(construct_input_rowset_readers());

    Merger merger(_tablet, READER_BASE_COMPACTION, _output_rs_writer, _input_rs_readers);
    OLAPStatus res = merger.merge();

    // 3. 如果merge失败，执行清理工作，返回错误码退出
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to do base compaction. res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", version=" << _base_version.first
                     << "-" << _base_version.second;
        return OLAP_ERR_BE_MERGE_ERROR;
    }

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << _output_rs_writer->version().first
                     << "-" << _output_rs_writer->version().second;
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

OLAPStatus BaseCompaction::construct_output_rowset_writer() {
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
    context.version = _base_version;
    context.version_hash = _base_version_hash;

    _output_rs_writer.reset(new (std::nothrow)AlphaRowsetWriter());
    RETURN_NOT_OK(_output_rs_writer->init(context));
    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader(rowset->create_reader());
        if (rs_reader == nullptr) {
            LOG(WARNING) << "rowset create reader failed. rowset:" <<  rowset->rowset_id();
            return OLAP_ERR_ROWSET_CREATE_READER;
        }
        _input_rs_readers.push_back(rs_reader);
    }
    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::save_meta() {
    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    OLAPStatus res = _tablet->modify_rowsets(output_rowsets, _input_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to replace data sources. res" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", new_base_version=" << _base_version.second
                   << ", old_base_version=" << _input_rowsets[0]->end_version();
        return res;
    }

    res = _tablet->save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet meta. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", new_base_version=" << _base_version.second
                   << ", old_base_version=" << _input_rowsets[0]->end_version();
        return OLAP_ERR_BE_SAVE_HEADER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::gc_unused_rowsets() {
    StorageEngine* storage_engine = StorageEngine::instance();
    if (_base_state == BaseCompactionState::FAILED) {
        storage_engine->add_unused_rowset(_output_rowset);
        return OLAP_SUCCESS;
    }
    for (auto& rowset : _input_rowsets) {
        storage_engine->add_unused_rowset(rowset);
    }
    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::check_version_continuity(const vector<RowsetSharedPtr>& rowsets) {
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

OLAPStatus BaseCompaction::check_correctness(const Merger& merger) {
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
