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
#include "util/doris_metrics.h"

namespace doris {

BaseCompaction::BaseCompaction(TabletSharedPtr tablet)
    : Compaction(tablet)
{ }

BaseCompaction::~BaseCompaction() { }

OLAPStatus BaseCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    MutexLock lock(_tablet->get_base_lock(), TRY_LOCK);
    if (!lock.own_lock()) {
        LOG(WARNING) << "another base compaction is running. tablet=" << _tablet->full_name();
        return OLAP_ERR_BE_TRY_BE_LOCK_ERROR;
    }

    // 1. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());

    // 2. do base compaction, merge rowsets
    RETURN_NOT_OK(do_compaction());

    // 3. set state to success
    _state = CompactionState::SUCCESS;

    // 4. garbage collect input rowsets after base compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    // 5. add metric to base compaction
    DorisMetrics::base_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::base_compaction_bytes_total.increment(_input_rowsets_size);

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    _tablet->pick_candicate_rowsets_to_base_compaction(&_input_rowsets);
    if (_input_rowsets.size() <= 1) {
        LOG(WARNING) << "There is no enough rowsets to do base compaction."
                     << ", the size of rowsets to compact=" << _input_rowsets.size()
                     << ", cumulative_point=" << _tablet->cumulative_layer_point();
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(_input_rowsets.begin(), _input_rowsets.end(), Rowset::comparator);
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

}  // namespace doris
