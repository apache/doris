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
#include "util/doris_metrics.h"

namespace doris {

CumulativeCompaction::CumulativeCompaction(TabletSharedPtr tablet)
    : Compaction(tablet),
      _cumulative_rowset_size_threshold(config::cumulative_compaction_budgeted_bytes)
{ }

CumulativeCompaction::~CumulativeCompaction() { }

OLAPStatus CumulativeCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    MutexLock lock(_tablet->get_cumulative_lock(), TRY_LOCK);
    if (!lock.own_lock()) {
        LOG(INFO) << "The tablet is under cumulative compaction. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    // 1.calculate cumulative point 
    RETURN_NOT_OK(_tablet->calculate_cumulative_point());

    // 2. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());

    // 3. do cumulative compaction, merge rowsets
    RETURN_NOT_OK(do_compaction());

    // 4. set state to success
    _state = CompactionState::SUCCESS;
    _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);
    
    // 5. garbage collect input rowsets after cumulative compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    DorisMetrics::cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::cumulative_compaction_bytes_total.increment(_input_rowsets_size);

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.size() <= 1) {
        LOG(WARNING) << "There is no enough rowsets to cumulative compaction."
                     << ", the size of rowsets to compact=" << candidate_rowsets.size()
                     << ", cumulative_point=" << _tablet->cumulative_layer_point();
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_NOT_OK(check_version_continuity(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    for (size_t i = 0; i < candidate_rowsets.size() - 1; ++i) {
        // VersionHash will calculated from chosen rowsets.
        // If ultimate singleton rowset is chosen, VersionHash
        // will be different from the value recorded in FE.
        // So the ultimate singleton rowset is revserved.
        RowsetSharedPtr rowset = candidate_rowsets[i];
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            if (transient_rowsets.size() > config::cumulative_compaction_num_singleton_deltas) {
                _input_rowsets = transient_rowsets;
                break;
            }
            transient_rowsets.clear();
            continue;
        }

        transient_rowsets.push_back(rowset); 
    }

    if (transient_rowsets.size() > config::cumulative_compaction_num_singleton_deltas) {
        _input_rowsets = transient_rowsets;
    }
		
    if (_input_rowsets.empty()) {
        // There are no rowsets choosed to do cumulative compaction.
        // Under this circumstance, cumulative_point should be set.
        // Otherwise, the next round will not choose rowsets. 
        _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version());
    }

    if (_input_rowsets.size() <= 1) {
        LOG(WARNING) << "There is no enough rowsets to cumulative compaction."
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

OLAPStatus CumulativeCompaction::do_compaction() {
    LOG(INFO) << "start cumulative compaction. tablet=" << _tablet->full_name();

    OlapStopWatch watch;

    // 1. prepare cumulative_version and cumulative_version
    _output_version = Version(_input_rowsets.front()->start_version(),
                              _input_rowsets.back()->end_version());
    _tablet->compute_version_hash_from_rowsets(_input_rowsets, &_output_version_hash);
    RETURN_NOT_OK(construct_output_rowset_writer());
    RETURN_NOT_OK(construct_input_rowset_readers());

    Merger merger(_tablet, READER_CUMULATIVE_COMPACTION, _output_rs_writer, _input_rs_readers);

    // 2. merge input rowsets to output rowset
    OLAPStatus res = merger.merge();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do cumulative merge. res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", output_version=" << _output_version.first
                     << "-" << _output_version.second;
        return res;
    }

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed."
                     << " output_version=" << _output_version.first
                     << "-" << _output_version.second;
        return OLAP_ERR_MALLOC_ERROR;
    }


    // 3. check correctness
    RETURN_NOT_OK(check_correctness(merger));

    // 4. modify rowsets in memory
    RETURN_NOT_OK(modify_rowsets());

    LOG(INFO) << "succeed to do cumulative compaction. tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version.first
              << "-" << _output_version.second
              << ". elapsed time of doing cumulative compaction"
              << ", time=" << watch.get_elapse_second() << "s";

    return OLAP_SUCCESS;
}

}  // namespace doris

