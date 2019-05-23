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

#include "olap/olap_engine.h"
#include "util/doris_metrics.h"

using std::list;
using std::nothrow;
using std::sort;
using std::vector;


namespace doris {

OLAPStatus CumulativeCompaction::init(OLAPTablePtr table) {
    LOG(INFO) << "init cumulative compaction handler. [table=" << table->full_name() << "]";

    if (_is_init) {
        OLAP_LOG_WARNING("cumulative handler has been inited.[table=%s]",
                         table->full_name().c_str());
        return OLAP_ERR_CUMULATIVE_REPEAT_INIT;
    }

    if (!table->is_loaded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    _table = table;
    _max_delta_file_size = config::cumulative_compaction_budgeted_bytes;

    if (!_table->try_cumulative_lock()) {
        OLAP_LOG_WARNING("another cumulative is running. [table=%s]",
                         _table->full_name().c_str());
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    _obtain_header_rdlock();
    _old_cumulative_layer_point = _table->cumulative_layer_point();
    _release_header_lock();
    // 如果为-1，则该table之前没有设置过cumulative layer point
    // 我们在这里设置一下
    if (_old_cumulative_layer_point == -1) {
        LOG(INFO) << "tablet has an unreasonable cumulative layer point. [tablet='" << _table->full_name()
                  << "' cumulative_layer_point=" << _old_cumulative_layer_point << "]";
        _table->release_cumulative_lock();
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    _obtain_header_wrlock();
    OLAPStatus res = _calculate_need_merged_versions();
    _release_header_lock();
    if (res != OLAP_SUCCESS) {
        _table->release_cumulative_lock();
        LOG(INFO) << "no suitable delta versions. don't do cumulative compaction now.";
        return res;
    }

    if (!_validate_need_merged_versions()) {
        _table->release_cumulative_lock();
        LOG(FATAL) << "error! invalid need merged versions.";
        return OLAP_ERR_CUMULATIVE_INVALID_NEED_MERGED_VERSIONS;
    }

    _is_init = true;
    _cumulative_version = Version(_need_merged_versions.begin()->first,
                                  _need_merged_versions.rbegin()->first);

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::run() {
    if (!_is_init) {
        _table->release_cumulative_lock();
        OLAP_LOG_WARNING("cumulative handler is not inited.");
        return OLAP_ERR_NOT_INITED;
    }

    // 0. 准备工作
    LOG(INFO) << "start cumulative compaction. tablet=" << _table->full_name()
              << ", cumulative_version=" << _cumulative_version.first << "-"
              << _cumulative_version.second;
    OlapStopWatch watch;

    // 1. 计算新的cumulative文件的version hash
    OLAPStatus res = OLAP_SUCCESS;
    res = _table->compute_all_versions_hash(_need_merged_versions, &_cumulative_version_hash);
    if (res != OLAP_SUCCESS) {
        _table->release_cumulative_lock();
        OLAP_LOG_WARNING("failed to computer cumulative version hash. "
                         "[table=%s; cumulative_version=%d-%d]",
                         _table->full_name().c_str(),
                         _cumulative_version.first,
                         _cumulative_version.second);
        return res;
    }

    // 2. 获取待合并的delta文件对应的data文件
    _table->acquire_data_sources_by_versions(_need_merged_versions, &_data_source);
    if (_data_source.size() == 0) {
        _table->release_cumulative_lock();
        OLAP_LOG_WARNING("failed to acquire data source. [table=%s]",
                         _table->full_name().c_str());
        return OLAP_ERR_CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE;
    }

    {
        DorisMetrics::cumulative_compaction_deltas_total.increment(_need_merged_versions.size());
        int64_t merge_bytes = 0;
        for (ColumnData* i_data : _data_source) {
            merge_bytes += i_data->segment_group()->data_size();
        }
        DorisMetrics::cumulative_compaction_bytes_total.increment(merge_bytes);
    }

    do {
        // 3. 生成新cumulative文件对应的olap index
        _new_segment_group = new (nothrow) SegmentGroup(_table.get(),
                                                        _cumulative_version,
                                                        _cumulative_version_hash,
                                                        false, 0, 0);
        if (_new_segment_group == NULL) {
            OLAP_LOG_WARNING("failed to malloc new cumulative olap index. "
                             "[table=%s; cumulative_version=%d-%d]",
                             _table->full_name().c_str(),
                             _cumulative_version.first,
                             _cumulative_version.second);
            break;
        }

        // 4. 执行cumulative compaction合并过程
        res = _do_cumulative_compaction();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to do cumulative compaction. "
                             "[table=%s; cumulative_version=%d-%d]",
                             _table->full_name().c_str(),
                             _cumulative_version.first,
                             _cumulative_version.second);
            break;
        }
    } while (0);

    // 5. 如果出现错误，执行清理工作
    if (res != OLAP_SUCCESS && _new_segment_group != NULL) {
        _new_segment_group->delete_all_files();
        SAFE_DELETE(_new_segment_group);
    }
    
    if (_data_source.size() != 0) {
        _table->release_data_sources(&_data_source);
    }

    _table->release_cumulative_lock();

    VLOG(10) << "elapsed time of doing cumulative compaction. "
             << "time=" << watch.get_elapse_time_us();
    return res;
}

OLAPStatus CumulativeCompaction::_calculate_need_merged_versions() {
    OLAPStatus res = OLAP_SUCCESS;
    
    Versions delta_versions;
    res = _get_delta_versions(&delta_versions);
    if (res != OLAP_SUCCESS) {
        LOG(INFO) << "failed to get delta versions.";
        return res;
    }

    // 此处减1，是为了确保最新版本的delta不会合入到cumulative里
    // 因为push可能会重复导入最新版本的delta
    uint32_t delta_number = delta_versions.size() - 1;
    uint32_t index = 0;
    // 在delta文件中寻找可合并的delta文件
    // 这些delta文件可能被delete或较大的delta文件(>= max_delta_file_size)分割为多个区间, 比如：
    // v1, v2, v3, D, v4, v5, D, v6, v7
    // 我们分区间进行查找直至找到合适的可合并delta文件
    while (index < delta_number) {
        Versions need_merged_versions;
        size_t total_size = 0;

        // 在其中1个区间里查找可以合并的delta文件
        for (; index < delta_number; ++index) {
            // 如果已找到的可合并delta文件大小大于等于_max_delta_file_size，我们认为可以执行合并了
            // 停止查找过程
            if (total_size >= _max_delta_file_size) {
                break;
            }

            Version delta = delta_versions[index];
            size_t delta_size = _table->get_version_data_size(delta);
            // 如果遇到大的delta文件，或delete版本文件，则：
            if (delta_size >= _max_delta_file_size
                    || _table->is_delete_data_version(delta)
                    || _table->is_load_delete_version(delta)) {
                // 1) 如果need_merged_versions为空，表示这2类文件在区间的开头，直接跳过
                if (need_merged_versions.empty()) {
                    continue;
                } else {
                    // 2) 如果need_merged_versions不为空，则已经找到区间的末尾，跳出循环
                    break;
                }
            }

            need_merged_versions.push_back(delta);
            total_size += delta_size;
        }

        // 该区间没有可以合并的delta文件，进行下一轮循环，继续查找下一个区间
        if (need_merged_versions.empty()) {
            continue;
        }

        // 如果该区间中只有一个delta，或者该区间的delta都是空的delta，则我们查看能否与区间末尾的
        // 大delta合并，或者与区间的开头的前一个版本合并
        if (need_merged_versions.size() == 1 || total_size == 0) {
            // 如果区间末尾是较大的delta版, 则与它合并
            if (index < delta_number
                    && _table->get_version_data_size(delta_versions[index]) >=
                           _max_delta_file_size) {
                need_merged_versions.push_back(delta_versions[index]);
                ++index;
            }
            // 如果区间前一个版本可以合并, 则将其加入到可合并版本中
            Version delta_before_interval;
            if (_find_previous_version(need_merged_versions[0], &delta_before_interval)) {
                need_merged_versions.insert(need_merged_versions.begin(),
                                            delta_before_interval); 
            }

            // 如果还是只有1个待合并的delta，则跳过，不进行合并
            if (need_merged_versions.size() == 1) {
                continue;
            }

            _need_merged_versions.swap(need_merged_versions);
            _new_cumulative_layer_point = delta_versions[index].first;
            return OLAP_SUCCESS;
        }

        // 如果有多个可合并文件，则可以进行cumulative compaction的合并过程
        // 如果只有只有一个可合并的文件，为了效率，不触发cumulative compaction的合并过程
        if (need_merged_versions.size() != 1) {
            // 如果在可合并区间开头之前的一个版本的大小没有达到delta文件的最大值，
            // 则将可合并区间的文件合并到之前那个版本上
            Version delta;
            if (_find_previous_version(need_merged_versions[0], &delta)) {
                need_merged_versions.insert(need_merged_versions.begin(), delta); 
            }

            _need_merged_versions.swap(need_merged_versions);
            _new_cumulative_layer_point = delta_versions[index].first;
            return OLAP_SUCCESS;
        }
    }
    
    // 没有找到可以合并的delta文件，无法执行合并过程，但我们仍然需要设置新的cumulative_layer_point
    // 如果不设置新的cumulative_layer_point, 则下次执行cumulative compaction时，扫描的文件和这次
    // 扫描的文件相同，依然找不到可以合并的delta文件, 无法执行合并过程。
    // 依此类推，就进入了死循环状态，永远不会进行cumulative compaction
    _table->set_cumulative_layer_point(delta_versions[index].first);
    _table->save_header();
    return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
}

static bool version_comparator(const Version& lhs, const Version& rhs) {
    return lhs.second < rhs.second;
}

OLAPStatus CumulativeCompaction::_get_delta_versions(Versions* delta_versions) {
    delta_versions->clear();
    
    Versions all_versions;
    _table->list_versions(&all_versions);

    for (Versions::const_iterator version = all_versions.begin();
            version != all_versions.end(); ++version) {
        if (version->first == version->second && version->first >= _old_cumulative_layer_point) {
            delta_versions->push_back(*version);
        }
    }

    if (delta_versions->size() == 0) {
        LOG(INFO) << "no delta versions. cumulative_point=" << _old_cumulative_layer_point;
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    // 如果size等于1，则这个delta一定是cumulative point所指向的delta
    // 因为我们总是保留最新的delta不合并到cumulative文件里，所以此时应该返回错误，不进行cumulative
    if (delta_versions->size() == 1) {
        delta_versions->clear();
        VLOG(10) << "only one delta version. no new delta versions. "
                 << ", cumulative_point=" << _old_cumulative_layer_point;
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    sort(delta_versions->begin(), delta_versions->end(), version_comparator);

    // can't do cumulative expansion if there has a hole
    Versions versions_path;
    OLAPStatus select_status = _table->select_versions_to_span(
        Version(delta_versions->front().first, delta_versions->back().second), &versions_path);
    if (select_status != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("can't do cumulative expansion if fail to select shortest version path. "
                         "[table=%s start=%d; end=%d]",
                         _table->full_name().c_str(),
                         delta_versions->front().first, delta_versions->back().second);
        return  select_status;
    }

    return OLAP_SUCCESS;
}

bool CumulativeCompaction::_find_previous_version(const Version current_version,
                                               Version* previous_version) {
    Versions all_versions;
    if (OLAP_SUCCESS != _table->select_versions_to_span(Version(0, current_version.second),
                                                        &all_versions)) {
        OLAP_LOG_WARNING("fail to select shortest version path. [start=%d; end=%d]",
                         0, current_version.second);
        return  false;
    }

    // previous_version.second应该等于current_version.first - 1
    for (Versions::const_iterator version = all_versions.begin();
            version != all_versions.end(); ++version) {
        // Skip base version
        if (version->first == 0) {
            continue;
        }

        if (version->second == current_version.first - 1) {
            if (_table->is_delete_data_version(*version)
                    || _table->is_load_delete_version(*version)) {
                return false;
            }

            size_t data_size = _table->get_version_data_size(*version);
            if (data_size >= _max_delta_file_size) {
                return false;
            }

            *previous_version = *version;
            return true;
        }
    }

    return false;
}

OLAPStatus CumulativeCompaction::_do_cumulative_compaction() {
    OLAPStatus res = OLAP_SUCCESS;
    OlapStopWatch watch;
    Merger merger(_table, _new_segment_group, READER_CUMULATIVE_COMPACTION);

    // 1. merge delta files into new cumulative file
    uint64_t merged_rows = 0;
    uint64_t filted_rows = 0;
    res = merger.merge(_data_source, &merged_rows, &filted_rows);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do cumulative merge. [table=%s; cumulative_version=%d-%d]",
                         _table->full_name().c_str(),
                         _cumulative_version.first,
                         _cumulative_version.second);
        return res;
    }

    // 2. load new cumulative file
    res = _new_segment_group->load();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to load cumulative index. [table=%s; cumulative_version=%d-%d]",
                         _table->full_name().c_str(),
                         _cumulative_version.first,
                         _cumulative_version.second);
        return res;
    }

    // Check row num changes
    uint64_t source_rows = 0;
    for (ColumnData* i_data : _data_source) {
        source_rows += i_data->segment_group()->num_rows();
    }
    bool row_nums_check = config::row_nums_check;
    if (row_nums_check) {
        if (source_rows != _new_segment_group->num_rows() + merged_rows + filted_rows) {
            LOG(FATAL) << "fail to check row num! "
                       << "source_rows=" << source_rows
                       << ", merged_rows=" << merged_rows
                       << ", filted_rows=" << filted_rows
                       << ", new_index_rows=" << _new_segment_group->num_rows();
            return OLAP_ERR_CHECK_LINES_ERROR;
        }
    } else {
        LOG(INFO) << "all row nums. source_rows=" << source_rows
                  << ", merged_rows=" << merged_rows
                  << ", filted_rows=" << filted_rows
                  << ", new_index_rows=" << _new_segment_group->num_rows()
                  << ", merged_version_num=" << _need_merged_versions.size()
                  << ", time_us=" << watch.get_elapse_time_us();
    }

    // 3. add new cumulative file into table
    vector<SegmentGroup*> unused_indices;
    _obtain_header_wrlock();
    res = _update_header(&unused_indices);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to update header for new cumulative."
                         "[table=%s; cumulative_version=%d-%d]",
                         _table->full_name().c_str(),
                         _cumulative_version.first,
                         _cumulative_version.second);
        _release_header_lock();
        return res;
    }

    // 4. validate that delete action is right
    res = _validate_delete_file_action();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "delete action of cumulative compaction has error. roll back."
                   << "tablet=" << _table->full_name()
                   << ", cumulative_version=" << _cumulative_version.first 
                   << "-" << _cumulative_version.second;
        // if error happened, roll back
        OLAPStatus ret = _roll_back(unused_indices);
        if (ret != OLAP_SUCCESS) {
            LOG(FATAL) << "roll back failed. [table=" <<  _table->full_name() << "]";
        }

        _release_header_lock();
        return res;
    }
    // 5. 如果合并成功，设置新的cumulative_layer_point
    _table->set_cumulative_layer_point(_new_cumulative_layer_point);
    _table->save_header();
    _release_header_lock();

    // 6. delete delta files which have been merged into new cumulative file
    _delete_unused_delta_files(&unused_indices);

    LOG(INFO) << "succeed to do cumulative compaction. tablet=" << _table->full_name()
              << ", cumulative_version=" << _cumulative_version.first << "-"
              << _cumulative_version.second;
    return res;
}

OLAPStatus CumulativeCompaction::_update_header(vector<SegmentGroup*>* unused_indices) {
    vector<SegmentGroup*> new_indices;
    new_indices.push_back(_new_segment_group);

    OLAPStatus res = OLAP_SUCCESS;
    res = _table->replace_data_sources(&_need_merged_versions, &new_indices, unused_indices);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to replace data sources. res=" << res
                   << ", tablet=" << _table->full_name();
        return res;
    }

    res = _table->save_header();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to save header. res=" << res
                   << ", tablet=" << _table->full_name();
        return res;
    }

    return res;
}

void CumulativeCompaction::_delete_unused_delta_files(vector<SegmentGroup*>* unused_indices) {
    if (!unused_indices->empty()) {
        OLAPEngine* unused_index = OLAPEngine::get_instance();

        for (vector<SegmentGroup*>::iterator it = unused_indices->begin();
                it != unused_indices->end(); ++it) {
            unused_index->add_unused_index(*it);
        }
    }
}

bool CumulativeCompaction::_validate_need_merged_versions() {
    // 1. validate versions in _need_merged_versions are continuous
    // Skip the first element
    for (unsigned int index = 1; index < _need_merged_versions.size(); ++index) {
        Version previous_version = _need_merged_versions[index - 1];
        Version current_version = _need_merged_versions[index];
        if (current_version.first != previous_version.second + 1) {
            OLAP_LOG_WARNING("wrong need merged version. "
                             "previous_version=%d-%d; current_version=%d-%d",
                             previous_version.first, previous_version.second,
                             current_version.first, current_version.second);
            return false;
        }
    }

    return true;
}

OLAPStatus CumulativeCompaction::_validate_delete_file_action() {
    // 1. acquire the new cumulative version to make sure that all is right after deleting files
    Version test_version = Version(0, _cumulative_version.second);
    vector<ColumnData*> test_sources;
    _table->acquire_data_sources(test_version, &test_sources);
    if (test_sources.size() == 0) {
        OLAP_LOG_WARNING("acquire data source failed. [test_verison=%d-%d]",
                         test_version.first, test_version.second);
        return OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION;
    }

    _table->release_data_sources(&test_sources);
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::_roll_back(const vector<SegmentGroup*>& old_olap_indices) {
    vector<Version> need_remove_version;
    need_remove_version.push_back(_cumulative_version);
    // unused_indices will only contain new cumulative index
    // we don't need to delete it here; we will delete new cumulative index in the end.
    vector<SegmentGroup*> unused_indices;

    OLAPStatus res = OLAP_SUCCESS;
    res = _table->replace_data_sources(&need_remove_version, &old_olap_indices, &unused_indices);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to replace data sources. [table=" << _table->full_name() << "]";
        return res;
    }

    res = _table->save_header();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to save header. [table=" << _table->full_name() << "]";
        return res;
    }

    return res;    
}

}  // namespace doris

