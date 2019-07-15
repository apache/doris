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

OLAPStatus CumulativeCompaction::init(TabletSharedPtr tablet) {
    LOG(INFO) << "init cumulative compaction handler. tablet=" << tablet->full_name();

    if (_is_init) {
        LOG(WARNING) << "cumulative handler has been inited. tablet=" << tablet->full_name();
        return OLAP_ERR_CUMULATIVE_REPEAT_INIT;
    }

    if (!tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    _tablet = tablet;
    _max_delta_file_size = config::cumulative_compaction_budgeted_bytes;

    if (!_tablet->try_cumulative_lock()) {
        LOG(INFO) << "skip compaction, because another cumulative is running. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    _tablet->obtain_header_rdlock();
    _old_cumulative_layer_point = _tablet->cumulative_layer_point();
    _tablet->release_header_lock();
    // 如果为-1，则该table之前没有设置过cumulative layer point
    // 我们在这里设置一下
    if (_old_cumulative_layer_point == -1) {
        LOG(INFO) << "tablet has an unreasonable cumulative layer point. tablet=" << _tablet->full_name()
                  << ", cumulative_layer_point=" << _old_cumulative_layer_point;
        _tablet->release_cumulative_lock();
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    _tablet->obtain_header_wrlock();
    OLAPStatus res = _calculate_need_merged_versions();
    _tablet->release_header_lock();
    if (res != OLAP_SUCCESS) {
        _tablet->release_cumulative_lock();
        LOG(INFO) << "no suitable delta versions. don't do cumulative compaction now.";
        return res;
    }

    if (!_validate_need_merged_versions()) {
        _tablet->release_cumulative_lock();
        LOG(FATAL) << "error! invalid need merged versions.";
        return OLAP_ERR_CUMULATIVE_INVALID_NEED_MERGED_VERSIONS;
    }

    _is_init = true;
    _cumulative_version = Version(_need_merged_versions.begin()->first,
                                  _need_merged_versions.rbegin()->first);
    _rs_writer.reset(new (std::nothrow)AlphaRowsetWriter());
    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::run() {
    if (!_is_init) {
        _tablet->release_cumulative_lock();
        LOG(WARNING) << "cumulative handler is not inited.";
        return OLAP_ERR_NOT_INITED;
    }

    // 0. 准备工作
    LOG(INFO) << "start cumulative compaction. tablet=" << _tablet->full_name()
              << ", cumulative_version=" << _cumulative_version.first << "-"
              << _cumulative_version.second;
    OlapStopWatch watch;

    // 1. 计算新的cumulative文件的version hash
    OLAPStatus res = OLAP_SUCCESS;
    res = _tablet->compute_all_versions_hash(_need_merged_versions, &_cumulative_version_hash);
    if (res != OLAP_SUCCESS) {
        _tablet->release_cumulative_lock();
        LOG(WARNING) << "failed to computer cumulative version hash."
                     << " tablet=" << _tablet->full_name()
                     << ", cumulative_version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        return res;
    }

    // 2. 获取待合并的delta文件对应的data文件
    res = _tablet->capture_consistent_rowsets(_need_merged_versions, &_rowsets);
    if (res != OLAP_SUCCESS) {
        _tablet->release_cumulative_lock();
        LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << _tablet->full_name()
                     << ", version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        return res;
    }

    {
        DorisMetrics::cumulative_compaction_deltas_total.increment(_need_merged_versions.size());
        int64_t merge_bytes = 0;
        for (auto& rowset : _rowsets) {
            merge_bytes += rowset->data_disk_size();
        }
        DorisMetrics::cumulative_compaction_bytes_total.increment(merge_bytes);
    }

    do {
        // 3. 生成新cumulative文件对应的olap index
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
        _rs_writer->init(context);

        // 4. 执行cumulative compaction合并过程
        for (auto& rowset : _rowsets) {
            RowsetReaderSharedPtr rs_reader(rowset->create_reader());
            if (rs_reader == nullptr) {
                LOG(WARNING) << "rowset create reader failed. rowset:" <<  rowset->rowset_id();
                _tablet->release_cumulative_lock();
                return OLAP_ERR_ROWSET_CREATE_READER;
            }
            _rs_readers.push_back(rs_reader);
        }
        res = _do_cumulative_compaction();
        _tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX + std::to_string(_rs_writer->rowset_id()));
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to do cumulative compaction."
                         << ", tablet=" << _tablet->full_name()
                         << ", cumulative_version=" << _cumulative_version.first
                         << "-" << _cumulative_version.second;
            break;
        }
    } while (0);

    // 5. 如果出现错误，执行清理工作
    if (res != OLAP_SUCCESS) {
        StorageEngine::instance()->add_unused_rowset(_rowset);
    }

    _tablet->release_cumulative_lock();

    LOG(INFO) << "succeed to do cumulative compaction. tablet=" << _tablet->full_name()
              << ", cumulative_version=" << _cumulative_version.first
              << "-" << _cumulative_version.second
              << ". elapsed time of doing cumulative compaction"
              << ", time=" << watch.get_elapse_second() << "s";
    return res;
}

OLAPStatus CumulativeCompaction::_calculate_need_merged_versions() {
    Versions delta_versions;
    OLAPStatus res = _get_delta_versions(&delta_versions);
    if (res != OLAP_SUCCESS) {
        LOG(INFO) << "failed to get delta versions. res=" << res;
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
            size_t delta_size = _tablet->get_rowset_size_by_version(delta);
            // 如果遇到大的delta文件，或delete版本文件，则：
            if (delta_size >= _max_delta_file_size
                    || _tablet->version_for_delete_predicate(delta)
                    || _tablet->version_for_load_deletion(delta)) {
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
                    && _tablet->get_rowset_size_by_version(delta_versions[index]) >=
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
    _tablet->set_cumulative_layer_point(delta_versions[index].first);
    _tablet->save_meta();
    return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
}

static bool version_comparator(const Version& lhs, const Version& rhs) {
    return lhs.second < rhs.second;
}

OLAPStatus CumulativeCompaction::_get_delta_versions(Versions* delta_versions) {
    delta_versions->clear();
    
    Versions all_versions;
    _tablet->list_versions(&all_versions);

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
        VLOG(10) << "only one delta version. no new delta versions."
                 << " cumulative_point=" << _old_cumulative_layer_point;
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    sort(delta_versions->begin(), delta_versions->end(), version_comparator);

    // can't do cumulative expansion if there has a hole
    Versions versions_path;
    OLAPStatus select_status = _tablet->capture_consistent_versions(
        Version(delta_versions->front().first, delta_versions->back().second), &versions_path);
    if (select_status != OLAP_SUCCESS) {
        LOG(WARNING) << "can't do cumulative expansion if fail to select shortest version path."
                     << " tablet=" << _tablet->full_name()
                     << ", start=" << delta_versions->front().first
                     << ", end=" << delta_versions->back().second;
        return  select_status;
    }

    return OLAP_SUCCESS;
}

bool CumulativeCompaction::_find_previous_version(const Version current_version,
                                               Version* previous_version) {
    Versions all_versions;
    if (OLAP_SUCCESS != _tablet->capture_consistent_versions(Version(0, current_version.second),
                                                             &all_versions)) {
        LOG(WARNING) << "fail to select shortest version path. start=0, end=" << current_version.second;
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
            if (_tablet->version_for_delete_predicate(*version)
                    || _tablet->version_for_load_deletion(*version)) {
                return false;
            }

            size_t data_size = _tablet->get_rowset_size_by_version(*version);
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
    Merger merger(_tablet, _rs_writer, READER_CUMULATIVE_COMPACTION);

    // 1. merge delta files into new cumulative file
    uint64_t merged_rows = 0;
    uint64_t filted_rows = 0;
    res = merger.merge(_rs_readers, &merged_rows, &filted_rows);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do cumulative merge."
                     << " tablet=" << _tablet->full_name()
                     << ", cumulative_version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        return res;
    }

    _rowset = _rs_writer->build();
    if (_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << _rs_writer->version().first << "-" << _rs_writer->version().second;
        return OLAP_ERR_MALLOC_ERROR;
    }

    // 2. Check row num changes
    uint64_t source_rows = 0;
    for (auto rowset : _rowsets) {
        source_rows += rowset->num_rows();
    }
    bool row_nums_check = config::row_nums_check;
    if (row_nums_check) {
        if (source_rows != _rowset->num_rows() + merged_rows + filted_rows) {
            LOG(FATAL) << "fail to check row num! "
                       << "source_rows=" << source_rows
                       << ", merged_rows=" << merged_rows
                       << ", filted_rows=" << filted_rows
                       << ", new_index_rows=" << _rowset->num_rows();
            return OLAP_ERR_CHECK_LINES_ERROR;
        }
    } else {
        LOG(INFO) << "all row nums. source_rows=" << source_rows
                  << ", merged_rows=" << merged_rows
                  << ", filted_rows=" << filted_rows
                  << ", new_index_rows=" << _rowset->num_rows()
                  << ", merged_version_num=" << _need_merged_versions.size()
                  << ", time_us=" << watch.get_elapse_time_us();
    }

    // 3. add new cumulative file into tablet
    vector<RowsetSharedPtr> unused_rowsets;
    _tablet->obtain_header_wrlock();
    res = _tablet->capture_consistent_rowsets(_need_merged_versions, &unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << _tablet->full_name()
                     << ", version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        _tablet->release_header_lock();
        return res;
    }
    res = _update_header(unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to update header for new cumulative."
                     << "tablet=" << _tablet->full_name()
                     << ", cumulative_version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        _tablet->release_header_lock();
        return res;
    }

    // 4. validate that delete action is right
    res = _validate_delete_file_action();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "delete action of cumulative compaction has error. roll back."
                   << "tablet=" << _tablet->full_name()
                   << ", cumulative_version=" << _cumulative_version.first
                   << "-" << _cumulative_version.second;
        // if error happened, roll back
        OLAPStatus ret = _roll_back(unused_rowsets);
        if (ret != OLAP_SUCCESS) {
            LOG(FATAL) << "roll back failed. [tablet=" <<  _tablet->full_name() << "]";
        }

        _tablet->release_header_lock();
        return res;
    }
    // 5. 如果合并成功，设置新的cumulative_layer_point
    _tablet->set_cumulative_layer_point(_new_cumulative_layer_point);
    _tablet->save_meta();
    _tablet->release_header_lock();

    // 6. delete delta files which have been merged into new cumulative file
    _delete_unused_rowsets(&unused_rowsets);

    return res;
}

OLAPStatus CumulativeCompaction::_update_header(const vector<RowsetSharedPtr>& unused_rowsets) {
    vector<RowsetSharedPtr> new_rowsets;
    new_rowsets.push_back(_rowset);

    OLAPStatus res = OLAP_SUCCESS;
    res = _tablet->modify_rowsets(new_rowsets, unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to replace data sources. res=" << res
                   << ", tablet=" << _tablet->full_name();
        return res;
    }

    res = _tablet->save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to save header. res=" << res
                   << ", tablet=" << _tablet->full_name();
        return res;
    }

    return res;
}

void CumulativeCompaction::_delete_unused_rowsets(vector<RowsetSharedPtr>* unused_rowsets) {
    if (!unused_rowsets->empty()) {
        StorageEngine* storage_engine = StorageEngine::instance();

        for (vector<RowsetSharedPtr>::iterator it = unused_rowsets->begin();
                it != unused_rowsets->end(); ++it) {
            storage_engine->add_unused_rowset(*it);
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
            LOG(WARNING) << "wrong need merged version. "
                         << "previous_version=" << previous_version.first
                         << "-" << previous_version.second
                         << ", current_version=" << current_version.first
                         << "-" << current_version.second;
            return false;
        }
    }

    return true;
}

OLAPStatus CumulativeCompaction::_validate_delete_file_action() {
    // 1. acquire the new cumulative version to make sure that all is right after deleting files
    Version spec_version = Version(0, _cumulative_version.second);
    vector<RowsetReaderSharedPtr> rs_readers;
    _tablet->capture_rs_readers(spec_version, &rs_readers);
    if (rs_readers.empty()) {
        LOG(WARNING) << "acquire data source failed. "
            << "spec_verison=" << spec_version.first << "-" << spec_version.second;
        return OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION;
    }

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::_roll_back(vector<RowsetSharedPtr>& old_olap_indices) {
    vector<RowsetSharedPtr> unused_rowsets;
    OLAPStatus res = _tablet->capture_consistent_rowsets(_cumulative_version, &unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << _tablet->full_name()
                     << ", version=" << _cumulative_version.first
                     << "-" << _cumulative_version.second;
        return res;
    }

    // unused_rowsets will only contain new cumulative index
    // we don't need to delete it here; we will delete new cumulative index in the end.

    res = OLAP_SUCCESS;
    res = _tablet->modify_rowsets(old_olap_indices, unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to replace data sources. [tablet=" << _tablet->full_name() << "]";
        return res;
    }

    res = _tablet->save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to save header. [tablet=" << _tablet->full_name() << "]";
        return res;
    }

    return res;
}

}  // namespace doris

