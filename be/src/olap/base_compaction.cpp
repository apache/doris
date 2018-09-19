// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include "olap/merger.h"
#include "olap/olap_data.h"
#include "olap/olap_engine.h"
#include "olap/olap_header.h"
#include "olap/olap_index.h"
#include "olap/olap_table.h"
#include "olap/utils.h"
#include "util/palo_metrics.h"

using std::list;
using std::map;
using std::string;
using std::vector;

namespace palo {

OLAPStatus BaseCompaction::init(SmartOLAPTable table, bool is_manual_trigger) {
    // 表在首次查询或PUSH等操作时，会被加载到内存
    // 如果表没有被加载，表明该表上目前没有任何操作，所以不进行BE操作
    if (!table->is_loaded()) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAP_LOG_TRACE("init base compaction handler. [table=%s]", table->full_name().c_str());

    _table = table;

    // 1. 尝试取得base compaction的锁
    if (!_try_base_compaction_lock()) {
        OLAP_LOG_WARNING("another base compaction is running. [table=%s]",
                         table->full_name().c_str());
        return OLAP_ERR_BE_TRY_BE_LOCK_ERROR;
    }

    // 2. 检查是否满足base compaction触发策略
    OLAP_LOG_TRACE("check whether satisfy base compaction policy.");
    bool is_policy_satisfied = false;
    vector<Version> candidate_versions;
    is_policy_satisfied = _check_whether_satisfy_policy(is_manual_trigger, &candidate_versions);

    // 2.1 如果不满足触发策略，则直接释放base compaction锁, 返回错误码
    if (!is_policy_satisfied) {
        _release_base_compaction_lock();

        return OLAP_ERR_BE_NO_SUITABLE_VERSION;
    }

    // 2.2 如果满足触发策略，触发base compaction
    //     不释放base compaction锁, 在run()完成之后再释放
    if (!_validate_need_merged_versions(candidate_versions)) {
        OLAP_LOG_FATAL("error! invalid need merged versions");
        _release_base_compaction_lock();
        return OLAP_ERR_BE_INVALID_NEED_MERGED_VERSIONS;
    }

    _need_merged_versions = candidate_versions;

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::run() {
    OLAP_LOG_INFO("start base compaction. [table=%s; old_base_version=%d; new_base_version=%d]",
                  _table->full_name().c_str(),
                  _old_base_version.second,
                  _new_base_version.second);

    OLAPStatus res = OLAP_SUCCESS;
    OlapStopWatch stage_watch;

    _table->set_base_compaction_status(BASE_COMPACTION_RUNNING, _new_base_version.second);

    // 1. 计算新base的version hash
    VersionHash new_base_version_hash;
    res = _table->compute_all_versions_hash(_need_merged_versions, &new_base_version_hash);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to calculate new base version hash.[table=%s; new_base_version=%d]",
                         _table->full_name().c_str(),
                         _new_base_version.second);
        _cleanup();
        return res;
    }

    OLAP_LOG_TRACE("new_base_version_hash", "%ld", new_base_version_hash);

    // 2. 获取生成新base需要的data sources
    vector<IData*> base_data_sources;
    _table->acquire_data_sources_by_versions(_need_merged_versions, &base_data_sources);
    if (base_data_sources.empty()) {
        OLAP_LOG_WARNING("fail to acquire need data sources. [table=%s; version=%d]",
                         _table->full_name().c_str(),
                         _new_base_version.second);
        _cleanup();
        return OLAP_ERR_BE_ACQUIRE_DATA_SOURCES_ERROR;
    }

    {
        PaloMetrics::base_compaction_deltas_total.increment(_need_merged_versions.size());
        int64_t merge_bytes = 0;
        for (IData* i_data : base_data_sources) {
            merge_bytes += i_data->olap_index()->data_size();
        }
        PaloMetrics::base_compaction_bytes_total.increment(merge_bytes);
    }

    // 保存生成base文件时候计算的selectivities
    vector<uint32_t> selectivities;
    // 保存生成base文件时候累积的行数
    uint64_t row_count = 0;

    // 3. 执行base compaction
    //    执行过程可能会持续比较长时间
    stage_watch.reset();
    res = _do_base_compaction(new_base_version_hash,
                             &base_data_sources,
                             &selectivities,
                             &row_count);
    // 释放不再使用的IData对象
    _table->release_data_sources(&base_data_sources);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to do base version. [table=%s; version=%d]",
                         _table->full_name().c_str(),
                         _new_base_version.second);
        _cleanup();
        return res;
    }
    OLAP_LOG_TRACE("elapsed time of doing base version", "%ldus",
                   stage_watch.get_elapse_time_us());

    // 4. 使新生成的base生效，并删除不再需要版本对应的文件
    _obtain_header_wrlock();
    vector<OLAPIndex*> unused_olap_indices;
    // 使得新生成的各个Version生效, 如果失败掉则需要清理掉已经生成的Version文件
    res = _update_header(selectivities, row_count, &unused_olap_indices);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to update header. [table=%s; version=%d]",
                         _table->full_name().c_str(),
                         _new_base_version.second);
        _cleanup();
        return res;
    }
    _release_header_lock();
    _delete_old_files(&unused_olap_indices);

    //  validate that delete action is right
    //  if error happened, sleep 1 hour. Report a fatal log every 1 minute
    if (_validate_delete_file_action() != OLAP_SUCCESS) {
        int sleep_count = 0;
        while (true) {
            if (sleep_count >= 60) {
                break;
            }

            ++sleep_count;
            OLAP_LOG_FATAL("base compaction's delete action has error.sleep 1 minute...");
            sleep(60);
        }

        _cleanup();
        return OLAP_ERR_BE_ERROR_DELETE_ACTION;
    }

    _table->set_base_compaction_status(BASE_COMPACTION_WAITING, -1);
    _release_base_compaction_lock();

    LOG(INFO) << "succeed to do base compaction. table=" << _table->full_name() << ", "
              << "base_version=" << _new_base_version.first << "-" << _new_base_version.second;
    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::_exclude_not_expired_delete(
        const vector<Version>& need_merged_versions,
        vector<Version>* candidate_versions) {
    const int64_t delete_delta_expire_time = config::delete_delta_expire_time * 60;
    OLAPStatus res = OLAP_SUCCESS;
    for (unsigned int index = 0; index < need_merged_versions.size(); ++index) {
        Version temp = need_merged_versions[index];
        int64_t file_creation_time = 0;
        res = _table->version_creation_time(temp, &file_creation_time);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("table doesn't have version. [table=%s; version=%d-%d]",
                             _table->full_name().c_str(), temp.first, temp.second);
            return res;
        }

        int64_t file_existed_time = time(NULL) - file_creation_time;
        // 从小版本号往大版本号查找；找到第1个没有过期的delete版本时，退出
        if (_table->is_delete_data_version(temp)
                && file_existed_time < delete_delta_expire_time) {
            OLAP_LOG_INFO("delete version is not expired."
                          "[delete_version=%d; existed_time=%ld; expired_time=%ld]",
                          temp.first, file_existed_time,
                          delete_delta_expire_time);
            break;
        }

        candidate_versions->push_back(temp);
    }

    return OLAP_SUCCESS;
}

static bool version_comparator(const Version& lhs, const Version& rhs) {
    return lhs.second < rhs.second;
}

bool BaseCompaction::_check_whether_satisfy_policy(bool is_manual_trigger,
                                                         vector<Version>* candidate_versions) {
    _obtain_header_rdlock();
    int32_t cumulative_layer_point = _table->cumulative_layer_point();
    if (cumulative_layer_point == -1) {
        OLAP_LOG_FATAL("tablet has an unreasonable cumulative layer point. "
                       "[tablet='%s' cumulative_layer_point=%d]",
                       _table->full_name().c_str(), cumulative_layer_point);
        _release_header_lock();
        return false;
    }

    // 为了后面计算方便，我们在这里先将cumulative_layer_point减1
    --cumulative_layer_point;

    vector<Version> path_versions;
    if (OLAP_SUCCESS != _table->select_versions_to_span(Version(0, cumulative_layer_point),
                                                        &path_versions)) {
        OLAP_LOG_WARNING("fail to select shortest version path. [start=%d end=%d]",
                         0, cumulative_layer_point);
        _release_header_lock();
        return  false;
    }

    // base_compaction_layer_point应该为cumulative_layer_point之前，倒数第2个cumulative文件的end version
    int64_t base_creation_time = 0;
    size_t base_size = 0;
    int32_t base_compaction_layer_point = -1;
    for (unsigned int index = 0; index < path_versions.size(); ++index) {
        Version temp = path_versions[index];
        // base文件
        if (temp.first == 0) {
            _old_base_version = temp;
            base_size = _table->get_version_entity_by_version(temp).data_size;
            base_creation_time = _table->file_version(index).creation_time();
            continue;
        }

        if (temp.second == cumulative_layer_point) {
            base_compaction_layer_point = temp.first - 1;
            _latest_cumulative = temp;
            _new_base_version = Version(0, base_compaction_layer_point);
        }
    }

    // 只有1个base文件和1个delta文件
    if (base_compaction_layer_point == -1) {
        OLAP_LOG_TRACE("can't do base compaction: no cumulative files. "
                       "[table=%s; base_version=0-%d; cumulative_layer_point=%d]",
                       _table->full_name().c_str(),
                       _old_base_version.second,
                       cumulative_layer_point + 1);

        _release_header_lock();
        return false;
    }

    // 只有1个cumulative文件
    if (base_compaction_layer_point == _old_base_version.second) {
        OLAP_LOG_TRACE("can't do base compaction: only one cumulative file. "
                       "[table=%s; base_version=0-%d; cumulative_layer_point=%d]",
                       _table->full_name().c_str(),
                       _old_base_version.second,
                       cumulative_layer_point + 1);

        _release_header_lock();
        return false;
    }

    // 使用最短路径算法，选择可合并的cumulative版本
    vector<Version> need_merged_versions;
    if (OLAP_SUCCESS != _table->select_versions_to_span(_new_base_version,
                                                        &need_merged_versions)) {
        OLAP_LOG_WARNING("fail to select shortest version path. [start=%d end=%d]",
                         _new_base_version.first, _new_base_version.second);
        _release_header_lock();
        return  false;
    }

    std::sort(need_merged_versions.begin(), need_merged_versions.end(), version_comparator);

    // 如果是手动执行START_BASE_COMPACTION命令，则不检查base compaction policy, 
    // 也不考虑删除版本过期问题,  只要有可以合并的cumulative，就执行base compaction
    if (is_manual_trigger) {
        OLAP_LOG_TRACE("manual trigger base compaction. [table=%s]", _table->full_name().c_str());

        *candidate_versions = need_merged_versions;
        _release_header_lock();
        return true;
    }

    if (_exclude_not_expired_delete(need_merged_versions, candidate_versions) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to exclude not expired delete version.");
        _release_header_lock();
        return false;
    }

    if (candidate_versions->size() != need_merged_versions.size()) {
        OLAP_LOG_INFO("reset new base version. "
                      "[previous_new_base_version=0-%d; new_base_version=0-%d]",
                      _new_base_version.second, candidate_versions->rbegin()->second);
        _new_base_version = Version(0, candidate_versions->rbegin()->second);
    }

    // 统计可合并cumulative版本文件的总大小
    size_t cumulative_total_size = 0;
    for (vector<Version>::const_iterator version_iter = candidate_versions->begin();
            version_iter != candidate_versions->end(); ++version_iter) {
        Version temp = *version_iter;
        // 跳过base文件
        if (temp.first == 0) {
            continue;
        }
        // cumulative文件
        cumulative_total_size += _table->get_version_entity_by_version(temp).data_size;
    }

    _release_header_lock();

    // 检查是否满足base compaction的触发条件
    // 满足以下条件时触发base compaction: 触发条件1 || 触发条件2 || 触发条件3
    // 触发条件1：cumulative文件个数超过一个阈值
    const uint32_t base_compaction_num_cumulative_deltas
        = config::base_compaction_num_cumulative_deltas;
    // candidate_versions中包含base文件，所以这里减1
    if (candidate_versions->size() - 1 >= base_compaction_num_cumulative_deltas) {
        OLAP_LOG_INFO("satisfy the base compaction policy. [table=%s; "
                      "num_cumualtive_deltas=%d; base_compaction_num_cumulative_deltas=%d]",
                      _table->full_name().c_str(),
                      candidate_versions->size() - 1,
                      base_compaction_num_cumulative_deltas);
        return true;
    }

    // 触发条件2：所有cumulative文件的大小超过base文件大小的某一比例
    const double base_cumulative_delta_ratio = config::base_cumulative_delta_ratio;
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;
    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        OLAP_LOG_INFO("satisfy the base compaction policy. [table=%s; cumualtive_total_size=%d; "
                      "base_size=%d; cumulative_base_ratio=%f; policy_ratio=%f]",
                      _table->full_name().c_str(),
                      cumulative_total_size,
                      base_size,
                      cumulative_base_ratio,
                      base_cumulative_delta_ratio);
        return true;
    }

    // 触发条件3：距离上一次进行base compaction已经超过设定的间隔时间
    const uint32_t interval_since_laste_operation = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_be = time(NULL) - base_creation_time;
    if (interval_since_last_be > interval_since_laste_operation) {
        OLAP_LOG_INFO("satisfy the base compaction policy. [table=%s; "
                      "interval_since_last_be=%ld; policy_interval=%ld]",
                      _table->full_name().c_str(),
                      interval_since_last_be, interval_since_laste_operation);
        return true;
    }

    OLAP_LOG_TRACE(
            "don't satisfy the base compaction policy."
            "[cumulative_files_number=%d; cumulative_base_ratio=%f; interval_since_last_be=%ld]",
            candidate_versions->size() - 1,
            cumulative_base_ratio,
            interval_since_last_be);

    return false;
}

OLAPStatus BaseCompaction::_do_base_compaction(VersionHash new_base_version_hash,
                                                    vector<IData*>* base_data_sources,
                                                    vector<uint32_t>* selectivities,
                                                    uint64_t* row_count) {
    // 1. 生成新base文件对应的olap index
    OLAPIndex* new_base = new (std::nothrow) OLAPIndex(_table.get(),
                                                       _new_base_version,
                                                       new_base_version_hash,
                                                       false,
                                                       0, 0);
    if (new_base == NULL) {
        OLAP_LOG_WARNING("fail to new OLAPIndex.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    OLAP_LOG_INFO("start merge new base. [table='%s' version=%d]",
                  _table->full_name().c_str(),
                  _new_base_version.second);

    // 2. 执行base compaction的merge
    // 注意：无论是行列存，还是列存，在执行merge时都使用Merger类，不能使用MassiveMerger。
    // 原因：MassiveMerger中的base文件不是通过Reader读取的，所以会导致删除条件失效,
    //       无法达到删除数据的目的
    // 想法：如果一定要使用MassiveMerger，这里可以提供一种方案
    //       1. 在此处加一个检查，检测此次BE是否包含删除条件, 即检查Reader中
    //          ReaderParams的delete_handler
    //       2. 如果包含删除条件，则不使用MassiveMerger，使用Merger
    //       3. 如果不包含删除条件，则可以使用MassiveMerger
    uint64_t merged_rows = 0;
    uint64_t filted_rows = 0;
    OLAPStatus res = OLAP_SUCCESS;
    if (_table->data_file_type() == OLAP_DATA_FILE
            || _table->data_file_type() == COLUMN_ORIENTED_FILE) {
        _table->obtain_header_rdlock();
        bool use_simple_merge = true;
        if (_table->delete_data_conditions_size() > 0) {
            use_simple_merge = false;
        }
        _table->release_header_lock();

        Merger merger(_table, new_base, READER_BASE_COMPACTION);
        res = merger.merge(
                *base_data_sources, use_simple_merge, &merged_rows, &filted_rows);
        if (res == OLAP_SUCCESS) {
            *row_count = merger.row_count();
            *selectivities = merger.selectivities();
        }
    } else {
        OLAP_LOG_WARNING("unknown data file type. [type=%s]",
                         DataFileType_Name(_table->data_file_type()).c_str());
        res = OLAP_ERR_DATA_FILE_TYPE_ERROR;
    }

    // 3. 如果merge失败，执行清理工作，返回错误码退出
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to make new base version. [table='%s' version='%d.%d' res=%d]",
                         _table->full_name().c_str(),
                         _new_base_version.first,
                         _new_base_version.second,
                         res);

        new_base->delete_all_files();
        SAFE_DELETE(new_base);

        return OLAP_ERR_BE_MERGE_ERROR;
    }

    // 4. 如果merge成功，则将新base文件对应的olap index载入
    _new_olap_indices.push_back(new_base);

    OLAP_LOG_TRACE("merge new base success, start load index. [table='%s' version=%d]",
                   _table->full_name().c_str(),
                   _new_base_version.second);

    res = new_base->load();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to load index. [version='%d-%d' version_hash=%ld table='%s']",
                         new_base->version().first,
                         new_base->version().second,
                         new_base->version_hash(),
                         _table->full_name().c_str());
        return res;
    }

    // Check row num changes
    uint64_t source_rows = 0;
    for (IData* i_data : *base_data_sources) {
        source_rows += i_data->olap_index()->num_rows();
    }
    bool row_nums_check = config::row_nums_check;
    if (row_nums_check) {
        if (source_rows != new_base->num_rows() + merged_rows + filted_rows) {
            OLAP_LOG_FATAL("fail to check row num! "
                           "[source_rows=%lu merged_rows=%lu filted_rows=%lu new_index_rows=%lu]",
                           source_rows, merged_rows, filted_rows, new_base->num_rows());
            return OLAP_ERR_CHECK_LINES_ERROR;
        }
    } else {
        OLAP_LOG_INFO("all row nums. "
                      "[source_rows=%lu merged_rows=%lu filted_rows=%lu new_index_rows=%lu]",
                      source_rows, merged_rows, filted_rows, new_base->num_rows());
    }

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::_update_header(const vector<uint32_t>& selectivities,
                                                uint64_t row_count,
                                                vector<OLAPIndex*>* unused_olap_indices) {
    vector<Version> unused_versions;
    _get_unused_versions(&unused_versions);

    OLAPStatus res = OLAP_SUCCESS;
    // 由于在replace_data_sources中可能会发生很小概率的非事务性失败, 因此这里定位FATAL错误
    res = _table->replace_data_sources(&unused_versions,
                                       &_new_olap_indices,
                                       unused_olap_indices);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to replace data sources. "
                       "[res=%d table=%s; new_base=%d; old_base=%d]",
                       _table->full_name().c_str(),
                       _new_base_version.second,
                       _old_base_version.second);
        return res;
    }

    _table->set_selectivities(selectivities);

    OLAP_LOG_INFO("BE remove delete conditions. [removed_version=%d]", _new_base_version.second);

    // Base Compaction完成之后，需要删除header中版本号小于等于新base文件版本号的删除条件
    DeleteConditionHandler cond_handler;
    cond_handler.delete_cond(_table, _new_base_version.second, true);

    // 如果保存Header失败, 所有新增的信息会在下次启动时丢失, 属于严重错误
    // 暂时没办法做很好的处理,报FATAL
    res = _table->save_header();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to save header. "
                       "[res=%d table=%s; new_base=%d; old_base=%d]",
                       _table->full_name().c_str(),
                       _new_base_version.second,
                       _old_base_version.second);
        return OLAP_ERR_BE_SAVE_HEADER_ERROR;
    }
    _new_olap_indices.clear();

    return OLAP_SUCCESS;
}

void BaseCompaction::_delete_old_files(vector<OLAPIndex*>* unused_indices) {
    if (!unused_indices->empty()) {
        OLAPUnusedIndex* unused_index = OLAPUnusedIndex::get_instance();

        for (vector<OLAPIndex*>::iterator it = unused_indices->begin();
                it != unused_indices->end(); ++it) {
            unused_index->add_unused_index(*it);
        }
    }
}

void BaseCompaction::_cleanup() {
    // 清理掉已生成的版本文件
    for (vector<OLAPIndex*>::iterator it = _new_olap_indices.begin();
            it != _new_olap_indices.end(); ++it) {
        (*it)->delete_all_files();
        SAFE_DELETE(*it);
    }
    _new_olap_indices.clear();

    // 释放打开的锁
    _release_header_lock();
    _release_base_compaction_lock();
    _table->set_base_compaction_status(BASE_COMPACTION_WAITING, -1);
}

bool BaseCompaction::_validate_need_merged_versions(
        const vector<Version>& candidate_versions) {
    if (candidate_versions.size() <= 1) {
        OLAP_LOG_WARNING("unenough versions need to be merged. [size=%lu]",
                         candidate_versions.size());
        return false;
    }

    // 1. validate versions in candidate_versions are continuous
    // Skip the first element
    for (unsigned int index = 1; index < candidate_versions.size(); ++index) {
        Version previous_version = candidate_versions[index - 1];
        Version current_version = candidate_versions[index];
        if (current_version.first != previous_version.second + 1) {
            OLAP_LOG_WARNING("wrong need merged version. "
                             "previous_version=%d-%d; current_version=%d-%d",
                             previous_version.first, previous_version.second,
                             current_version.first, current_version.second);
            return false;
        }
    }

    // 2. validate m_new_base_version is OK
    if (_new_base_version.first != 0
            || _new_base_version.first != candidate_versions.begin()->first
            || _new_base_version.second != candidate_versions.rbegin()->second) {
        OLAP_LOG_WARNING("new_base_version is wrong. "
                         "[new_base_version=%d-%d; vector_version=%d-%d]",
                         _new_base_version.first, _new_base_version.second,
                         candidate_versions.begin()->first,
                         candidate_versions.rbegin()->second);
        return false;
    }

    OLAP_LOG_TRACE("valid need merged version");
    return true;
}

OLAPStatus BaseCompaction::_validate_delete_file_action() {
    // 1. acquire the latest version to make sure all is right after deleting files
    _obtain_header_rdlock();
    const FileVersionMessage* latest_version = _table->latest_version();
    Version test_version = Version(0, latest_version->end_version());
    vector<IData*> test_sources;
    _table->acquire_data_sources(test_version, &test_sources);

    if (test_sources.size() == 0) {
        OLAP_LOG_INFO("acquire data sources failed. version=%d-%d",
                      test_version.first, test_version.second);
        _release_header_lock();
        return OLAP_ERR_BE_ERROR_DELETE_ACTION;
    }

    _table->release_data_sources(&test_sources);
    OLAP_LOG_TRACE("delete file action is OK");
    _release_header_lock();

    return OLAP_SUCCESS;
}

}  // namespace palo
