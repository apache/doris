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
#include "olap/merger.h"
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

OLAPStatus BaseCompaction::init(TabletSharedPtr tablet, bool is_manual_trigger) {
    // 表在首次查询或PUSH等操作时，会被加载到内存
    // 如果表没有被加载，表明该表上目前没有任何操作，所以不进行BE操作
    if (!tablet->init_succeeded()) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    LOG(INFO) << "init base compaction handler. [tablet=" << tablet->full_name() << "]";

    _tablet = tablet;

    // 1. 尝试取得base compaction的锁
    if (!_try_base_compaction_lock()) {
        LOG(WARNING) << "another base compaction is running. tablet=" << tablet->full_name();
        return OLAP_ERR_BE_TRY_BE_LOCK_ERROR;
    }

    // 2. 检查是否满足base compaction触发策略
    VLOG(3) << "check whether satisfy base compaction policy.";
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
        LOG(FATAL) << "error! invalid need merged versions";
        _release_base_compaction_lock();
        return OLAP_ERR_BE_INVALID_NEED_MERGED_VERSIONS;
    }

    _need_merged_versions = candidate_versions;

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::run() {
    LOG(INFO) << "start base compaction. tablet=" << _tablet->full_name()
              << ", old_base_version=" << _old_base_version.second
              << ", new_base_version=" << _new_base_version.second;

    OLAPStatus res = OLAP_SUCCESS;
    OlapStopWatch stage_watch;

    // 1. 计算新base的version hash
    VersionHash new_base_version_hash;
    res = _tablet->compute_all_versions_hash(_need_merged_versions, &new_base_version_hash);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to calculate new base version hash. tablet=" << _tablet->full_name()
                     << ", new_base_version=" << _new_base_version.second;
        _garbage_collection();
        return res;
    }

    VLOG(10) << "new_base_version_hash:" << new_base_version_hash;

    // 2. 获取生成新base需要的data sources
    vector<RowsetSharedPtr> rowsets;
    res = _tablet->capture_consistent_rowsets(_need_merged_versions, &rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to acquire need data sources. tablet=" << _tablet->full_name()
                     << ", version=" << _new_base_version.second;
        _garbage_collection();
        return res;
    }

    {
        DorisMetrics::base_compaction_deltas_total.increment(_need_merged_versions.size());
        int64_t merge_bytes = 0;
        for (auto& rowset : rowsets) {
            merge_bytes += rowset->data_disk_size();
        }
        DorisMetrics::base_compaction_bytes_total.increment(merge_bytes);
    }

    // 3. 执行base compaction
    //    执行过程可能会持续比较长时间
    stage_watch.reset();
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
    context.version = _new_base_version;
    context.version_hash = new_base_version_hash;

    _rs_writer.reset(new (std::nothrow)AlphaRowsetWriter());
    if (_rs_writer == nullptr) {
        LOG(WARNING) << "fail to new rowset.";
        _garbage_collection();
        return OLAP_ERR_MALLOC_ERROR;
    }
    RETURN_NOT_OK(_rs_writer->init(context));
    res = _do_base_compaction(new_base_version_hash, rowsets);
    _tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX + std::to_string(_rs_writer->rowset_id()));
    // 释放不再使用的ColumnData对象
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to do base version. tablet=" << _tablet->full_name()
                     << ", version=" << _new_base_version.second;
        _garbage_collection();
        return res;
    }

    //  validate that delete action is right
    //  if error happened, sleep 1 hour. Report a fatal log every 1 minute
    if (_validate_delete_file_action() != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do base compaction. delete action has error.";
        _garbage_collection();
        return OLAP_ERR_BE_ERROR_DELETE_ACTION;
    }

    // 4. make new versions visable.
    //    If success, remove files belong to old versions;
    //    If fail, gc files belong to new versions.
    vector<RowsetSharedPtr> unused_rowsets;
    vector<Version> unused_versions;
    _get_unused_versions(&unused_versions);
    res = _tablet->capture_consistent_rowsets(unused_versions, &unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << _tablet->full_name()
                     << ", version=" << _new_base_version.second;
        _garbage_collection();
        return res;
    }

    res = _update_header(unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to update header. tablet=" << _tablet->full_name()
                     << ", version=" << _new_base_version.first << "-" << _new_base_version.second;
        _garbage_collection();
        return res;
    }
    _delete_old_files(&unused_rowsets);

    _release_base_compaction_lock();

    LOG(INFO) << "succeed to do base compaction. tablet=" << _tablet->full_name()
              << ", base_version=" << _new_base_version.first << "-" << _new_base_version.second
              << ". elapsed time of doing base compaction"
              << ", time=" << stage_watch.get_elapse_second() << "s";

    return OLAP_SUCCESS;
}

static bool version_comparator(const Version& lhs, const Version& rhs) {
    return lhs.second < rhs.second;
}

bool BaseCompaction::_check_whether_satisfy_policy(bool is_manual_trigger,
                                                         vector<Version>* candidate_versions) {
    ReadLock rdlock(_tablet->get_header_lock_ptr());
    int64_t cumulative_layer_point = _tablet->cumulative_layer_point();
    if (cumulative_layer_point == -1) {
        LOG(FATAL) << "tablet has an unreasonable cumulative layer point. [tablet='" << _tablet->full_name()
                   << "' cumulative_layer_point=" << cumulative_layer_point << "]";
        return false;
    }

    // 为了后面计算方便，我们在这里先将cumulative_layer_point减1
    --cumulative_layer_point;

    vector<Version> path_versions;
    if (OLAP_SUCCESS != _tablet->capture_consistent_versions(Version(0, cumulative_layer_point), &path_versions)) {
        LOG(WARNING) << "fail to select shortest version path. start=0, end=" << cumulative_layer_point;
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
            base_size = _tablet->get_rowset_size_by_version(temp);
            base_creation_time = _tablet->get_rowset_by_version(temp)->creation_time();
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
        VLOG(3) << "can't do base compaction: no cumulative files."
                << "tablet=" << _tablet->full_name()
                << ", base_version=0-" << _old_base_version.second
                << ", cumulative_layer_point=" << cumulative_layer_point + 1;
        return false;
    }

    // 只有1个cumulative文件
    if (base_compaction_layer_point == _old_base_version.second) {
        VLOG(3) << "can't do base compaction: only one cumulative file."
                << "tablet=" << _tablet->full_name()
                << ", base_version=0-" << _old_base_version.second
                << ", cumulative_layer_point=" << cumulative_layer_point + 1;
        return false;
    }

    // 使用最短路径算法，选择可合并的cumulative版本
    if (OLAP_SUCCESS != _tablet->capture_consistent_versions(_new_base_version, candidate_versions)) {
        LOG(WARNING) << "fail to select shortest version path."
            << "start=" << _new_base_version.first
            << ", end=" << _new_base_version.second;
        return  false;
    }

    std::sort(candidate_versions->begin(), candidate_versions->end(), version_comparator);

    // 如果是手动执行START_BASE_COMPACTION命令，则不检查base compaction policy,
    // 也不考虑删除版本过期问题,  只要有可以合并的cumulative，就执行base compaction
    if (is_manual_trigger) {
        VLOG(3) << "manual trigger base compaction. tablet=" << _tablet->full_name();
        return true;
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
        cumulative_total_size += _tablet->get_rowset_size_by_version(temp);
    }

    // 检查是否满足base compaction的触发条件
    // 满足以下条件时触发base compaction: 触发条件1 || 触发条件2 || 触发条件3
    // 触发条件1：cumulative文件个数超过一个阈值
    const uint32_t base_compaction_num_cumulative_deltas
        = config::base_compaction_num_cumulative_deltas;
    // candidate_versions中包含base文件，所以这里减1
    if (candidate_versions->size() - 1 >= base_compaction_num_cumulative_deltas) {
        LOG(INFO) << "satisfy the base compaction policy. tablet="<< _tablet->full_name()
            << ", num_cumulative_deltas=" << candidate_versions->size() - 1
            << ", base_compaction_num_cumulative_deltas=" << base_compaction_num_cumulative_deltas;
        return true;
    }

    // 触发条件2：所有cumulative文件的大小超过base文件大小的某一比例
    const double base_cumulative_delta_ratio = config::base_cumulative_delta_ratio;
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;
    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
            << ", cumualtive_total_size=" << cumulative_total_size
            << ", base_size=" << base_size
            << ", cumulative_base_ratio=" << cumulative_base_ratio
            << ", policy_ratio=" << base_cumulative_delta_ratio;
        return true;
    }

    // 触发条件3：距离上一次进行base compaction已经超过设定的间隔时间
    const uint32_t interval_since_last_operation = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_be = time(NULL) - base_creation_time;
    if (interval_since_last_be > interval_since_last_operation) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
            << ", interval_since_last_be=" << interval_since_last_be
            << ", policy_interval=" << interval_since_last_operation;
        return true;
    }

    VLOG(3) << "don't satisfy the base compaction policy. tablet=" << _tablet->full_name()
        << ", cumulative_files_number=" << candidate_versions->size() - 1
        << ", cumulative_base_ratio=" << cumulative_base_ratio
        << ", interval_since_last_be=" << interval_since_last_be;

    return false;
}

OLAPStatus BaseCompaction::_do_base_compaction(VersionHash new_base_version_hash,
                                               const vector<RowsetSharedPtr>& rowsets) {
    OlapStopWatch watch;
    vector<RowsetReaderSharedPtr> rs_readers;
    for (auto& rowset : rowsets) {
        RowsetReaderSharedPtr rs_reader(rowset->create_reader());
        if (rs_reader == nullptr) {
            LOG(WARNING) << "rowset create reader failed. rowset:" <<  rowset->rowset_id();
            return OLAP_ERR_ROWSET_CREATE_READER;
        }
        rs_readers.push_back(rs_reader);
    }

    LOG(INFO) << "start merge new base. tablet=" << _tablet->full_name()
              << ", version=" << _new_base_version.second;
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

    Merger merger(_tablet, _rs_writer, READER_BASE_COMPACTION);
    res = merger.merge(rs_readers, &merged_rows, &filted_rows);
    // 3. 如果merge失败，执行清理工作，返回错误码退出
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to make new base version. res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", version=" << _new_base_version.first
                     << "-" << _new_base_version.second;
        return OLAP_ERR_BE_MERGE_ERROR;
    }
    RowsetSharedPtr new_base = _rs_writer->build();
    if (new_base == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << _rs_writer->version().first << "-" << _rs_writer->version().second;
        return OLAP_ERR_MALLOC_ERROR;
    }

    // 4. 如果merge成功，则将新base文件对应的olap index载入
    _new_rowsets.push_back(new_base);

    VLOG(10) << "merge new base success, start load index. tablet=" << _tablet->full_name()
             << ", version=" << _new_base_version.second;

    // Check row num changes
    uint64_t source_rows = 0;
    for (auto& rowset : rowsets) {
        source_rows += rowset->num_rows();
    }
    bool row_nums_check = config::row_nums_check;
    if (row_nums_check) {
        if (source_rows != new_base->num_rows() + merged_rows + filted_rows) {
            LOG(WARNING) << "fail to check row num!"
                << "source_rows=" << source_rows
                << ", merged_rows=" << merged_rows
                << ", filted_rows=" << filted_rows
                << ", new_index_rows=" << new_base->num_rows();
            return OLAP_ERR_CHECK_LINES_ERROR;
        }
    } else {
        LOG(INFO) << "all row nums."
            << "source_rows=" << source_rows
            << ", merged_rows=" << merged_rows
            << ", filted_rows=" << filted_rows
            << ", new_index_rows=" << new_base->num_rows()
            << ", merged_version_num=" << _need_merged_versions.size()
            << ", time_us=" << watch.get_elapse_time_us();
    }

    return OLAP_SUCCESS;
}

OLAPStatus BaseCompaction::_update_header(const vector<RowsetSharedPtr>& unused_rowsets) {
    WriteLock wrlock(_tablet->get_header_lock_ptr());

    OLAPStatus res = OLAP_SUCCESS;
    // 由于在modify_rowsets中可能会发生很小概率的非事务性失败, 因此这里定位FATAL错误
    res = _tablet->modify_rowsets(_new_rowsets, unused_rowsets);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to replace data sources. res" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", new_base_version=" << _new_base_version.second
                   << ", old_base_verison=" << _old_base_version.second;
        return res;
    }

    // 如果保存Header失败, 所有新增的信息会在下次启动时丢失, 属于严重错误
    // 暂时没办法做很好的处理,报FATAL
    res = _tablet->save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet meta. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", new_base_version=" << _new_base_version.second
                   << ", old_base_version=" << _old_base_version.second;
        return OLAP_ERR_BE_SAVE_HEADER_ERROR;
    }
    _new_rowsets.clear();

    return OLAP_SUCCESS;
}

void BaseCompaction::_delete_old_files(vector<RowsetSharedPtr>* unused_indices) {
    if (!unused_indices->empty()) {
        StorageEngine* storage_engine = StorageEngine::instance();

        for (vector<RowsetSharedPtr>::iterator it = unused_indices->begin();
                it != unused_indices->end(); ++it) {
            storage_engine->add_unused_rowset(*it);
        }
    }
}

void BaseCompaction::_garbage_collection() {
    // 清理掉已生成的版本文件
    StorageEngine* storage_engine = StorageEngine::instance();
    for (vector<RowsetSharedPtr>::iterator it = _new_rowsets.begin();
            it != _new_rowsets.end(); ++it) {
        storage_engine->add_unused_rowset(*it);
    }
    _new_rowsets.clear();

    _release_base_compaction_lock();
}

bool BaseCompaction::_validate_need_merged_versions(
        const vector<Version>& candidate_versions) {
    if (candidate_versions.size() <= 1) {
        LOG(WARNING) << "unenough versions need to be merged. size=" << candidate_versions.size();
        return false;
    }

    // 1. validate versions in candidate_versions are continuous
    // Skip the first element
    for (unsigned int index = 1; index < candidate_versions.size(); ++index) {
        Version previous_version = candidate_versions[index - 1];
        Version current_version = candidate_versions[index];
        if (current_version.first != previous_version.second + 1) {
            LOG(WARNING) << "wrong need merged version. "
                         << "previous_version=" << previous_version.first
                         << "-" << previous_version.second
                         << ", current_version=" << current_version.first
                         << "-" << current_version.second;
            return false;
        }
    }

    // 2. validate m_new_base_version is OK
    if (_new_base_version.first != 0
            || _new_base_version.first != candidate_versions.begin()->first
            || _new_base_version.second != candidate_versions.rbegin()->second) {
        LOG(WARNING) << "new_base_version is wrong."
                     << " new_base_version=" << _new_base_version.first
                     << "-" << _new_base_version.second
                     << ", vector_version=" << candidate_versions.begin()->first
                     << "-" << candidate_versions.rbegin()->second;
        return false;
    }

    VLOG(10) << "valid need merged version";
    return true;
}

OLAPStatus BaseCompaction::_validate_delete_file_action() {
    // 1. acquire the latest version to make sure all is right after deleting files
    ReadLock rdlock(_tablet->get_header_lock_ptr());
    const RowsetSharedPtr rowset = _tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(INFO) << "version is -1 when validate_delete_file_action";
        return OLAP_ERR_BE_ERROR_DELETE_ACTION;
    }
    Version spec_version = Version(0, rowset->end_version());
    vector<RowsetReaderSharedPtr> rs_readers;
    _tablet->capture_rs_readers(spec_version, &rs_readers);

    if (rs_readers.empty()) {
        LOG(INFO) << "acquire data sources failed. version="
           << spec_version.first << "-" << spec_version.second;
        return OLAP_ERR_BE_ERROR_DELETE_ACTION;
    }

    VLOG(3) << "delete file action is OK";

    return OLAP_SUCCESS;
}

}  // namespace doris
