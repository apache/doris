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

#ifndef DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H
#define DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H

#include <list>
#include <map>
#include <string>
#include <vector>

#include "olap/merger.h"
#include "olap/column_data.h"
#include "olap/olap_define.h"
#include "olap/olap_table.h"

namespace doris {

class SegmentGroup;

class CumulativeCompaction {
public:
    CumulativeCompaction() :
            _is_init(false),
            _header_locked(false),
            _old_cumulative_layer_point(0),
            _new_cumulative_layer_point(0),
            _max_delta_file_size(0),
            _new_segment_group(NULL) {}

    ~CumulativeCompaction() {}
    
    // 初始化CumulativeCompaction对象，包括:
    // - 检查是否触发cumulative compaction
    // - 计算可合并的delta文件
    //
    // 输入参数：
    // - table 待执行cumulative compaction的olap table
    //
    // 返回值：
    // - 如果触发cumulative compaction，返回OLAP_SUCCESS
    // - 否则，返回对应错误码
    OLAPStatus init(OLAPTablePtr table);

    // 执行cumulative compaction
    //
    // 返回值：
    // - 如果执行成功，返回OLAP_SUCCESS
    // - 如果执行失败，返回相应错误码
    OLAPStatus run();
    
private:

    // 计算可以合并的delta文件，以及新的cumulative层标识点
    //
    // 返回值：
    // - 如果成功，返回OLAP_SUCCESS
    // - 如果不成功，返回相应错误码
    OLAPStatus _calculate_need_merged_versions();

    // 获取table现有的delta文件
    //
    // 输出参数：
    // - delta_versions: 将table现有delta文件的版本号存入该参数
    //
    // 返回值：
    // - 如果成功，返回OLAP_SUCCESS
    // - 如果不成功，返回相应错误码
    OLAPStatus _get_delta_versions(Versions* delta_versions);

    // 找出某一版本文件的前一个版本文件, 即 current_version.first = previous_version.second + 1
    //
    // 输入参数：
    // - current_version: 某一指定版本
    //
    // 输出参数：
    // - previous_version: 待查找的指定版本的前一个版本
    //
    // 返回值：
    // - 如果查找成功，返回true
    // - 如果查找失败，返回false
    bool _find_previous_version(const Version current_version, Version* previous_version);

    // 执行cumulative compaction合并过程
    //
    // 返回值：
    // - 如果成功，返回OLAP_SUCCESS
    // - 如果不成功，返回相应错误码
    OLAPStatus _do_cumulative_compaction();

    // 将合并得到的新cumulative文件载入table
    //
    // 输出参数：
    // - unused_indices: 返回不再使用的delta文件对应的olap index
    //
    // 返回值：
    // - 如果成功，返回OLAP_SUCCESS
    // - 如果不成功，返回相应错误码
    OLAPStatus _update_header(std::vector<SegmentGroup*>* unused_indices);

    // 删除不再使用的delta文件
    //
    // 输入输出参数
    // - unused_indices: 待删除的不再使用的delta文件对应的olap index
    void _delete_unused_delta_files(std::vector<SegmentGroup*>* unused_indices);

    // 验证得到的m_need_merged_versions是否正确
    //
    // 返回值：
    // - 如果错误，返回false
    // - 如果正确，返回true
    bool _validate_need_merged_versions();

    // 验证得到的删除文件操作是否正确; 使用该函数前需要对header文件加锁
    //
    // 返回值：
    // - 如果错误，返回OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION
    // - 如果正确，返回OLAP_SUCCESS
    OLAPStatus _validate_delete_file_action();

    // 恢复header头文件的文件版本和table的data source
    OLAPStatus _roll_back(const std::vector<SegmentGroup*>& old_olap_indices);

    void _obtain_header_rdlock() {
        _table->obtain_header_rdlock();
        _header_locked = true;
    }

    void _obtain_header_wrlock() {
        _table->obtain_header_wrlock();
        _header_locked = true;
    }

    void _release_header_lock() {
        if (_header_locked) {
            _table->release_header_lock();
            _header_locked = false;
        }
    }

    // CumulativeCompaction对象是否初始化
    bool _is_init;
    // header文件是否加锁
    bool _header_locked;
    // table现有的cumulative层的标识点
    int32_t _old_cumulative_layer_point;
    // 待cumulative compaction完成之后，新的cumulative层的标识点
    int32_t _new_cumulative_layer_point;
    // 一个cumulative文件大小的最大值
    // 当delta文件的大小超过该值时，我们认为该delta文件是cumulative文件
    size_t _max_delta_file_size;
    // 待执行cumulative compaction的olap table
    OLAPTablePtr _table;
    // 新cumulative文件的版本
    Version _cumulative_version;
    // 新cumulative文件的version hash
    VersionHash _cumulative_version_hash;
    // 新cumulative文件对应的olap index
    SegmentGroup* _new_segment_group;
    // 可合并的delta文件的data文件
    std::vector<ColumnData*> _data_source;
    // 可合并的delta文件的版本
    std::vector<Version> _need_merged_versions;

    DISALLOW_COPY_AND_ASSIGN(CumulativeCompaction);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H
