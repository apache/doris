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

#ifndef DORIS_BE_SRC_OLAP_DELETE_HANDLER_H
#define DORIS_BE_SRC_OLAP_DELETE_HANDLER_H

#include <string>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/field.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/olap_table.h"
#include "olap/row_cursor.h"

namespace doris {

// 实现了删除条件的存储，移除和显示功能
// *  存储删除条件：
//    OLAPStatus res;
//    DeleteConditionHandler cond_handler;
//    res = cond_handler.store_cond(olap_table, condition_version, delete_condition);
// *  移除删除条件
//    res = cond_handler.delete_cond(olap_table, condition_version, true);
//    或者
//    res = cond_handler.delete_cond(olap_table, condition_version, false);
// *  将一个table上现存有的所有删除条件打印到log中
//    res = cond_handler.log_conds(olap_table);
// 注:
//    *  在调用这个类存储和移除删除条件时，需要先对Header文件加写锁；
//       并在调用完成之后调用olap_table->save_header()，然后再释放Header文件的锁
//    *  在调用log_conds()的时候，只需要加读锁
class DeleteConditionHandler {
public:
    typedef google::protobuf::RepeatedPtrField<DeleteConditionMessage> del_cond_array;

    DeleteConditionHandler() {}
    ~DeleteConditionHandler() {}

    // 检查cond表示的删除条件是否符合要求；
    // 如果不符合要求，返回OLAP_ERR_DELETE_INVALID_CONDITION；符合要求返回OLAP_SUCCESS
    OLAPStatus check_condition_valid(OLAPTablePtr table, const TCondition& cond);

    // 存储指定版本号的删除条件到Header文件中。因此，调用之前需要对Header文件加写锁
    //
    // 输入参数：
    //     * table：指定删除条件要作用的olap engine表；删除条件就存储在这个表的Header文件中
    //     * version: 删除条件的版本
    //     * del_condition: 用字符串形式表示的删除条件
    // 返回值：
    //     * OLAP_SUCCESS：调用成功
    //     * OLAP_ERR_DELETE_INVALID_PARAMETERS：函数参数不符合要求
    //     * OLAP_ERR_DELETE_INVALID_CONDITION：del_condition不符合要求
    OLAPStatus store_cond(
            OLAPTablePtr table,
            const int32_t version,
            const std::vector<TCondition>& conditions);

    // construct sub condition from TCondition
    std::string construct_sub_conditions(const TCondition& condition);

    // 从Header文件中移除特定版本号的删除条件。在调用之前需要对Header文件加写锁
    //
    // 输入参数：
    //     * table：需要移除删除条件的olap engine表
    //     * version：要移除的删除条件的版本
    //     * delete_smaller_version_conditions:
    //         * 如果true，则移除小于等于指定版本号的删除条件；
    //         * 如果false，则只删除指定版本的删除条件
    // 返回值：
    //     * OLAP_SUCCESS:
    //         * 移除删除条件成功
    //         * 这个表没有任何删除条件
    //         * 这个表没有指定版本号的删除条件
    //     * OLAP_ERR_DELETE_INVALID_PARAMETERS：函数参数不符合要求
    OLAPStatus delete_cond(
            OLAPTablePtr table, const int32_t version, bool delete_smaller_version_conditions);

    // 将一个olap engine的表上存有的所有删除条件打印到log中。调用前只需要给Header文件加读锁
    //
    // 输入参数：
    //     table: 要打印删除条件的olap engine表
    // 返回值：
    //     OLAP_SUCCESS：调用成功
    OLAPStatus log_conds(OLAPTablePtr table);
private:

    // 检查指定的删除条件版本是否符合要求；
    // 如果不符合要求，返回OLAP_ERR_DELETE_INVALID_VERSION；符合要求返回OLAP_SUCCESS
    OLAPStatus _check_version_valid(OLAPTablePtr table, const int32_t filter_version);

    // 检查指定版本的删除条件是否已经存在。如果存在，返回指定版本删除条件的数组下标；不存在返回-1
    int _check_whether_condition_exist(OLAPTablePtr, int cond_version);
};

// 表示一个删除条件
struct DeleteConditions {
    DeleteConditions() : filter_version(0), del_cond(NULL) {}
    ~DeleteConditions() {}

    int32_t filter_version; // 删除条件版本号
    Conditions* del_cond;   // 删除条件
};

// 这个类主要用于判定一条数据(RowCursor)是否符合删除条件。这个类的使用流程如下：
// 1. 使用一个版本号来初始化handler
//    OLAPStatus res;
//    DeleteHandler delete_handler;
//    res = delete_handler.init(olap_table, condition_version);
// 2. 使用这个handler来判定一条数据是否符合删除条件
//    bool filter_data;
//    filter_data = delete_handler.is_filter_data(data_version, row_cursor);
// 3. 如果有多条数据要判断，可重复调用delete_handler.is_filter_data(data_version, row_data)
// 4. 完成所有数据的判断后，需要销毁delete_handler对象
//    delete_handler.finalize();
//
// 注：
//    * 第1步中，在调用init()函数之前，需要对Header文件加读锁
class DeleteHandler {
public:
    typedef std::vector<DeleteConditions>::size_type cond_num_t;
    typedef google::protobuf::RepeatedPtrField<DeleteConditionMessage> del_cond_array;

    DeleteHandler() : _is_inited(false) {}
    ~DeleteHandler() {}

    bool get_init_status() const {
        return _is_inited;
    }

    // 初始化handler，将从Header文件中取出小于等于指定版本号的删除条件填充到_del_conds中
    // 调用前需要先对Header文件加读锁
    //
    // 输入参数：
    //     * olap_table: 删除条件和数据所在的table
    //     * version: 要取出的删除条件版本号
    // 返回值：
    //     * OLAP_SUCCESS: 调用成功
    //     * OLAP_ERR_DELETE_INVALID_PARAMETERS: 参数不符合要求
    //     * OLAP_ERR_MALLOC_ERROR: 在填充_del_conds时，分配内存失败
    OLAPStatus init(OLAPTablePtr olap_table, int32_t version);

    // 判定一条数据是否符合删除条件
    //
    // 输入参数：
    //     * data_version: 待判定数据的版本号
    //     * row: 待判定的一行数据
    // 返回值：
    //     * true: 数据符合删除条件
    //     * false: 数据不符合删除条件
    bool is_filter_data(const int32_t data_version, const RowCursor& row) const;

    // 返回handler中有存有多少条删除条件
    cond_num_t conditions_num() const{
        return _del_conds.size();
    }

    bool empty() const {
        return _del_conds.empty();
    }

    // 返回handler中存有的所有删除条件的版本号
    std::vector<int32_t> get_conds_version();

    // 销毁handler对象
    void finalize();

    // 获取只读删除条件
    const std::vector<DeleteConditions>& get_delete_conditions() const {
        return _del_conds;
    }

private:
    // Use regular expression to extract 'column_name', 'op' and 'operands'
    bool _parse_condition(const std::string& condition_str, TCondition* condition);

    bool _is_inited;
    std::vector<DeleteConditions> _del_conds;
};

}  // namespace doris
#endif // DORIS_BE_SRC_OLAP_DELETE_HANDLER_H
