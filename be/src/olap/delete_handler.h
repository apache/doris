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
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"

namespace doris {

typedef google::protobuf::RepeatedPtrField<DeletePredicatePB> DelPredicateArray;
class Conditions;
class RowCursor;

class DeleteConditionHandler {
public:

    DeleteConditionHandler() {}
    ~DeleteConditionHandler() {}

    // generated DeletePredicatePB by TCondition
    OLAPStatus generate_delete_predicate(const TabletSchema& schema,
                                         const std::vector<TCondition>& conditions,
                                         DeletePredicatePB* del_pred);

    // 检查cond表示的删除条件是否符合要求；
    // 如果不符合要求，返回OLAP_ERR_DELETE_INVALID_CONDITION；符合要求返回OLAP_SUCCESS
    OLAPStatus check_condition_valid(const TabletSchema& tablet_schema, const TCondition& cond);

    // construct sub condition from TCondition
    std::string construct_sub_predicates(const TCondition& condition);

private:

    int32_t _get_field_index(const TabletSchema& schema, const std::string& field_name) const {
        for (int i = 0; i < schema.num_columns(); i++) {
            if (schema.column(i).name() == field_name) {
                return i;
            }
        }
        LOG(WARNING) << "invalid field name. name='" << field_name;
        return -1;
    }

    bool is_condition_value_valid(const TabletColumn& column, const TCondition& cond, const string& value);
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
//    res = delete_handler.init(tablet, condition_version);
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

    DeleteHandler() : _is_inited(false) {}
    ~DeleteHandler() {}

    // 初始化handler，将从Header文件中取出小于等于指定版本号的删除条件填充到_del_conds中
    // 调用前需要先对Header文件加读锁
    //
    // 输入参数：
    //     * tablet: 删除条件和数据所在的tablet
    //     * version: 要取出的删除条件版本号
    // 返回值：
    //     * OLAP_SUCCESS: 调用成功
    //     * OLAP_ERR_DELETE_INVALID_PARAMETERS: 参数不符合要求
    //     * OLAP_ERR_MALLOC_ERROR: 在填充_del_conds时，分配内存失败
    OLAPStatus init(const TabletSchema& schema,
        const DelPredicateArray& delete_conditions, int32_t version);

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

    void get_delete_conditions_after_version(int32_t version,
            std::vector<const Conditions*>* delete_conditions) const;

private:
    // Use regular expression to extract 'column_name', 'op' and 'operands'
    bool _parse_condition(const std::string& condition_str, TCondition* condition);

    bool _is_inited;
    std::vector<DeleteConditions> _del_conds;
};

}  // namespace doris
#endif // DORIS_BE_SRC_OLAP_DELETE_HANDLER_H
