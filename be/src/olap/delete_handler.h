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
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/olap_define.h"

namespace doris {

typedef google::protobuf::RepeatedPtrField<DeletePredicatePB> DelPredicateArray;
class Conditions;
class RowCursor;
class TabletReader;

class DeleteConditionHandler {
public:
    DeleteConditionHandler() {}
    ~DeleteConditionHandler() {}

    // generated DeletePredicatePB by TCondition
    OLAPStatus generate_delete_predicate(const TabletSchema& schema,
                                         const std::vector<TCondition>& conditions,
                                         DeletePredicatePB* del_pred);

    // construct sub condition from TCondition
    std::string construct_sub_predicates(const TCondition& condition);

private:
    // Validate the condition on the schema.
    // Return OLAP_SUCCESS, if valid
    //        OLAP_ERR_DELETE_INVALID_CONDITION, otherwise
    OLAPStatus check_condition_valid(const TabletSchema& tablet_schema, const TCondition& cond);

    // Check whether the condition value is valid according to its type.
    // 1. For integers(int8,int16,in32,int64,uint8,uint16,uint32,uint64), check whether they are overflow
    // 2. For decimal, check whether precision or scale is overflow
    // 3. For date and datetime, check format and value
    // 4. For char and varchar, check length
    bool is_condition_value_valid(const TabletColumn& column, const std::string& condition_op,
                                  const std::string& value_str);
};

// Represent a delete condition.
struct DeleteConditions {
    int64_t filter_version = 0;     // The version of this condition
    Conditions* del_cond = nullptr; // The delete condition
    std::vector<const ColumnPredicate*> column_predicate_vec;
};

// This class is used for checking whether a row should be deleted.
// It is used in the following processes：
// 1. Create and initialize a DeleteHandler object:
//    OLAPStatus res;
//    DeleteHandler delete_handler;
//    res = delete_handler.init(tablet, condition_version);
// 2. Use it to check whether a row should be deleted:
//    bool should_be_deleted = delete_handler.is_filter_data(data_version, row_cursor);
// 3. If there are multiple rows, you can invoke function is_filter_data multiple times:
//    should_be_deleted = delete_handler.is_filter_data(data_version, row_cursor);
// 4. After all rows have been checked, you should release this object by calling:
//    delete_handler.finalize();
//
// NOTE：
//    * In the first step, before calling delete_handler.init(), you should lock the tablet's header file.
class DeleteHandler {
public:
    DeleteHandler() = default;
    ~DeleteHandler() { finalize(); }

    // Initialize DeleteHandler, use the delete conditions of this tablet whose version less than or equal to
    // 'version' to fill '_del_conds'.
    // NOTE: You should lock the tablet's header file before calling this function.
    //
    // input:
    //     * schema: tablet's schema, the delete conditions and data rows are in this schema
    //     * version: maximum version
    // return:
    //     * OLAP_SUCCESS: succeed
    //     * OLAP_ERR_DELETE_INVALID_PARAMETERS: input parameters are not valid
    //     * OLAP_ERR_MALLOC_ERROR: alloc memory failed
    OLAPStatus init(const TabletSchema& schema, const DelPredicateArray& delete_conditions,
                    int64_t version, const doris::TabletReader* = nullptr);

    // Check whether a row should be deleted.
    //
    // input:
    //     * data_version: the version of this row
    //     * row: the row data to be checked
    // return:
    //     * true: this row should be deleted
    //     * false: this row should NOT be deleted
    bool is_filter_data(const int64_t data_version, const RowCursor& row) const;

    // Return the delete conditions' size.
    size_t conditions_num() const { return _del_conds.size(); }

    bool empty() const { return _del_conds.empty(); }

    // Return all the versions of the delete conditions.
    std::vector<int64_t> get_conds_version();

    // Release an instance of this class.
    void finalize();

    // Return all the delete conditions.
    const std::vector<DeleteConditions>& get_delete_conditions() const { return _del_conds; }

    void get_delete_conditions_after_version(
            int64_t version, std::vector<const Conditions*>* delete_conditions,
            AndBlockColumnPredicate* and_block_column_predicate_ptr) const;

private:
    // Use regular expression to extract 'column_name', 'op' and 'operands'
    bool _parse_condition(const std::string& condition_str, TCondition* condition);

    bool _is_inited = false;
    // DeleteConditions in _del_conds are in 'OR' relationship
    std::vector<DeleteConditions> _del_conds;

    DISALLOW_COPY_AND_ASSIGN(DeleteHandler);
};

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_DELETE_HANDLER_H
