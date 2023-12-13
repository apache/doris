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

#pragma once

#include <butil/macros.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema.h"
#include "vec/common/arena.h"

namespace doris {

class AndBlockColumnPredicate;
class ColumnPredicate;
class DeletePredicatePB;
class TCondition;

// Represent a delete condition.
struct DeleteConditions {
    int64_t filter_version = 0; // The version of this condition
    std::vector<const ColumnPredicate*> column_predicate_vec;
};

// This class is used for checking whether a row should be deleted.
// It is used in the following processes：
// 1. Create and initialize a DeleteHandler object:
//    Status res;
//    DeleteHandler delete_handler;
//    res = delete_handler.init(tablet, condition_version);
// 2. After all rows have been checked, you should release this object by calling:
//    delete_handler.finalize();
//
// NOTE：
//    * In the first step, before calling delete_handler.init(), you should lock the tablet's header file.
class DeleteHandler {
    // These static method is used to generate delete predicate pb during write or push handler
public:
    // generated DeletePredicatePB by TCondition
    static Status generate_delete_predicate(const TabletSchema& schema,
                                            const std::vector<TCondition>& conditions,
                                            DeletePredicatePB* del_pred);

    static void convert_to_sub_pred_v2(DeletePredicatePB* delete_pred, TabletSchemaSPtr schema);

private:
    // Validate the condition on the schema.
    static Status check_condition_valid(const TabletSchema& tablet_schema, const TCondition& cond);

    // Check whether the condition value is valid according to its type.
    // 1. For integers(int8,int16,in32,int64,uint8,uint16,uint32,uint64), check whether they are overflow
    // 2. For decimal, check whether precision or scale is overflow
    // 3. For date and datetime, check format and value
    // 4. For char and varchar, check length
    static bool is_condition_value_valid(const TabletColumn& column,
                                         const std::string& condition_op,
                                         const std::string& value_str);

    // construct sub condition from TCondition
    static std::string construct_sub_predicate(const TCondition& condition);

    // make operators from FE adaptive to BE
    [[nodiscard]] static std::string trans_op(const string& op);

    // extract 'column_name', 'op' and 'operands' to condition
    static Status parse_condition(const DeleteSubPredicatePB& sub_cond, TCondition* condition);
    static Status parse_condition(const std::string& condition_str, TCondition* condition);

public:
    DeleteHandler() = default;
    ~DeleteHandler() { finalize(); }

    // Initialize DeleteHandler, use the delete conditions of this tablet whose version less than or equal to
    // 'version' to fill '_del_conds'.
    // NOTE: You should lock the tablet's header file before calling this function.
    // input:
    //     * schema: tablet's schema, the delete conditions and data rows are in this schema
    //     * version: maximum version
    //     * with_sub_pred_v2: whether to use delete sub predicate v2 (v2 is based on PB, v1 is based on condition string)
    // return:
    //     * Status::Error<DELETE_INVALID_PARAMETERS>(): input parameters are not valid
    //     * Status::Error<MEM_ALLOC_FAILED>(): alloc memory failed
    Status init(TabletSchemaSPtr tablet_schema,
                const std::vector<RowsetMetaSharedPtr>& delete_preds, int64_t version,
                bool with_sub_pred_v2 = false);

    [[nodiscard]] bool empty() const { return _del_conds.empty(); }

    // Release an instance of this class.
    void finalize();

    void get_delete_conditions_after_version(
            int64_t version, AndBlockColumnPredicate* and_block_column_predicate_ptr,
            std::unordered_map<int32_t, std::vector<const ColumnPredicate*>>*
                    del_predicates_for_zone_map) const;

private:
    template <typename SubPredType>
    Status _parse_column_pred(
            TabletSchemaSPtr complete_schema, TabletSchemaSPtr delete_pred_related_schema,
            const ::google::protobuf::RepeatedPtrField<SubPredType>& sub_pred_list,
            DeleteConditions* delete_conditions);

    bool _is_inited = false;
    // DeleteConditions in _del_conds are in 'OR' relationship
    std::vector<DeleteConditions> _del_conds;
    std::unique_ptr<vectorized::Arena> _predicate_arena;

    DISALLOW_COPY_AND_ASSIGN(DeleteHandler);
};

} // namespace doris
