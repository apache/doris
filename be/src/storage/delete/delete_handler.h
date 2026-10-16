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

#include <cstdint>
#include <string>
#include <type_traits>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/arena.h"
#include "storage/predicate/column_predicate.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class AndBlockColumnPredicate;
class ColumnPredicate;
class DeletePredicatePB;
class TCondition;

// Represent a delete condition.
struct DeleteConditions {
    int64_t filter_version = 0; // The version of this condition
    std::vector<std::shared_ptr<const ColumnPredicate>> column_predicate_vec;
};

// This class resolves persisted delete conditions to read-schema ordinals and evaluates them while
// reading rows.
class DeleteHandler {
    ENABLE_FACTORY_CREATOR(DeleteHandler);

public:
    struct ConditionParseResult {
        int32_t col_unique_id;
        std::string column_name;
        PredicateType condition_op;
        std::list<std::string> value_str;
    };
    // generated DeletePredicatePB by TCondition
    static Status generate_delete_predicate(const TabletSchema& schema,
                                            const std::vector<TCondition>& conditions,
                                            DeletePredicatePB* del_pred);

    static Status convert_to_sub_pred_v2(DeletePredicatePB* delete_pred, TabletSchemaSPtr schema);

    /**
     * Use regular expression to extract 'column_name', 'op' and 'operands'
     *
     * @param condition_str input predicate string in form of `X OP Y`
     * @param condition output param
     * @return OK if matched and extracted correctly otherwise DELETE_INVALID_PARAMETERS
     */
    static ConditionParseResult parse_condition(const std::string& condition_str);
    static ConditionParseResult parse_condition(const DeleteSubPredicatePB& sub_cond);
    static PredicateType parse_condition_op(const std::string& op_str,
                                            const std::list<std::string>& cond_values);

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

public:
    DeleteHandler() = default;
    ~DeleteHandler();

    // Initialize DeleteHandler, use the delete conditions whose version is less
    // than or equal to 'version' to fill '_del_conds'.
    //
    // Delete-condition columns that are absent from `read_schema` are resolved
    // against the schema stored in the corresponding delete-predicate rowset and
    // returned through `dropped_columns`.
    // NOTE: You should lock the tablet's header file before calling this function.
    // input:
    //     * version: maximum version
    //     * read_schema: schema used to bind delete-predicate column ordinals
    // output:
    //     * dropped_columns: missing delete-predicate columns in append order
    // return:
    //     * Status::Error<DELETE_INVALID_PARAMETERS>(): input parameters are not valid
    //     * Status::Error<MEM_ALLOC_FAILED>(): alloc memory failed
    Status init(const std::vector<RowsetMetaSharedPtr>& delete_preds, int64_t version,
                const ReadSchemaSPtr& read_schema, std::vector<TabletColumn>* dropped_columns);

    [[nodiscard]] bool empty() const { return _del_conds.empty(); }

    void get_delete_conditions_after_version(
            int64_t version, AndBlockColumnPredicate* and_block_column_predicate_ptr,
            std::unordered_map<int32_t, std::vector<std::shared_ptr<const ColumnPredicate>>>*
                    del_predicates_for_zone_map) const;

private:
    template <typename SubPredType>
        requires(std::is_same_v<SubPredType, DeleteSubPredicatePB> or
                 std::is_same_v<SubPredType, std::string>)
    Status _parse_column_pred(
            const ReadSchema& read_schema, const TabletSchemaSPtr& delete_pred_related_schema,
            const ::google::protobuf::RepeatedPtrField<SubPredType>& sub_pred_list,
            DeleteConditions* delete_conditions, std::vector<TabletColumn>* dropped_columns);

    // Resolve in order:
    // 1. Look up ReadSchema by predicate UID when present.
    // 2. Resolve the historical TabletColumn in the predicate rowset schema by predicate UID, or
    //    by predicate name for legacy metadata without a UID.
    // 3. Look up ReadSchema by the historical TabletColumn's UID.
    // 4. Look up dropped_columns by the historical column's UID.
    // 5. Append the historical column to dropped_columns when no match exists.
    // Example: if old x(uid=10) is dropped and x(uid=20) is added, step 2 recovers uid=10 and avoids
    // binding the predicate to uid=20.
    static Status _resolve_column(const ReadSchema& read_schema, int32_t col_unique_id,
                                  const std::string& column_name,
                                  const TabletSchemaSPtr& delete_pred_related_schema,
                                  ColumnId* column_id, const TabletColumn** column,
                                  std::vector<TabletColumn>* dropped_columns);

    bool _is_inited = false;
    // DeleteConditions in _del_conds are in 'OR' relationship
    std::vector<DeleteConditions> _del_conds;
    Arena _predicate_arena;

    DISALLOW_COPY_AND_ASSIGN(DeleteHandler);
};

} // namespace doris
