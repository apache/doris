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

#include <string>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema.h"

namespace doris {

class RowCursor;
class Tablet;
class TabletReader;
class TabletSchema;

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

    // construct sub condition from TCondition
    static std::string construct_sub_predicates(const TCondition& condition);

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
    ~DeleteHandler() { finalize(); }

    // Initialize DeleteHandler, use the delete conditions of this tablet whose version less than or equal to
    // 'version' to fill '_del_conds'.
    // NOTE: You should lock the tablet's header file before calling this function.
    //
    // input:
    //     * schema: tablet's schema, the delete conditions and data rows are in this schema
    //     * version: maximum version
    // return:
    //     * Status::Error<DELETE_INVALID_PARAMETERS>(): input parameters are not valid
    //     * Status::Error<MEM_ALLOC_FAILED>(): alloc memory failed
    Status init(TabletSchemaSPtr tablet_schema,
                const std::vector<RowsetMetaSharedPtr>& delete_conditions, int64_t version);

    bool empty() const { return _del_conds.empty(); }

    // Release an instance of this class.
    void finalize();

    void get_delete_conditions_after_version(
            int64_t version, AndBlockColumnPredicate* and_block_column_predicate_ptr,
            std::unordered_map<int32_t, std::vector<const ColumnPredicate*>>*
                    col_id_to_del_predicates) const;

private:
    // Use regular expression to extract 'column_name', 'op' and 'operands'
    bool _parse_condition(const std::string& condition_str, TCondition* condition);

    bool _is_inited = false;
    // DeleteConditions in _del_conds are in 'OR' relationship
    std::vector<DeleteConditions> _del_conds;
    std::unique_ptr<MemPool> _predicate_mem_pool;

    DISALLOW_COPY_AND_ASSIGN(DeleteHandler);
};

} // namespace doris
