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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H

#include "olap/olap_common.h"
#include "olap/column_predicate.h"
#include "runtime/runtime_state.h"

namespace doris {

class RowCursor;
class Conditions;
class DeleteHandler;
class TabletSchema;

struct RowsetReaderContext {
    ReaderType reader_type = READER_QUERY;
    const TabletSchema* tablet_schema = nullptr;
    // whether rowset should return ordered rows.
    bool need_ordered_result = true;
    // projection columns
    const std::vector<uint32_t>* return_columns = nullptr;
    const std::vector<uint32_t>* seek_columns = nullptr;
    // columns to load bloom filter index
    // including columns in "=" or "in" conditions
    const std::set<uint32_t>* load_bf_columns = nullptr;
    // column filter conditions by delete sql
    const Conditions* conditions = nullptr;
    // column name -> column predicate
    // adding column_name for predicate to make use of column selectivity
    const std::vector<ColumnPredicate*>* predicates = nullptr;
    const std::vector<RowCursor*>* lower_bound_keys = nullptr;
    const std::vector<bool>* is_lower_keys_included = nullptr;
    const std::vector<RowCursor*>* upper_bound_keys = nullptr;
    const std::vector<bool>* is_upper_keys_included = nullptr;
    const DeleteHandler* delete_handler = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
