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

#include "olap/schema.h"
#include "olap/column_predicate.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"
#include "olap/lru_cache.h"
#include "olap/olap_cond.h"
#include "olap/delete_handler.h"
#include "runtime/runtime_state.h"

namespace doris {

struct RowsetReaderContext {
    RowsetReaderContext() : reader_type(READER_QUERY),
        tablet_schema(nullptr),
        preaggregation(false),
        return_columns(nullptr),
        seek_columns(nullptr),
        load_bf_columns(nullptr),
        conditions(nullptr),
        predicates(nullptr),
        lower_bound_keys(nullptr),
        is_lower_keys_included(nullptr),
        upper_bound_keys(nullptr),
        is_upper_keys_included(nullptr),
        delete_handler(nullptr),
        stats(nullptr),
        is_using_cache(false),
        lru_cache(nullptr),
        runtime_state(nullptr) { }

    ReaderType reader_type;
    const TabletSchema* tablet_schema;
    bool preaggregation;
    // projection columns
    const std::vector<uint32_t>* return_columns;
    const std::vector<uint32_t>* seek_columns;
    // columns to load bloom filter index
    // including columns in "=" or "in" conditions
    const std::set<uint32_t>* load_bf_columns;
    // column filter conditions by delete sql
    const Conditions* conditions;
    // column name -> column predicate
    // adding column_name for predicate to make use of column selectivity
    const std::vector<ColumnPredicate*>* predicates;
    const std::vector<RowCursor*>* lower_bound_keys;
    const std::vector<bool>* is_lower_keys_included;
    const std::vector<RowCursor*>* upper_bound_keys;
    const std::vector<bool>* is_upper_keys_included;
    const DeleteHandler* delete_handler;
    OlapReaderStatistics* stats;
    bool is_using_cache;
    Cache* lru_cache;
    RuntimeState* runtime_state;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
