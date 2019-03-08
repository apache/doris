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
    ReaderType reader_type;
	const TabletSchema* tablet_schema;
    // projection columns
    const std::vector<uint32_t>* return_columns;
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

    RowsetReaderContext() : tablet_schema(nullptr),
        return_columns(nullptr),
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
        runtime_state(nullptr) {
    }
};

class RowsetReaderContextBuilder {
public:
    RowsetReaderContextBuilder& set_reader_type(const ReaderType& reader_type) {
        _reader_context.reader_type = reader_type;
        return *this;
    }

    RowsetReaderContextBuilder& set_tablet_schema(const TabletSchema* tablet_schema) {
        _reader_context.tablet_schema = tablet_schema;
        return *this;
    }

    RowsetReaderContextBuilder& set_return_columns(const std::vector<uint32_t>* return_columns) {
        _reader_context.return_columns = return_columns;
        return *this;
    }

    RowsetReaderContextBuilder& set_load_bf_columns(const std::set<uint32_t>* load_bf_columns) {
        _reader_context.load_bf_columns = load_bf_columns;
        return *this;
    }

    RowsetReaderContextBuilder& set_conditions(const Conditions* conditions) {
        _reader_context.conditions = conditions;
        return *this;
    }

    RowsetReaderContextBuilder& set_predicates(
            const std::vector<ColumnPredicate*>* predicates) {
        _reader_context.predicates = predicates;
        return *this;
    }

    RowsetReaderContextBuilder& set_lower_bound_keys(const std::vector<RowCursor*>* lower_bound_keys) {
        _reader_context.lower_bound_keys = lower_bound_keys;
        return *this;
    }

    RowsetReaderContextBuilder& set_is_lower_keys_included(const std::vector<bool>* is_lower_keys_included) {
        _reader_context.is_lower_keys_included = is_lower_keys_included;
        return *this;
    }

    RowsetReaderContextBuilder& set_upper_bound_keys(const std::vector<RowCursor*>* upper_bound_keys) {
        _reader_context.upper_bound_keys = upper_bound_keys;
        return *this;
    }

    RowsetReaderContextBuilder& set_is_upper_keys_included(const std::vector<bool>* is_upper_keys_included) {
        _reader_context.is_upper_keys_included = is_upper_keys_included;
        return *this;
    }

    RowsetReaderContextBuilder& set_delete_handler(const DeleteHandler* delete_handler) {
        _reader_context.delete_handler = delete_handler;
        return *this;
    }

    RowsetReaderContextBuilder& set_stats(OlapReaderStatistics* stats) {
        _reader_context.stats = stats;
        return *this;
    }

    RowsetReaderContextBuilder& set_is_using_cache(bool is_using_cache) {
        _reader_context.is_using_cache = is_using_cache;
        return *this;
    }

    RowsetReaderContextBuilder& set_lru_cache(Cache* lru_cache) {
        _reader_context.lru_cache = lru_cache;
        return *this;
    }

    RowsetReaderContextBuilder& set_runtime_state(RuntimeState* runtime_state) {
        _reader_context.runtime_state = runtime_state;
        return *this;
    }

    RowsetReaderContext build() {
        return _reader_context;
    }

private:
    RowsetReaderContext _reader_context;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
