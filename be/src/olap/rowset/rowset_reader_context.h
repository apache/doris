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

#include "io/io_common.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

class RowCursor;
class DeleteBitmap;
class DeleteHandler;
class TabletSchema;

struct RowsetReaderContext {
    ReaderType reader_type = ReaderType::READER_QUERY;
    Version version {-1, -1};
    TabletSchemaSPtr tablet_schema = nullptr;
    std::vector<int> topn_filter_source_node_ids;
    int topn_filter_target_node_id = -1;
    // whether rowset should return ordered rows.
    bool need_ordered_result = true;
    // used for special optimization for query : ORDER BY key DESC LIMIT n
    bool read_orderby_key_reverse = false;
    // columns for orderby keys
    std::vector<uint32_t>* read_orderby_key_columns = nullptr;
    // limit of rows for read_orderby_key
    size_t read_orderby_key_limit = 0;
    // filter_block arguments
    vectorized::VExprContextSPtrs filter_block_conjuncts;
    // projection columns: the set of columns rowset reader should return
    const std::vector<uint32_t>* return_columns = nullptr;
    TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
    // column name -> column predicate
    // adding column_name for predicate to make use of column selectivity
    const std::vector<ColumnPredicate*>* predicates = nullptr;
    // value column predicate in UNIQUE table
    const std::vector<ColumnPredicate*>* value_predicates = nullptr;
    const std::vector<RowCursor>* lower_bound_keys = nullptr;
    const std::vector<bool>* is_lower_keys_included = nullptr;
    const std::vector<RowCursor>* upper_bound_keys = nullptr;
    const std::vector<bool>* is_upper_keys_included = nullptr;
    const DeleteHandler* delete_handler = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;
    std::vector<vectorized::VExprSPtr> remaining_conjunct_roots;
    vectorized::VExprContextSPtrs common_expr_ctxs_push_down;
    bool use_page_cache = false;
    int sequence_id_idx = -1;
    int batch_size = 1024;
    bool is_unique = false;
    //record row num merged in generic iterator
    uint64_t* merged_rows = nullptr;
    // for unique key merge on write
    bool enable_unique_key_merge_on_write = false;
    const DeleteBitmap* delete_bitmap = nullptr;
    bool record_rowids = false;
    bool is_vertical_compaction = false;
    bool is_key_column_group = false;
    const std::set<int32_t>* output_columns = nullptr;
    RowsetId rowset_id;
    // slots that cast may be eliminated in storage layer
    std::map<std::string, TypeDescriptor> target_cast_type_for_variants;
    int64_t ttl_seconds = 0;
    size_t topn_limit = 0;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
