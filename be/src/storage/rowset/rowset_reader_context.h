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

#include <optional>
#include <set>
#include <vector>

#include "exprs/score_runtime.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/column_predicate.h"
#include "storage/row_cursor.h"
#include "storage/rowid_conversion.h"
#include "storage/schema.h"
#include "storage/segment/variant/variant_compaction_paths.h"

namespace doris {

class DeleteBitmap;
class DeleteHandler;
class TabletSchema;

// What the TabletReader tells the readers of its rowsets: the read schema, the version read,
// the predicates and key ranges to push down, how the rows are merged, and the knobs of the
// segment reads. It is read-only for the rowset readers; one context serves every rowset of
// the read.
struct RowsetReaderContext {
    ReaderType reader_type = ReaderType::READER_QUERY;
    bool read_changelog = false;
    // The version range of the read, [0, N] for a query.
    Version version {-1, -1};
    // Set only by compaction, alongside its extended tablet_schema.
    VariantCompactionPathsSPtr variant_compaction_paths;
    std::vector<int> topn_filter_source_node_ids;
    MergeDecision merge;
    // ORDER BY key LIMIT n pushed down: every segment stops after this many rows, and the TopN
    // read keeps the best n of every rowset.
    size_t read_orderby_key_limit = 0;
    ReadSchemaSPtr read_schema = nullptr;
    TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
    // column name -> column predicate
    // adding column_name for predicate to make use of column selectivity
    const std::vector<std::shared_ptr<ColumnPredicate>>* predicates = nullptr;
    // value column predicate in UNIQUE table
    const std::vector<std::shared_ptr<ColumnPredicate>>* value_predicates = nullptr;
    const std::vector<std::optional<RowCursor>>* lower_bound_keys = nullptr;
    const std::vector<bool>* is_lower_keys_included = nullptr;
    const std::vector<std::optional<RowCursor>>* upper_bound_keys = nullptr;
    const std::vector<bool>* is_upper_keys_included = nullptr;
    const DeleteHandler* delete_handler = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;
    VExprContextSPtrs common_expr_ctxs_push_down;
    bool use_page_cache = false;
    int batch_size = 1024;
    // Effective adaptive batch size byte budget. 0 means disabled internally.
    size_t preferred_block_size_bytes = 8388608UL;

    // for unique key merge on write
    bool enable_unique_key_merge_on_write = false;
    DeleteBitmapPtr delete_bitmap = nullptr;
    RowIdConversion* rowid_conversion = nullptr;
    // column unique ids referenced by output/orderby exprs (NOT ordinals)
    const std::set<int32_t>* output_columns = nullptr;
    std::set<ColumnId> extra_columns;
    // slots that cast may be eliminated in storage layer
    std::map<std::string, DataTypePtr> target_cast_type_for_variants;
    // Absolute timestamp (seconds since epoch) after which cache blocks filled by this
    // read stop being TTL protected; 0 means no TTL.
    // See TabletMeta::file_cache_ttl_expiration_time().
    int64_t file_cache_expiration_time = 0;

    std::map<ColumnId, VExprContextSPtr> virtual_column_exprs;

    std::map<int32_t, TColumnAccessPaths> all_access_paths;
    std::map<int32_t, TColumnAccessPaths> predicate_access_paths;

    std::shared_ptr<ScoreRuntime> score_runtime;
    CollectionStatisticsPtr collection_statistics;
    std::shared_ptr<segment_v2::AnnTopNRuntime> ann_topn_runtime;

    uint64_t condition_cache_digest = 0;

    // When true, push down value predicates for MOR tables
    bool enable_mor_value_predicate_pushdown = false;

    // General LIMIT budget forwarded to SegmentIterator.
    int64_t general_read_limit = -1;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
