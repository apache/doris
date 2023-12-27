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

#include <memory>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "olap/tablet_schema.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"

namespace doris {

class RowCursor;
class Schema;
class ColumnPredicate;

namespace vectorized {
struct IteratorRowRef;
};

namespace segment_v2 {
struct StreamReader;
}

class StorageReadOptions {
public:
    struct KeyRange {
        KeyRange()
                : lower_key(nullptr),
                  include_lower(false),
                  upper_key(nullptr),
                  include_upper(false) {}

        KeyRange(const RowCursor* lower_key_, bool include_lower_, const RowCursor* upper_key_,
                 bool include_upper_)
                : lower_key(lower_key_),
                  include_lower(include_lower_),
                  upper_key(upper_key_),
                  include_upper(include_upper_) {}

        // the lower bound of the range, nullptr if not existed
        const RowCursor* lower_key = nullptr;
        // whether `lower_key` is included in the range
        bool include_lower;
        // the upper bound of the range, nullptr if not existed
        const RowCursor* upper_key = nullptr;
        // whether `upper_key` is included in the range
        bool include_upper;
    };

    // reader's key ranges, empty if not existed.
    // used by short key index to filter row blocks
    std::vector<KeyRange> key_ranges;

    // For unique-key merge-on-write, the effect is similar to delete_conditions
    // that filters out rows that are deleted in realtime.
    // For a particular row, if delete_bitmap.contains(rowid) means that row is
    // marked deleted and invisible to user anymore.
    // segment_id -> roaring::Roaring*
    std::unordered_map<uint32_t, std::shared_ptr<roaring::Roaring>> delete_bitmap;

    std::shared_ptr<AndBlockColumnPredicate> delete_condition_predicates =
            std::make_shared<AndBlockColumnPredicate>();
    // reader's column predicate, nullptr if not existed
    // used to fiter rows in row block
    std::vector<ColumnPredicate*> column_predicates;
    std::vector<ColumnPredicate*> column_predicates_except_leafnode_of_andnode;
    std::unordered_map<int32_t, std::shared_ptr<AndBlockColumnPredicate>> col_id_to_predicates;
    std::unordered_map<int32_t, std::vector<const ColumnPredicate*>> del_predicates_for_zone_map;
    TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;

    // REQUIRED (null is not allowed)
    OlapReaderStatistics* stats = nullptr;
    bool use_page_cache = false;
    int block_row_max = 4096 - 32; // see https://github.com/apache/doris/pull/11816

    TabletSchemaSPtr tablet_schema = nullptr;
    bool record_rowids = false;
    // flag for enable topn opt
    bool use_topn_opt = false;
    // used for special optimization for query : ORDER BY key DESC LIMIT n
    bool read_orderby_key_reverse = false;
    // columns for orderby keys
    std::vector<uint32_t>* read_orderby_key_columns = nullptr;
    io::IOContext io_ctx;
    vectorized::VExpr* remaining_vconjunct_root = nullptr;
    std::vector<vectorized::VExprSPtr> remaining_conjunct_roots;
    vectorized::VExprContextSPtrs common_expr_ctxs_push_down;
    const std::set<int32_t>* output_columns = nullptr;
    // runtime state
    RuntimeState* runtime_state = nullptr;
    RowsetId rowset_id;
    Version version;
    int32_t tablet_id = 0;
    // slots that cast may be eliminated in storage layer
    std::map<std::string, PrimitiveType> target_cast_type_for_variants;
    RowRanges row_ranges;
};

class RowwiseIterator;
using RowwiseIteratorUPtr = std::unique_ptr<RowwiseIterator>;
class RowwiseIterator {
public:
    RowwiseIterator() = default;
    virtual ~RowwiseIterator() = default;

    // Initialize this iterator and make it ready to read with
    // input options.
    // Input options may contain scan range in which this scan.
    // Return Status::OK() if init successfully,
    // Return other error otherwise
    virtual Status init(const StorageReadOptions& opts) = 0;

    // If there is any valid data, this function will load data
    // into input batch with Status::OK() returned
    // If there is no data to read, will return Status::EndOfFile.
    // If other error happens, other error code will be returned.
    virtual Status next_batch(vectorized::Block* block) {
        return Status::NotSupported("to be implemented");
    }

    virtual Status next_block_view(vectorized::BlockView* block_view) {
        return Status::NotSupported("to be implemented");
    }

    virtual Status next_row(vectorized::IteratorRowRef* ref) {
        return Status::NotSupported("to be implemented");
    }
    virtual Status unique_key_next_row(vectorized::IteratorRowRef* ref) {
        return Status::NotSupported("to be implemented");
    }

    virtual bool support_return_data_by_ref() { return false; }

    virtual Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) {
        return Status::NotSupported("to be implemented");
    }

    // return schema for this Iterator
    virtual const Schema& schema() const = 0;

    // Only used by UT. Whether lazy-materialization-read is used by this iterator or not.
    virtual bool is_lazy_materialization_read() const { return false; }

    // Return the data id such as segment id, used for keep the insert order when do
    // merge sort in priority queue
    virtual uint64_t data_id() const { return 0; }

    virtual bool update_profile(RuntimeProfile* profile) { return false; }
    // return rows merged count by iterator
    virtual uint64_t merged_rows() const { return 0; }

    // return if it's an empty iterator
    virtual bool empty() const { return false; }
};

} // namespace doris
