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

#include "storage/index/inverted/reader/scoring_delete_predicates.h"

#include <algorithm>
#include <array>
#include <numeric>
#include <set>
#include <unordered_map>

#include "core/custom_allocator.h"
#include "runtime/runtime_state.h"
#include "storage/iterators.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"

namespace doris {

Status ScoringDeletePredicates::init(const std::vector<RowsetMetaSharedPtr>& predicates,
                                     int64_t read_version, const TabletSchemaSPtr& tablet_schema) {
    // Match a scan's current types for existing UIDs (including widened columns), then let
    // DeleteHandler recover dropped UIDs from the schema that recorded each DELETE. A same-name
    // replacement must not reinterpret a predicate on the dropped column.
    _schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    std::vector<TabletColumn> columns;
    RETURN_IF_ERROR(_handler.init(predicates, read_version, _schema, &columns));
    _schema->append_dropped_columns(std::move(columns));
    return Status::OK();
}

Status ScoringDeletePredicates::collect(const std::shared_ptr<segment_v2::Segment>& segment,
                                        int64_t rowset_version,
                                        const StorageReadOptions& read_options, RuntimeState* state,
                                        const roaring::Roaring& already_hidden,
                                        DorisVector<uint32_t>* hidden) const {
    hidden->clear();
    auto predicates = AndBlockColumnPredicate::create_shared();
    std::unordered_map<int32_t, std::vector<std::shared_ptr<const ColumnPredicate>>> zone_map_preds;
    // A DELETE at version 5 affects a rowset ending at 4, never one inserted at version 6.
    _handler.get_delete_conditions_after_version(rowset_version, predicates.get(), &zone_map_preds);
    if (predicates->num_of_column_predicate() == 0) {
        return Status::OK();
    }
    std::set<ColumnId> ordinals;
    predicates->get_all_column_ids(ordinals);
    DorisVector<std::unique_ptr<segment_v2::ColumnIterator>> iterators(_schema->num_read_columns());
    segment_v2::ColumnIteratorOptions options {
            .use_page_cache = read_options.use_page_cache,
            .file_reader = segment->file_reader().get(),
            .stats = read_options.stats,
            .io_ctx = read_options.io_ctx,
    };
    for (auto ordinal : ordinals) {
        RETURN_IF_ERROR(segment->new_column_iterator(*_schema->column(ordinal), &iterators[ordinal],
                                                     &read_options));
        RETURN_IF_ERROR(iterators[ordinal]->init(options));
        RETURN_IF_ERROR(iterators[ordinal]->seek_to_ordinal(0));
    }

    constexpr uint16_t kBatch = 4096;
    std::array<uint16_t, kBatch> selected;
    for (uint32_t begin = 0; begin < segment->num_rows();) {
        if (state != nullptr) {
            RETURN_IF_CANCELLED(state);
        }
        const auto rows =
                static_cast<uint16_t>(std::min<uint32_t>(kBatch, segment->num_rows() - begin));
        MutableColumns columns(_schema->num_read_columns());
        for (auto ordinal : ordinals) {
            // Canonical columns decode dictionary values, so the normal predicate evaluator
            // sees values rather than dictionary codes requiring a separate conversion step.
            columns[ordinal] = _schema->data_type(ordinal)->create_column();
            size_t read_rows = rows;
            RETURN_IF_ERROR(iterators[ordinal]->next_batch(&read_rows, columns[ordinal]));
            if (read_rows != rows) {
                return Status::Corruption(
                        "segment {} predicate column {} ended after {} of {} rows", segment->id(),
                        ordinal, begin + read_rows, segment->num_rows());
            }
        }
        std::iota(selected.begin(), selected.begin() + rows, uint16_t {0});
        // DeleteHandler builds the keep predicate, including OR within a compound DELETE.
        // evaluate() supports that tree; evaluate_vec() does not support its OR nodes.
        const auto kept = predicates->evaluate(columns, selected.data(), rows);
        size_t next = 0;
        for (uint16_t row = 0; row < rows; ++row) {
            if (next < kept && selected[next] == row) {
                ++next;
            } else if (!already_hidden.contains(begin + row)) {
                // Keep the added row IDs tracked and disjoint from bitmap/delete-sign rows.
                // The caller can merge the two sorted lists without an untracked growing bitmap.
                hidden->push_back(begin + row);
            }
        }
        begin += rows;
    }
    return Status::OK();
}
} // namespace doris
