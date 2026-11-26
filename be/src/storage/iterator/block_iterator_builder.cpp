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

#include "storage/iterator/block_iterator_builder.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/iterator/changelog_detail_block_iterator.h"
#include "storage/iterator/changelog_min_delta_block_iterator.h"
#include "storage/iterator/direct_iterator.h"
#include "storage/iterator/heap_merge_iterator.h"
#include "storage/iterator/merge_block_iterator.h"
#include "storage/iterator/sequence_map_block_iterator.h"
#include "storage/iterator/topn_block_iterator.h"
#include "storage/iterator/unique_key_block_iterator.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {

namespace {

// Pins `rs_reader` (its rowset and StorageReadOptions) until `inner` is destroyed at segment EOF,
// so merged reads release each RowsetReader as soon as its last segment child finishes.
class RowsetBoundIterator final : public RowwiseIterator {
public:
    RowsetBoundIterator(RowsetReaderSharedPtr rs_reader, RowwiseIteratorUPtr inner)
            : _rs_reader(std::move(rs_reader)), _inner(std::move(inner)) {}

    Status init(const StorageReadOptions& opts) override { return _inner->init(opts); }
    Status next_batch(Block* block) override { return _inner->next_batch(block); }
    const ReadSchema& schema() const override { return _inner->schema(); }
    void update_profile(RuntimeProfile* profile) override { _inner->update_profile(profile); }

private:
    // Declared before `_inner` so `_inner` is destroyed first while `_rs_reader` is still alive.
    RowsetReaderSharedPtr _rs_reader;
    RowwiseIteratorUPtr _inner;
};

class BlockIteratorBuilder {
public:
    BlockIteratorBuilder(const TabletReader::ReaderParams& read_params,
                         BlockIteratorBuildContext ctx)
            : _read_params(read_params), _ctx(std::move(ctx)) {}

    Status build(BlockIterMode mode, std::unique_ptr<BlockIterator>* out_iter);

private:
    Status _validate_merge_schema(const HeapMergeOrder& order) const;
    Status _build_heap_merge_iter(HeapMergeOrder order, RowwiseIteratorUPtr* merge_iter);
    Status _init_direct_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_ordered_block_iter(HeapMergeOrder order, std::unique_ptr<BlockIterator>* out_iter);
    Status _init_changelog_input_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_key_ordered_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_unique_key_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_sequence_map_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_agg_key_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_topn_iter(std::unique_ptr<BlockIterator>* out_iter);

    const TabletReader::ReaderParams& _read_params;
    BlockIteratorBuildContext _ctx;
};

Status BlockIteratorBuilder::build(BlockIterMode mode, std::unique_ptr<BlockIterator>* out_iter) {
    switch (mode) {
    case BlockIterMode::TOPN:
        return _init_topn_iter(out_iter);
    case BlockIterMode::CHANGELOG_MIN_DELTA: {
        std::unique_ptr<BlockIterator> input;
        RETURN_IF_ERROR(_init_changelog_input_iter(&input));
        *out_iter = std::make_unique<ChangelogMinDeltaBlockIterator>(
                std::move(input), _ctx.batch_limit, _ctx.read_schema);
        return Status::OK();
    }
    case BlockIterMode::CHANGELOG_DETAIL: {
        std::unique_ptr<BlockIterator> input;
        RETURN_IF_ERROR(_init_changelog_input_iter(&input));
        *out_iter = std::make_unique<ChangelogDetailBlockIterator>(
                std::move(input), _ctx.batch_limit, _ctx.read_schema);
        return Status::OK();
    }
    case BlockIterMode::DIRECT:
        return _init_direct_iter(out_iter);
    case BlockIterMode::KEY_ORDERED:
        return _init_key_ordered_iter(out_iter);
    case BlockIterMode::UNIQUE_NEWEST:
        return _init_unique_key_iter(out_iter);
    case BlockIterMode::SEQUENCE_MAP:
        return _init_sequence_map_iter(out_iter);
    case BlockIterMode::AGG_FOLD:
        return _init_agg_key_iter(out_iter);
    case BlockIterMode::NONE:
        DORIS_CHECK(false);
    }
    return Status::OK();
}

Status BlockIteratorBuilder::_validate_merge_schema(const HeapMergeOrder& order) const {
    const size_t num_block_columns = _ctx.read_schema->num_block_columns();
    const size_t num_key_columns = _ctx.read_schema->num_key_columns();
    auto contract_error = [&](std::string_view detail) {
        return Status::InternalError(
                "merge heap compare contract violated: {}, tablet_id={}, read_columns={}, "
                "num_key_columns={}",
                detail, _ctx.tablet.tablet_id(), num_block_columns, num_key_columns);
    };
    if (order.uses_all_keys()) {
        if (num_key_columns > num_block_columns) {
            return contract_error("read schema has fewer columns than the key prefix");
        }
        for (size_t col_idx = 0; col_idx < num_key_columns; ++col_idx) {
            if (!_ctx.read_schema->column(col_idx)->is_key()) {
                return contract_error("read schema key prefix contains a non-key column");
            }
        }
    }
    for (const auto& [col_idx, _] : order.resolve_sort_cols(num_key_columns)) {
        if (col_idx >= num_block_columns) {
            return contract_error("compare column index out of range");
        }
    }
    return Status::OK();
}

Status BlockIteratorBuilder::_build_heap_merge_iter(HeapMergeOrder order,
                                                    RowwiseIteratorUPtr* merge_iter) {
    RETURN_IF_ERROR(_validate_merge_schema(order));
    std::vector<RowwiseIteratorUPtr> children;
    RuntimeState* runtime_state = _read_params.runtime_state;
    {
        SCOPED_RAW_TIMER(&_ctx.stats.block_reader_rs_readers_init_timer_ns);
        int64_t previous_start_version = -1;
        std::vector<RowwiseIteratorUPtr> rs_children;
        for (const auto& rs_split : _read_params.rs_splits) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }
            const RowsetReaderSharedPtr& rs_reader = rs_split.rs_reader;
            RETURN_IF_ERROR(rs_reader->init(&_ctx.reader_context, rs_split));
            DORIS_CHECK_GE(rs_reader->rowset()->start_version(), previous_start_version);
            previous_start_version = rs_reader->rowset()->start_version();
            rs_children.clear();
            RETURN_IF_ERROR(
                    rs_reader->get_key_ordered_iterators(&_ctx.reader_context, &rs_children));
            for (auto& child : rs_children) {
                children.push_back(
                        std::make_unique<RowsetBoundIterator>(rs_reader, std::move(child)));
            }
        }
    }

    SCOPED_RAW_TIMER(&_ctx.stats.block_reader_build_heap_init_timer_ns);
    *merge_iter = std::make_unique<VHeapMergeIterator>(std::move(children), _ctx.read_schema,
                                                       std::move(order));
    StorageReadOptions opts(_ctx.stats);
    opts.block_row_max = cast_set<uint32_t>(_ctx.batch_limit.max_rows());
    opts.preferred_block_size_bytes = _ctx.batch_limit.preferred_bytes();
    opts.runtime_state = runtime_state;
    return (*merge_iter)->init(opts);
}

Status BlockIteratorBuilder::_init_direct_iter(std::unique_ptr<BlockIterator>* out_iter) {
    const size_t read_limit = _ctx.reader_context.general_read_limit > 0
                                      ? static_cast<size_t>(_ctx.reader_context.general_read_limit)
                                      : 0;
    std::vector<RowsetReaderSharedPtr> rs_readers;
    rs_readers.reserve(_read_params.rs_splits.size());
    RuntimeState* runtime_state = _read_params.runtime_state;
    {
        SCOPED_RAW_TIMER(&_ctx.stats.block_reader_rs_readers_init_timer_ns);
        for (const auto& rs_split : _read_params.rs_splits) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }
            if (read_limit > 0) {
                rs_split.rs_reader->set_topn_limit(read_limit);
            }
            RETURN_IF_ERROR(rs_split.rs_reader->init(&_ctx.reader_context, rs_split));
            rs_readers.push_back(rs_split.rs_reader);
        }
    }
    *out_iter = std::make_unique<DirectIterator>(std::move(rs_readers), &_ctx.reader_context,
                                                 &_ctx.stats);
    return Status::OK();
}

Status BlockIteratorBuilder::_init_ordered_block_iter(HeapMergeOrder order,
                                                      std::unique_ptr<BlockIterator>* out_iter) {
    if (!rowsets_not_mono_asc_disjoint(_read_params)) {
        return _init_direct_iter(out_iter);
    }
    RowwiseIteratorUPtr merge_iter;
    RETURN_IF_ERROR(_build_heap_merge_iter(std::move(order), &merge_iter));
    *out_iter = std::make_unique<MergeBlockIterator>(std::move(merge_iter));
    return Status::OK();
}

Status BlockIteratorBuilder::_init_changelog_input_iter(std::unique_ptr<BlockIterator>* out_iter) {
    const int32_t tso_col_idx = _ctx.read_schema->tso_ordinal();
    HeapMergeOrder order = tso_col_idx >= 0
                                   ? HeapMergeOrder::by_all_keys_and_same_key_col(
                                             tso_col_idx, HeapSortOrder::ASC, HeapSortOrder::ASC)
                                   : HeapMergeOrder::by_all_keys(HeapSortOrder::ASC);
    return _init_ordered_block_iter(std::move(order), out_iter);
}

Status BlockIteratorBuilder::_init_key_ordered_iter(std::unique_ptr<BlockIterator>* out_iter) {
    const auto child_order = _ctx.reader_context.use_insert_order_when_same ? HeapSortOrder::ASC
                                                                            : HeapSortOrder::DESC;
    const int32_t tso_col_idx = _ctx.read_schema->tso_ordinal();
    const int32_t seq_col_idx = _ctx.read_schema->sequence_ordinal();
    HeapMergeOrder order;
    if (tso_col_idx >= 0) {
        order = HeapMergeOrder::by_all_keys_and_same_key_col(tso_col_idx, HeapSortOrder::ASC,
                                                             child_order);
    } else if (seq_col_idx >= 0) {
        order = HeapMergeOrder::by_all_keys_and_same_key_col(seq_col_idx, HeapSortOrder::DESC,
                                                             child_order);
    } else {
        order = HeapMergeOrder::by_all_keys(child_order);
    }
    return _init_ordered_block_iter(std::move(order), out_iter);
}

Status BlockIteratorBuilder::_init_unique_key_iter(std::unique_ptr<BlockIterator>* out_iter) {
    DORIS_CHECK(!_ctx.reader_context.read_changelog);
    DORIS_CHECK(!_read_params.read_orderby_key);

    if (!rowsets_not_mono_asc_disjoint(_read_params)) {
        return _init_direct_iter(out_iter);
    }

    const int32_t seq_col_idx = _ctx.read_schema->sequence_ordinal();
    HeapMergeOrder order = seq_col_idx >= 0 ? HeapMergeOrder::by_all_keys_and_same_key_col(
                                                      seq_col_idx, HeapSortOrder::DESC)
                                            : HeapMergeOrder::by_all_keys();
    RowwiseIteratorUPtr merge_iter;
    RETURN_IF_ERROR(_build_heap_merge_iter(std::move(order), &merge_iter));
    *out_iter = std::make_unique<UniqueKeyBlockIterator>(std::move(merge_iter), _ctx.read_schema,
                                                         _ctx.merged_rows);
    return Status::OK();
}

Status BlockIteratorBuilder::_init_sequence_map_iter(std::unique_ptr<BlockIterator>* out_iter) {
    DORIS_CHECK(!_ctx.reader_context.read_changelog);
    DORIS_CHECK(!_read_params.read_orderby_key);

    if (!rowsets_not_mono_asc_disjoint(_read_params)) {
        return _init_direct_iter(out_iter);
    }

    RowwiseIteratorUPtr merge_iter;
    RETURN_IF_ERROR(_build_heap_merge_iter(HeapMergeOrder::by_all_keys(), &merge_iter));
    *out_iter = std::make_unique<SequenceMapBlockIterator>(std::move(merge_iter), _ctx.batch_limit,
                                                           _ctx.read_schema, _ctx.merged_rows);
    return Status::OK();
}

Status BlockIteratorBuilder::_init_agg_key_iter(std::unique_ptr<BlockIterator>* out_iter) {
    DORIS_CHECK(!_ctx.reader_context.read_changelog);
    DORIS_CHECK(!_read_params.read_orderby_key);
    DORIS_CHECK(_ctx.reader_context.general_read_limit <= 0);

    RowwiseIteratorUPtr merge_iter;
    RETURN_IF_ERROR(_build_heap_merge_iter(HeapMergeOrder::by_all_keys(), &merge_iter));
    auto agg_iter = std::make_unique<AggKeyBlockIterator>(
            std::move(merge_iter), _ctx.batch_limit, _ctx.read_schema,
            _read_params.get_be_exec_version(), _ctx.merged_rows);
    Status st = agg_iter->init();
    if (!st.ok()) {
        st.prepend(fmt::format(
                "Failed to init reader when init agg state: tablet_id: {}, schema_hash: {}, "
                "reader_type: {}, version: {}: ",
                _read_params.tablet->tablet_id(), _read_params.tablet->schema_hash(),
                int(_read_params.reader_type), _read_params.version.to_string()));
        return st;
    }
    *out_iter = std::move(agg_iter);
    return Status::OK();
}

Status BlockIteratorBuilder::_init_topn_iter(std::unique_ptr<BlockIterator>* out_iter) {
    // The scanner pushes a TopN down only without a LIMIT, and TabletReader sets up its ORDER BY
    // columns.
    RowsetReaderContext* reader_ctx = &_ctx.reader_context;
    DORIS_CHECK(reader_ctx->general_read_limit <= 0);
    DORIS_CHECK(reader_ctx->read_orderby_key_columns != nullptr);
    const size_t limit = reader_ctx->read_orderby_key_limit;
    std::vector<TopNBlockIterator::RowsetOpener> rowsets;
    for (const auto& rs_split : _read_params.rs_splits) {
        // A segment gives `limit` rows at most: its later rows cannot be among the first `limit`.
        rs_split.rs_reader->set_topn_limit(limit);
        rowsets.emplace_back([reader_ctx, rs_split](std::vector<RowwiseIteratorUPtr>* streams) {
            RETURN_IF_ERROR(rs_split.rs_reader->init(reader_ctx, rs_split));
            // Each stream gives its rows in key order, which the ORDER BY columns lead.
            return rs_split.rs_reader->get_key_ordered_iterators(reader_ctx, streams);
        });
    }
    // Newer rowsets tend to hold larger keys, so a descending read starts from the newest.
    if (reader_ctx->read_orderby_key_reverse) {
        std::ranges::reverse(rowsets);
    }
    std::map<uint32_t, DataTypePtr> virtual_column_types;
    for (const auto& [position, expr_ctx] : reader_ctx->virtual_column_exprs) {
        virtual_column_types.emplace(position, expr_ctx->root()->data_type());
    }
    TopNBlockIterator::FilterUpdater update_filter;
    if (!reader_ctx->topn_filter_source_node_ids.empty()) {
        update_filter = [reader_ctx](const Field& value) {
            auto* query_ctx = reader_ctx->runtime_state->get_query_ctx();
            for (int id : reader_ctx->topn_filter_source_node_ids) {
                RETURN_IF_ERROR(query_ctx->get_runtime_predicate(id).update(value));
            }
            return Status::OK();
        };
    }
    *out_iter = std::make_unique<TopNBlockIterator>(
            std::move(rowsets), limit, *reader_ctx->read_orderby_key_columns,
            reader_ctx->read_orderby_key_reverse, std::move(virtual_column_types),
            std::move(update_filter));
    return Status::OK();
}

} // namespace

bool rowsets_not_mono_asc_disjoint(const TabletReader::ReaderParams& read_params) {
    std::string pre_rs_last_key;
    bool pre_rs_key_bounds_truncated {false};
    for (const auto& rs_split : read_params.rs_splits) {
        if (rs_split.rs_reader->rowset()->num_rows() == 0) {
            continue;
        }
        if (rs_split.rs_reader->rowset()->is_segments_overlapping()) {
            return true;
        }
        std::string rs_first_key;
        if (!rs_split.rs_reader->rowset()->first_key(&rs_first_key)) {
            return true;
        }
        const bool cur_rs_key_bounds_truncated =
                rs_split.rs_reader->rowset()->is_segments_key_bounds_truncated();
        if (!Slice::lhs_is_strictly_less_than_rhs(Slice {pre_rs_last_key},
                                                  pre_rs_key_bounds_truncated, Slice {rs_first_key},
                                                  cur_rs_key_bounds_truncated)) {
            return true;
        }
        const bool has_last_key = rs_split.rs_reader->rowset()->last_key(&pre_rs_last_key);
        pre_rs_key_bounds_truncated = cur_rs_key_bounds_truncated;
        CHECK(has_last_key);
    }
    return false;
}

BlockIterMode select_block_iter_mode(const TabletReader::ReaderParams& read_params,
                                     const BaseTablet& tablet, const TabletSchema& tablet_schema,
                                     const ReadSchema& read_schema, bool direct_mode,
                                     bool merge_by_sequence_mapping) {
    if (!config::enable_new_block_iterator) {
        return BlockIterMode::NONE;
    }
    // From read_params: _reader_context gets the limit only in _capture_rs_readers().
    if (read_params.read_orderby_key_limit > 0) {
        return BlockIterMode::TOPN;
    }
    if (read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA) {
        return BlockIterMode::CHANGELOG_MIN_DELTA;
    }
    if (read_params.changelog_scan_type == TChangelogScanType::DETAIL) {
        return BlockIterMode::CHANGELOG_DETAIL;
    }
    if (direct_mode || tablet_schema.keys_type() == KeysType::DUP_KEYS ||
        (read_params.reader_type == ReaderType::READER_QUERY &&
         tablet.enable_unique_key_merge_on_write())) {
        if (read_params.read_orderby_key) {
            return BlockIterMode::KEY_ORDERED;
        }
        if (read_params.reader_type == ReaderType::READER_QUERY || direct_mode ||
            read_params.read_changelog || read_schema.num_key_columns() == 0) {
            return BlockIterMode::DIRECT;
        }
        return BlockIterMode::KEY_ORDERED;
    }
    if (tablet_schema.keys_type() == KeysType::UNIQUE_KEYS) {
        return merge_by_sequence_mapping ? BlockIterMode::SEQUENCE_MAP
                                         : BlockIterMode::UNIQUE_NEWEST;
    }
    if (tablet_schema.keys_type() == KeysType::AGG_KEYS) {
        return BlockIterMode::AGG_FOLD;
    }
    return BlockIterMode::NONE;
}

Status build_block_iterator(const TabletReader::ReaderParams& read_params, BlockIterMode mode,
                            BlockIteratorBuildContext ctx,
                            std::unique_ptr<BlockIterator>* out_iter) {
    BlockIteratorBuilder builder(read_params, std::move(ctx));
    return builder.build(mode, out_iter);
}

} // namespace doris
