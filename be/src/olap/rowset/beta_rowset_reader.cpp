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

#include "beta_rowset_reader.h"

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "io/io_common.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/delete_handler.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/segment_v2/lazy_init_segment_iterator.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_cache.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/olap/vgeneric_iterators.h"

namespace doris {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
        : _read_context(nullptr), _rowset(std::move(rowset)), _stats(&_owned_stats) {
    _rowset->acquire();
}

void BetaRowsetReader::reset_read_options() {
    _read_options.delete_condition_predicates = AndBlockColumnPredicate::create_shared();
    _read_options.column_predicates.clear();
    _read_options.col_id_to_predicates.clear();
    _read_options.del_predicates_for_zone_map.clear();
    _read_options.key_ranges.clear();
}

RowsetReaderSharedPtr BetaRowsetReader::clone() {
    return RowsetReaderSharedPtr(new BetaRowsetReader(_rowset));
}

void BetaRowsetReader::update_profile(RuntimeProfile* profile) {
    if (_iterator != nullptr) {
        _iterator->update_profile(profile);
    }
}

Status BetaRowsetReader::get_segment_iterators(RowsetReaderContext* read_context,
                                               std::vector<RowwiseIteratorUPtr>* out_iters,
                                               bool use_cache) {
    _read_context = read_context;
    // The segment iterator is created with its own statistics,
    // and the member variable '_stats'  is initialized by '_stats(&owned_stats)'.
    // The choice of statistics used depends on the workload of the rowset reader.
    // For instance, if it's for query, the get_segment_iterators function
    // will receive one valid read_context with corresponding valid statistics,
    // and we will use those statistics.
    // However, for compaction or schema change workloads,
    // the read_context passed to the function will have null statistics,
    // and in such cases we will try to use the beta rowset reader's own statistics.
    if (_read_context->stats != nullptr) {
        _stats = _read_context->stats;
    }
    SCOPED_RAW_TIMER(&_stats->rowset_reader_get_segment_iterators_timer_ns);

    RETURN_IF_ERROR(_rowset->load());

    // convert RowsetReaderContext to StorageReadOptions
    _read_options.block_row_max = read_context->batch_size;
    _read_options.stats = _stats;
    _read_options.push_down_agg_type_opt = _read_context->push_down_agg_type_opt;
    _read_options.remaining_conjunct_roots = _read_context->remaining_conjunct_roots;
    _read_options.common_expr_ctxs_push_down = _read_context->common_expr_ctxs_push_down;
    _read_options.virtual_column_exprs = _read_context->virtual_column_exprs;
    _read_options.vir_cid_to_idx_in_block = _read_context->vir_cid_to_idx_in_block;
    _read_options.vir_col_idx_to_type = _read_context->vir_col_idx_to_type;
    _read_options.rowset_id = _rowset->rowset_id();
    _read_options.version = _rowset->version();
    _read_options.tablet_id = _rowset->rowset_meta()->tablet_id();
    _read_options.topn_limit = _topn_limit;
    if (_read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < _read_context->lower_bound_keys->size(); ++i) {
            _read_options.key_ranges.emplace_back(&_read_context->lower_bound_keys->at(i),
                                                  _read_context->is_lower_keys_included->at(i),
                                                  &_read_context->upper_bound_keys->at(i),
                                                  _read_context->is_upper_keys_included->at(i));
        }
    }

    // delete_hanlder is always set, but it maybe not init, so that it will return empty conditions
    // or predicates when it is not inited.
    if (_read_context->delete_handler != nullptr) {
        _read_context->delete_handler->get_delete_conditions_after_version(
                _rowset->end_version(), _read_options.delete_condition_predicates.get(),
                &_read_options.del_predicates_for_zone_map);
    }

    std::vector<uint32_t> read_columns;
    std::set<uint32_t> read_columns_set;
    std::set<uint32_t> delete_columns_set;
    for (int i = 0; i < _read_context->return_columns->size(); ++i) {
        read_columns.push_back(_read_context->return_columns->at(i));
        read_columns_set.insert(_read_context->return_columns->at(i));
    }
    _read_options.delete_condition_predicates->get_all_column_ids(delete_columns_set);
    for (auto cid : delete_columns_set) {
        if (read_columns_set.find(cid) == read_columns_set.end()) {
            read_columns.push_back(cid);
        }
    }
    VLOG_NOTICE << "read columns size: " << read_columns.size();
    _input_schema = std::make_shared<Schema>(_read_context->tablet_schema->columns(), read_columns);
    if (_read_context->predicates != nullptr) {
        _read_options.column_predicates.insert(_read_options.column_predicates.end(),
                                               _read_context->predicates->begin(),
                                               _read_context->predicates->end());
        for (auto pred : *(_read_context->predicates)) {
            if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                _read_options.col_id_to_predicates.insert(
                        {pred->column_id(), AndBlockColumnPredicate::create_shared()});
            }
            _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                    SingleColumnBlockPredicate::create_unique(pred));
        }
    }

    // Take a delete-bitmap for each segment, the bitmap contains all deletes
    // until the max read version, which is read_context->version.second
    if (_read_context->delete_bitmap != nullptr) {
        {
            SCOPED_RAW_TIMER(&_stats->delete_bitmap_get_agg_ns);
            RowsetId rowset_id = rowset()->rowset_id();
            for (uint32_t seg_id = 0; seg_id < rowset()->num_segments(); ++seg_id) {
                auto d = _read_context->delete_bitmap->get_agg(
                        {rowset_id, seg_id, _read_context->version.second});
                if (d->isEmpty()) {
                    continue; // Empty delete bitmap for the segment
                }
                VLOG_TRACE << "Get the delete bitmap for rowset: " << rowset_id.to_string()
                           << ", segment id:" << seg_id << ", size:" << d->cardinality();
                _read_options.delete_bitmap.emplace(seg_id, std::move(d));
            }
        }
    }

    if (_should_push_down_value_predicates()) {
        if (_read_context->value_predicates != nullptr) {
            _read_options.column_predicates.insert(_read_options.column_predicates.end(),
                                                   _read_context->value_predicates->begin(),
                                                   _read_context->value_predicates->end());
            for (auto pred : *(_read_context->value_predicates)) {
                if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                    _read_options.col_id_to_predicates.insert(
                            {pred->column_id(), AndBlockColumnPredicate::create_shared()});
                }
                _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                        SingleColumnBlockPredicate::create_unique(pred));
            }
        }
    }
    _read_options.use_page_cache = _read_context->use_page_cache;
    _read_options.tablet_schema = _read_context->tablet_schema;
    _read_options.enable_unique_key_merge_on_write =
            _read_context->enable_unique_key_merge_on_write;
    _read_options.record_rowids = _read_context->record_rowids;
    _read_options.topn_filter_source_node_ids = _read_context->topn_filter_source_node_ids;
    _read_options.topn_filter_target_node_id = _read_context->topn_filter_target_node_id;
    _read_options.read_orderby_key_reverse = _read_context->read_orderby_key_reverse;
    _read_options.read_orderby_key_columns = _read_context->read_orderby_key_columns;
    _read_options.io_ctx.reader_type = _read_context->reader_type;
    _read_options.io_ctx.file_cache_stats = &_stats->file_cache_stats;
    _read_options.runtime_state = _read_context->runtime_state;
    _read_options.output_columns = _read_context->output_columns;
    _read_options.io_ctx.reader_type = _read_context->reader_type;
    _read_options.io_ctx.is_disposable = _read_context->reader_type != ReaderType::READER_QUERY;
    _read_options.target_cast_type_for_variants = _read_context->target_cast_type_for_variants;
    if (_read_context->runtime_state != nullptr) {
        _read_options.io_ctx.query_id = &_read_context->runtime_state->query_id();
        _read_options.io_ctx.read_file_cache =
                _read_context->runtime_state->query_options().enable_file_cache;
        _read_options.io_ctx.is_disposable =
                _read_context->runtime_state->query_options().disable_file_cache;
    }

    _read_options.io_ctx.expiration_time =
            read_context->ttl_seconds > 0 && _rowset->rowset_meta()->newest_write_timestamp() > 0
                    ? _rowset->rowset_meta()->newest_write_timestamp() + read_context->ttl_seconds
                    : 0;
    if (_read_options.io_ctx.expiration_time <= UnixSeconds()) {
        _read_options.io_ctx.expiration_time = 0;
    }

    // use rate_limiter
    // !fix: need to check reader_type
    _read_options.io_ctx.is_limit_io =
            _read_context->reader_type == ReaderType::READER_BASE_COMPACTION ||
            _read_context->reader_type == ReaderType::READER_COLD_DATA_COMPACTION ||
            _read_context->reader_type == ReaderType::READER_CUMULATIVE_COMPACTION ||
            _read_context->reader_type == ReaderType::READER_FULL_COMPACTION;

    bool enable_segment_cache = true;
    auto* state = read_context->runtime_state;
    if (state != nullptr) {
        enable_segment_cache = state->query_options().__isset.enable_segment_cache
                                       ? state->query_options().enable_segment_cache
                                       : true;
    }
    // When reader type is for query, session variable `enable_segment_cache` should be respected.
    bool should_use_cache = use_cache || (_read_context->reader_type == ReaderType::READER_QUERY &&
                                          enable_segment_cache);

    auto segment_count = _rowset->num_segments();
    auto [seg_start, seg_end] = _segment_offsets;
    // If seg_start == seg_end, it means that the segments of a rowset is not
    // split scanned by multiple scanners, and the rowset reader is used to read the whole rowset.
    if (seg_start == seg_end) {
        seg_start = 0;
        seg_end = segment_count;
    }
    if (_read_context->record_rowids && _read_context->rowid_conversion) {
        // init segment rowid map for rowid conversion
        std::vector<uint32_t> segment_rows;
        RETURN_IF_ERROR(_rowset->get_segment_num_rows(&segment_rows, true));
        RETURN_IF_ERROR(_read_context->rowid_conversion->init_segment_map(rowset()->rowset_id(),
                                                                          segment_rows));
    }

    for (int64_t i = seg_start; i < seg_end; i++) {
        SCOPED_RAW_TIMER(&_stats->rowset_reader_create_iterators_timer_ns);
        std::unique_ptr<RowwiseIterator> iter;

        /// For iterators, we don't need to initialize them all at once when creating them.
        /// Instead, we should initialize each iterator separately when really using them.
        /// This optimization minimizes the lifecycle of resources like column readers
        /// and prevents excessive memory consumption, especially for wide tables.
        if (_segment_row_ranges.empty()) {
            _read_options.row_ranges.clear();
            iter = std::make_unique<LazyInitSegmentIterator>(_rowset, i, should_use_cache,
                                                             _input_schema, _read_options);
        } else {
            DCHECK_EQ(seg_end - seg_start, _segment_row_ranges.size());
            auto local_options = _read_options;
            local_options.row_ranges = _segment_row_ranges[i - seg_start];
            iter = std::make_unique<LazyInitSegmentIterator>(_rowset, i, should_use_cache,
                                                             _input_schema, local_options);
        }

        if (iter->empty()) {
            continue;
        }
        out_iters->push_back(std::move(iter));
    }

    return Status::OK();
}

Status BetaRowsetReader::init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits) {
    _read_context = read_context;
    _read_context->rowset_id = _rowset->rowset_id();
    _segment_offsets = rs_splits.segment_offsets;
    _segment_row_ranges = rs_splits.segment_row_ranges;
    return Status::OK();
}

Status BetaRowsetReader::_init_iterator_once() {
    return _init_iter_once.call([this] { return _init_iterator(); });
}

Status BetaRowsetReader::_init_iterator() {
    std::vector<RowwiseIteratorUPtr> iterators;
    RETURN_IF_ERROR(get_segment_iterators(_read_context, &iterators));

    SCOPED_RAW_TIMER(&_stats->rowset_reader_init_iterators_timer_ns);

    if (_read_context->merged_rows == nullptr) {
        _read_context->merged_rows = &_merged_rows;
    }
    // merge or union segment iterator
    if (_is_merge_iterator()) {
        auto sequence_loc = -1;
        if (_read_context->sequence_id_idx != -1) {
            for (int loc = 0; loc < _read_context->return_columns->size(); loc++) {
                if (_read_context->return_columns->at(loc) == _read_context->sequence_id_idx) {
                    sequence_loc = loc;
                    break;
                }
            }
        }
        _iterator = vectorized::new_merge_iterator(
                std::move(iterators), sequence_loc, _read_context->is_unique,
                _read_context->read_orderby_key_reverse, _read_context->merged_rows);
    } else {
        if (_read_context->read_orderby_key_reverse) {
            // reverse iterators to read backward for ORDER BY key DESC
            std::reverse(iterators.begin(), iterators.end());
        }
        _iterator = vectorized::new_union_iterator(std::move(iterators));
    }

    auto s = _iterator->init(_read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        _iterator.reset();
        return Status::Error<ROWSET_READER_INIT>(s.to_string());
    }
    return Status::OK();
}

Status BetaRowsetReader::next_block(vectorized::Block* block) {
    RETURN_IF_ERROR(_init_iterator_once());
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    if (_empty) {
        return Status::Error<END_OF_FILE>("BetaRowsetReader is empty");
    }

    RuntimeState* runtime_state = nullptr;
    if (_read_context != nullptr) {
        runtime_state = _read_context->runtime_state;
    }

    do {
        auto s = _iterator->next_batch(block);
        if (!s.ok()) {
            if (!s.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to read next block: " << s.to_string();
            }
            return s;
        }

        if (runtime_state != nullptr && runtime_state->is_cancelled()) [[unlikely]] {
            return runtime_state->cancel_reason();
        }
    } while (block->empty());

    return Status::OK();
}

Status BetaRowsetReader::next_block_view(vectorized::BlockView* block_view) {
    RETURN_IF_ERROR(_init_iterator_once());
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    RuntimeState* runtime_state = nullptr;
    if (_read_context != nullptr) {
        runtime_state = _read_context->runtime_state;
    }

    do {
        auto s = _iterator->next_block_view(block_view);
        if (!s.ok()) {
            if (!s.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to read next block view: " << s.to_string();
            }
            return s;
        }

        if (runtime_state != nullptr && runtime_state->is_cancelled()) [[unlikely]] {
            return runtime_state->cancel_reason();
        }
    } while (block_view->empty());

    return Status::OK();
}

bool BetaRowsetReader::_should_push_down_value_predicates() const {
    // if unique table with rowset [0-x] or [0-1] [2-y] [...],
    // value column predicates can be pushdown on rowset [0-x] or [2-y], [2-y]
    // must be compaction, not overlapping and don't have sequence column
    return _rowset->keys_type() == UNIQUE_KEYS &&
           (((_rowset->start_version() == 0 || _rowset->start_version() == 2) &&
             !_rowset->_rowset_meta->is_segments_overlapping() &&
             _read_context->sequence_id_idx == -1) ||
            _read_context->enable_unique_key_merge_on_write);
}
#include "common/compile_check_end.h"
} // namespace doris
