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
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_cache.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/olap/vgeneric_iterators.h"

namespace doris {
using namespace ErrorCode;

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
        : _context(nullptr), _rowset(std::move(rowset)), _stats(&_owned_stats) {
    _rowset->acquire();
}

void BetaRowsetReader::reset_read_options() {
    _read_options.delete_condition_predicates = std::make_shared<AndBlockColumnPredicate>();
    _read_options.column_predicates.clear();
    _read_options.col_id_to_predicates.clear();
    _read_options.del_predicates_for_zone_map.clear();
    _read_options.key_ranges.clear();
}

RowsetReaderSharedPtr BetaRowsetReader::clone() {
    return RowsetReaderSharedPtr(new BetaRowsetReader(_rowset));
}

bool BetaRowsetReader::update_profile(RuntimeProfile* profile) {
    if (_iterator != nullptr) {
        return _iterator->update_profile(profile);
    }
    return false;
}

Status BetaRowsetReader::get_segment_iterators(RowsetReaderContext* read_context,
                                               std::vector<RowwiseIteratorUPtr>* out_iters,
                                               const RowSetSplits& rs_splits, bool use_cache) {
    RETURN_IF_ERROR(_rowset->load());
    _context = read_context;
    // The segment iterator is created with its own statistics,
    // and the member variable '_stats'  is initialized by '_stats(&owned_stats)'.
    // The choice of statistics used depends on the workload of the rowset reader.
    // For instance, if it's for query, the get_segment_iterators function
    // will receive one valid read_context with corresponding valid statistics,
    // and we will use those statistics.
    // However, for compaction or schema change workloads,
    // the read_context passed to the function will have null statistics,
    // and in such cases we will try to use the beta rowset reader's own statistics.
    if (_context->stats != nullptr) {
        _stats = _context->stats;
    }

    // convert RowsetReaderContext to StorageReadOptions
    _read_options.block_row_max = read_context->batch_size;
    _read_options.stats = _stats;
    _read_options.push_down_agg_type_opt = _context->push_down_agg_type_opt;
    _read_options.remaining_conjunct_roots = _context->remaining_conjunct_roots;
    _read_options.common_expr_ctxs_push_down = _context->common_expr_ctxs_push_down;
    _read_options.rowset_id = _rowset->rowset_id();
    _read_options.version = _rowset->version();
    _read_options.tablet_id = _rowset->rowset_meta()->tablet_id();
    if (read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < read_context->lower_bound_keys->size(); ++i) {
            _read_options.key_ranges.emplace_back(&read_context->lower_bound_keys->at(i),
                                                  read_context->is_lower_keys_included->at(i),
                                                  &read_context->upper_bound_keys->at(i),
                                                  read_context->is_upper_keys_included->at(i));
        }
    }

    // delete_hanlder is always set, but it maybe not init, so that it will return empty conditions
    // or predicates when it is not inited.
    if (read_context->delete_handler != nullptr) {
        read_context->delete_handler->get_delete_conditions_after_version(
                _rowset->end_version(), _read_options.delete_condition_predicates.get(),
                &_read_options.del_predicates_for_zone_map);
    }

    std::vector<uint32_t> read_columns;
    std::set<uint32_t> read_columns_set;
    std::set<uint32_t> delete_columns_set;
    for (int i = 0; i < _context->return_columns->size(); ++i) {
        read_columns.push_back(_context->return_columns->at(i));
        read_columns_set.insert(_context->return_columns->at(i));
    }
    _read_options.delete_condition_predicates->get_all_column_ids(delete_columns_set);
    for (auto cid : delete_columns_set) {
        if (read_columns_set.find(cid) == read_columns_set.end()) {
            read_columns.push_back(cid);
        }
    }
    VLOG_NOTICE << "read columns size: " << read_columns.size();

    // Variant schema is not stable, should not use schema cache at present
    if (_context->tablet_schema->num_variant_columns() == 0) {
        std::string schema_key = SchemaCache::get_schema_key(
                _read_options.tablet_id, _context->tablet_schema, read_columns,
                _context->tablet_schema->schema_version(), SchemaCache::Type::SCHEMA);
        // It is necessary to ensure that there is a schema version when using a cache
        // because the absence of a schema version can result in reading a stale version
        // of the schema after a schema change.
        if (_context->tablet_schema->schema_version() < 0 ||
            (_input_schema = SchemaCache::instance()->get_schema<SchemaSPtr>(schema_key)) ==
                    nullptr) {
            _input_schema =
                    std::make_shared<Schema>(_context->tablet_schema->columns(), read_columns);
            SchemaCache::instance()->insert_schema(schema_key, _input_schema);
        }
    } else {
        _input_schema = std::make_shared<Schema>(_context->tablet_schema->columns(), read_columns);
    }

    if (read_context->predicates != nullptr) {
        _read_options.column_predicates.insert(_read_options.column_predicates.end(),
                                               read_context->predicates->begin(),
                                               read_context->predicates->end());
        for (auto pred : *(read_context->predicates)) {
            if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                _read_options.col_id_to_predicates.insert(
                        {pred->column_id(), std::make_shared<AndBlockColumnPredicate>()});
            }
            auto single_column_block_predicate = new SingleColumnBlockPredicate(pred);
            _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                    single_column_block_predicate);
        }
    }

    if (read_context->predicates_except_leafnode_of_andnode != nullptr) {
        _read_options.column_predicates_except_leafnode_of_andnode.insert(
                _read_options.column_predicates_except_leafnode_of_andnode.end(),
                read_context->predicates_except_leafnode_of_andnode->begin(),
                read_context->predicates_except_leafnode_of_andnode->end());
    }

    // Take a delete-bitmap for each segment, the bitmap contains all deletes
    // until the max read version, which is read_context->version.second
    if (read_context->delete_bitmap != nullptr) {
        RowsetId rowset_id = rowset()->rowset_id();
        for (uint32_t seg_id = 0; seg_id < rowset()->num_segments(); ++seg_id) {
            auto d = read_context->delete_bitmap->get_agg(
                    {rowset_id, seg_id, read_context->version.second});
            if (d->isEmpty()) {
                continue; // Empty delete bitmap for the segment
            }
            VLOG_TRACE << "Get the delete bitmap for rowset: " << rowset_id.to_string()
                       << ", segment id:" << seg_id << ", size:" << d->cardinality();
            _read_options.delete_bitmap.emplace(seg_id, std::move(d));
        }
    }

    if (_should_push_down_value_predicates()) {
        if (read_context->value_predicates != nullptr) {
            _read_options.column_predicates.insert(_read_options.column_predicates.end(),
                                                   read_context->value_predicates->begin(),
                                                   read_context->value_predicates->end());
            for (auto pred : *(read_context->value_predicates)) {
                if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                    _read_options.col_id_to_predicates.insert(
                            {pred->column_id(), std::make_shared<AndBlockColumnPredicate>()});
                }
                auto single_column_block_predicate = new SingleColumnBlockPredicate(pred);
                _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                        single_column_block_predicate);
            }
        }
    }
    _read_options.use_page_cache = read_context->use_page_cache;
    _read_options.tablet_schema = read_context->tablet_schema;
    _read_options.record_rowids = read_context->record_rowids;
    _read_options.use_topn_opt = read_context->use_topn_opt;
    _read_options.read_orderby_key_reverse = read_context->read_orderby_key_reverse;
    _read_options.read_orderby_key_columns = read_context->read_orderby_key_columns;
    _read_options.io_ctx.reader_type = read_context->reader_type;
    _read_options.io_ctx.file_cache_stats = &_stats->file_cache_stats;
    _read_options.runtime_state = read_context->runtime_state;
    _read_options.output_columns = read_context->output_columns;
    _read_options.suspended_eliminate_cast_slots = read_context->suspended_eliminate_cast_slots;

    // load segments
    bool should_use_cache = use_cache || read_context->reader_type == ReaderType::READER_QUERY;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(_rowset, &_segment_cache_handle,
                                                             should_use_cache));

    // create iterator for each segment
    auto& segments = _segment_cache_handle.get_segments();
    auto [seg_start, seg_end] = rs_splits.segment_offsets;
    if (seg_start == seg_end) {
        seg_start = 0;
        seg_end = segments.size();
    }

    for (int i = seg_start; i < seg_end; i++) {
        auto& seg_ptr = segments[i];
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(_input_schema, _read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return Status::Error<ROWSET_READER_INIT>(s.to_string());
        }
        if (iter->empty()) {
            continue;
        }
        out_iters->push_back(std::move(iter));
    }

    return Status::OK();
}

Status BetaRowsetReader::init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits) {
    _context = read_context;
    _context->rowset_id = _rowset->rowset_id();
    std::vector<RowwiseIteratorUPtr> iterators;
    RETURN_IF_ERROR(get_segment_iterators(_context, &iterators, rs_splits));

    // merge or union segment iterator
    if (read_context->need_ordered_result && _rowset->rowset_meta()->is_segments_overlapping()) {
        auto sequence_loc = -1;
        if (read_context->sequence_id_idx != -1) {
            for (size_t loc = 0; loc < read_context->return_columns->size(); loc++) {
                if (read_context->return_columns->at(loc) == read_context->sequence_id_idx) {
                    sequence_loc = loc;
                    break;
                }
            }
        }
        _iterator = vectorized::new_merge_iterator(
                std::move(iterators), sequence_loc, read_context->is_unique,
                read_context->read_orderby_key_reverse, read_context->merged_rows);
    } else {
        if (read_context->read_orderby_key_reverse) {
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
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    if (_empty) {
        return Status::Error<END_OF_FILE>("BetaRowsetReader is empty");
    }

    do {
        auto s = _iterator->next_batch(block);
        if (!s.ok()) {
            if (!s.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to read next block: " << s.to_string();
            }
            return s;
        }
    } while (block->empty());

    return Status::OK();
}

Status BetaRowsetReader::next_block_view(vectorized::BlockView* block_view) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    do {
        auto s = _iterator->next_block_view(block_view);
        if (!s.ok()) {
            if (!s.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to read next block view: " << s.to_string();
            }
            return s;
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
             _context->sequence_id_idx == -1) ||
            _context->enable_unique_key_merge_on_write);
}

Status BetaRowsetReader::get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) {
    auto& seg_ptrs = _segment_cache_handle.get_segments();
    segment_num_rows->resize(seg_ptrs.size());
    for (size_t i = 0; i < seg_ptrs.size(); i++) {
        (*segment_num_rows)[i] = seg_ptrs[i]->num_rows();
    }
    return Status::OK();
}

} // namespace doris
