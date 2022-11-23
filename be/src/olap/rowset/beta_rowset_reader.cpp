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

#include <utility>

#include "olap/delete_handler.h"
#include "olap/generic_iterators.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/schema.h"
#include "olap/tablet_meta.h"
#include "vec/core/block.h"
#include "vec/olap/vgeneric_iterators.h"

namespace doris {

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
        : _context(nullptr), _rowset(std::move(rowset)), _stats(&_owned_stats) {
    _rowset->acquire();
}

void BetaRowsetReader::reset_read_options() {
    _read_options.delete_condition_predicates = std::make_shared<AndBlockColumnPredicate>();
    _read_options.column_predicates.clear();
    _read_options.col_id_to_predicates.clear();
    _read_options.col_id_to_del_predicates.clear();
    _read_options.key_ranges.clear();
}

Status BetaRowsetReader::get_segment_iterators(RowsetReaderContext* read_context,
                                               std::vector<RowwiseIterator*>* out_iters) {
    RETURN_NOT_OK(_rowset->load());
    _context = read_context;
    if (_context->stats != nullptr) {
        // schema change/compaction should use owned_stats
        // When doing schema change/compaction,
        // only statistics of this RowsetReader is necessary.
        _stats = _context->stats;
    }

    // convert RowsetReaderContext to StorageReadOptions
    _read_options.stats = _stats;
    _read_options.push_down_agg_type_opt = _context->push_down_agg_type_opt;
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
                &_read_options.col_id_to_del_predicates);
        // if del cond is not empty, schema may be different in multiple rowset
        _can_reuse_schema = _read_options.col_id_to_del_predicates.empty();
    }
    // In vertical compaction, every column group need new schema
    if (read_context->is_vertical_compaction) {
        _can_reuse_schema = false;
    }
    if (!_can_reuse_schema || _context->reuse_input_schema == nullptr) {
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
        _input_schema = std::make_shared<Schema>(_context->tablet_schema->columns(), read_columns);
        if (_can_reuse_schema) {
            _context->reuse_input_schema = _input_schema;
        }
    }

    // if can reuse schema, context must have reuse_input_schema
    // if can't reuse schema, context mustn't have reuse_input_schema
    DCHECK(_can_reuse_schema ^ (_context->reuse_input_schema == nullptr));
    if (_context->reuse_input_schema != nullptr && _input_schema == nullptr) {
        _input_schema = _context->reuse_input_schema;
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
    _read_options.read_orderby_key_reverse = read_context->read_orderby_key_reverse;
    _read_options.read_orderby_key_columns = read_context->read_orderby_key_columns;
    _read_options.io_ctx.reader_type = read_context->reader_type;

    // load segments
    RETURN_NOT_OK(SegmentLoader::instance()->load_segments(
            _rowset, &_segment_cache_handle,
            read_context->reader_type == ReaderType::READER_QUERY));

    // create iterator for each segment
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : _segment_cache_handle.get_segments()) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(*_input_schema, _read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_READER_INIT);
        }
        seg_iterators.push_back(std::move(iter));
    }

    for (auto& owned_it : seg_iterators) {
        auto st = owned_it->init(_read_options);
        if (!st.ok()) {
            LOG(WARNING) << "failed to init iterator: " << st.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_READER_INIT);
        }
        // transfer ownership of segment iterator to `_iterator`
        out_iters->push_back(owned_it.release());
    }
    return Status::OK();
}

Status BetaRowsetReader::init(RowsetReaderContext* read_context) {
    _context = read_context;
    std::vector<RowwiseIterator*> iterators;
    RETURN_NOT_OK(get_segment_iterators(_context, &iterators));

    // merge or union segment iterator
    RowwiseIterator* final_iterator;
    if (config::enable_storage_vectorization && read_context->is_vec) {
        if (read_context->need_ordered_result &&
            _rowset->rowset_meta()->is_segments_overlapping()) {
            final_iterator = vectorized::new_merge_iterator(
                    iterators, read_context->sequence_id_idx, read_context->is_unique,
                    read_context->read_orderby_key_reverse, read_context->merged_rows);
        } else {
            if (read_context->read_orderby_key_reverse) {
                // reverse iterators to read backward for ORDER BY key DESC
                std::reverse(iterators.begin(), iterators.end());
            }
            final_iterator = vectorized::new_union_iterator(iterators);
        }
    } else {
        if (read_context->need_ordered_result &&
            _rowset->rowset_meta()->is_segments_overlapping()) {
            final_iterator = new_merge_iterator(iterators, read_context->sequence_id_idx,
                                                read_context->is_unique, read_context->merged_rows);
        } else {
            final_iterator = new_union_iterator(iterators);
        }
    }

    auto s = final_iterator->init(_read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_READER_INIT);
    }
    _iterator.reset(final_iterator);

    // The data in _input_block will be copied shallowly to _output_block.
    // Therefore, for nestable fields, the _input_block can't be shared.
    bool has_nestable_fields = false;
    for (const auto* field : _input_schema->columns()) {
        if (field != nullptr && field->get_sub_field_count() > 0) {
            has_nestable_fields = true;
            break;
        }
    }

    // init input block
    if (_can_reuse_schema && !has_nestable_fields) {
        if (read_context->reuse_block == nullptr) {
            read_context->reuse_block.reset(
                    new RowBlockV2(*_input_schema, std::min(1024, read_context->batch_size)));
        }
        _input_block = read_context->reuse_block;
    } else {
        _input_block.reset(
                new RowBlockV2(*_input_schema, std::min(1024, read_context->batch_size)));
    }

    if (!read_context->is_vec) {
        // init input/output block and row
        _output_block.reset(new RowBlock(read_context->tablet_schema));

        RowBlockInfo output_block_info;
        output_block_info.row_num = std::min(1024, read_context->batch_size);
        output_block_info.null_supported = true;
        output_block_info.column_ids = *(_context->return_columns);
        _output_block->init(output_block_info);
        _row.reset(new RowCursor());
        RETURN_NOT_OK(_row->init(read_context->tablet_schema, *(_context->return_columns)));
    }

    return Status::OK();
}

Status BetaRowsetReader::next_block(RowBlock** block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    // read next input block
    _input_block->clear();
    {
        auto s = _iterator->next_batch(_input_block.get());
        if (!s.ok()) {
            if (s.is_end_of_file()) {
                *block = nullptr;
                return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
            }
            LOG(WARNING) << "failed to read next block: " << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_READ_FAILED);
        }
    }

    // convert to output block
    _output_block->clear();
    {
        SCOPED_RAW_TIMER(&_stats->block_convert_ns);
        _input_block->convert_to_row_block(_row.get(), _output_block.get());
    }
    *block = _output_block.get();
    return Status::OK();
}

Status BetaRowsetReader::next_block(vectorized::Block* block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    if (config::enable_storage_vectorization && _context->is_vec) {
        do {
            auto s = _iterator->next_batch(block);
            if (!s.ok()) {
                if (s.is_end_of_file()) {
                    return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
                } else {
                    LOG(WARNING) << "failed to read next block: " << s.to_string();
                    return Status::OLAPInternalError(OLAP_ERR_ROWSET_READ_FAILED);
                }
            }
        } while (block->rows() == 0);
    } else {
        bool is_first = true;

        do {
            // read next input block
            {
                _input_block->clear();
                {
                    auto s = _iterator->next_batch(_input_block.get());
                    if (!s.ok()) {
                        if (s.is_end_of_file()) {
                            if (is_first) {
                                return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
                            } else {
                                break;
                            }
                        } else {
                            LOG(WARNING) << "failed to read next block: " << s.to_string();
                            return Status::OLAPInternalError(OLAP_ERR_ROWSET_READ_FAILED);
                        }
                    } else if (_input_block->selected_size() == 0) {
                        continue;
                    }
                }
            }

            {
                SCOPED_RAW_TIMER(&_stats->block_convert_ns);
                auto s = _input_block->convert_to_vec_block(block);
                if (UNLIKELY(!s.ok())) {
                    LOG(WARNING) << "failed to read next block: " << s.to_string();
                    return Status::OLAPInternalError(OLAP_ERR_STRING_OVERFLOW_IN_VEC_ENGINE);
                }
            }
            is_first = false;
        } while (block->rows() <
                 _context->batch_size); // here we should keep block.rows() < batch_size
    }

    return Status::OK();
}

Status BetaRowsetReader::next_block_view(vectorized::BlockView* block_view) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    if (config::enable_storage_vectorization && _context->is_vec) {
        do {
            auto s = _iterator->next_block_view(block_view);
            if (!s.ok()) {
                if (s.is_end_of_file()) {
                    return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
                } else {
                    LOG(WARNING) << "failed to read next block: " << s.to_string();
                    return Status::OLAPInternalError(OLAP_ERR_ROWSET_READ_FAILED);
                }
            }
        } while (block_view->empty());
    } else {
        return Status::NotSupported("block view only support enable_storage_vectorization");
    }

    return Status::OK();
}

bool BetaRowsetReader::_should_push_down_value_predicates() const {
    // if unique table with rowset [0-x] or [0-1] [2-y] [...],
    // value column predicates can be pushdown on rowset [0-x] or [2-y], [2-y] must be compaction and not overlapping
    return _rowset->keys_type() == UNIQUE_KEYS &&
           (((_rowset->start_version() == 0 || _rowset->start_version() == 2) &&
             !_rowset->_rowset_meta->is_segments_overlapping()) ||
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
