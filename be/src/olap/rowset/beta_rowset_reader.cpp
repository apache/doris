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
#include "vec/olap/vgeneric_iterators.h"

#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/schema.h"

#include "vec/core/block.h"

namespace doris {

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
        : _context(nullptr),
          _rowset(std::move(rowset)),
          _stats(&_owned_stats) {
    _rowset->acquire();
}

OLAPStatus BetaRowsetReader::init(RowsetReaderContext* read_context) {
    RETURN_NOT_OK(_rowset->load());
    _context = read_context;
    if (_context->stats != nullptr) {
        // schema change/compaction should use owned_stats
        // When doing schema change/compaction,
        // only statistics of this RowsetReader is necessary.
        _stats = _context->stats;
    }
    // SegmentIterator will load seek columns on demand
    _schema = std::make_unique<Schema>(_context->tablet_schema->columns(), *(_context->return_columns));

    // convert RowsetReaderContext to StorageReadOptions
    StorageReadOptions read_options;
    read_options.stats = _stats;
    read_options.conditions = read_context->conditions;
    if (read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < read_context->lower_bound_keys->size(); ++i) {
            read_options.key_ranges.emplace_back(&read_context->lower_bound_keys->at(i),
                                                 read_context->is_lower_keys_included->at(i),
                                                 &read_context->upper_bound_keys->at(i),
                                                 read_context->is_upper_keys_included->at(i));
        }
    }
    if (read_context->delete_handler != nullptr) {
        read_context->delete_handler->get_delete_conditions_after_version(
                _rowset->end_version(), &read_options.delete_conditions,
                read_options.delete_condition_predicates.get());
    }
    if (read_context->predicates != nullptr) {
        read_options.column_predicates.insert(read_options.column_predicates.end(),
                                              read_context->predicates->begin(),
                                              read_context->predicates->end());
    }
    // if unique table with rowset [0-x] or [0-1] [2-y] [...],
    // value column predicates can be pushdown on rowset [0-x] or [2-y]
    if (_rowset->keys_type() == UNIQUE_KEYS &&
        (_rowset->start_version() == 0 || _rowset->start_version() == 2)) {
        if (read_context->value_predicates != nullptr) {
            read_options.column_predicates.insert(read_options.column_predicates.end(),
                                                  read_context->value_predicates->begin(),
                                                  read_context->value_predicates->end());
        }
        if (read_context->all_conditions != nullptr && !read_context->all_conditions->empty()) {
            read_options.conditions = read_context->all_conditions;
        }
    }
    read_options.use_page_cache = read_context->use_page_cache;

    // load segments
    RETURN_NOT_OK(SegmentLoader::instance()->load_segments(
            _rowset, &_segment_cache_handle, read_context->reader_type == ReaderType::READER_QUERY));

    // create iterator for each segment
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : _segment_cache_handle.get_segments()) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(*_schema, read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return OLAP_ERR_ROWSET_READER_INIT;
        }
        seg_iterators.push_back(std::move(iter));
    }

    std::vector<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) {
        // transfer ownership of segment iterator to `_iterator`
        iterators.push_back(owned_it.release());
    }

    // merge or union segment iterator
    RowwiseIterator* final_iterator;
    if (config::enable_storage_vectorization && read_context->is_vec) {
        if (read_context->need_ordered_result && _rowset->rowset_meta()->is_segments_overlapping()) {
            final_iterator = vectorized::new_merge_iterator(iterators, read_context->sequence_id_idx);
        } else {
            final_iterator = vectorized::new_union_iterator(iterators);
        }
    } else {
        if (read_context->need_ordered_result && _rowset->rowset_meta()->is_segments_overlapping()) {
            final_iterator = new_merge_iterator(iterators, read_context->sequence_id_idx);
        } else {
            final_iterator = new_union_iterator(iterators);
        }
    }

    auto s = final_iterator->init(read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        return OLAP_ERR_ROWSET_READER_INIT;
    }
    _iterator.reset(final_iterator);

    // init input block
    _input_block.reset(new RowBlockV2(*_schema,
            std::min(1024, read_context->batch_size)));

    if (!read_context->is_vec) {
        // init input/output block and row
        _output_block.reset(new RowBlock(read_context->tablet_schema));

        RowBlockInfo output_block_info;
        output_block_info.row_num = std::min(1024, read_context->batch_size);
        output_block_info.null_supported = true;
        // the output block's schema should be seek_columns to conform to v1
        // TODO(hkp): this should be optimized to use return_columns
        output_block_info.column_ids = *(_context->seek_columns);
        _output_block->init(output_block_info);
        _row.reset(new RowCursor());
        RETURN_NOT_OK(_row->init(*(read_context->tablet_schema), *(_context->seek_columns)));
    }

    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetReader::next_block(RowBlock** block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    // read next input block
    _input_block->clear();
    {
        auto s = _iterator->next_batch(_input_block.get());
        if (!s.ok()) {
            if (s.is_end_of_file()) {
                *block = nullptr;
                return OLAP_ERR_DATA_EOF;
            }
            LOG(WARNING) << "failed to read next block: " << s.to_string();
            return OLAP_ERR_ROWSET_READ_FAILED;
        }
    }

    // convert to output block
    _output_block->clear();
    {
        SCOPED_RAW_TIMER(&_stats->block_convert_ns);
        _input_block->convert_to_row_block(_row.get(), _output_block.get());
    }
    *block = _output_block.get();
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetReader::next_block(vectorized::Block* block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    if (config::enable_storage_vectorization && _context->is_vec) {
        auto s = _iterator->next_batch(block);
        if (!s.ok()) {
            if (s.is_end_of_file()) {
                return OLAP_ERR_DATA_EOF;
            } else {
                LOG(WARNING) << "failed to read next block: " << s.to_string();
                return OLAP_ERR_ROWSET_READ_FAILED;
            }
        }
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
                                return OLAP_ERR_DATA_EOF;
                            } else {
                                break;
                            }
                        } else {
                            LOG(WARNING) << "failed to read next block: " << s.to_string();
                            return OLAP_ERR_ROWSET_READ_FAILED;
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
                    return OLAP_ERR_STRING_OVERFLOW_IN_VEC_ENGINE;
                }
            }
            is_first = false;
        } while (block->rows() < _context->batch_size); // here we should keep block.rows() < batch_size
    }

    return OLAP_SUCCESS;
}

} // namespace doris
