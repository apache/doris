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
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/schema.h"

namespace doris {

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset,
                                   std::shared_ptr<MemTracker> parent_tracker)
        : _rowset(std::move(rowset)),
          _context(nullptr),
          _stats(&_owned_stats),
          _parent_tracker(std::move(parent_tracker)) {
    _rowset->aquire();
}

OLAPStatus BetaRowsetReader::init(RowsetReaderContext* read_context) {
    // If do not init the RowsetReader with a parent_tracker, use the runtime_state instance_mem_tracker
    if (_parent_tracker == nullptr && read_context->runtime_state != nullptr) {
        _parent_tracker = read_context->runtime_state->instance_mem_tracker();
    }

    RETURN_NOT_OK(_rowset->load(true, _parent_tracker));
    _context = read_context;
    if (_context->stats != nullptr) {
        // schema change/compaction should use owned_stats
        // When doing schema change/compaction,
        // only statistics of this RowsetReader is necessary.
        _stats = _context->stats;
    }
    // SegmentIterator will load seek columns on demand
    Schema schema(_context->tablet_schema->columns(), *(_context->return_columns));

    // convert RowsetReaderContext to StorageReadOptions
    StorageReadOptions read_options;
    read_options.stats = _stats;
    read_options.conditions = read_context->conditions;
    if (read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < read_context->lower_bound_keys->size(); ++i) {
            read_options.key_ranges.emplace_back(read_context->lower_bound_keys->at(i),
                                                 read_context->is_lower_keys_included->at(i),
                                                 read_context->upper_bound_keys->at(i),
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
    if (read_context->value_predicates != nullptr && _rowset->keys_type() == UNIQUE_KEYS &&
        (_rowset->start_version() == 0 || _rowset->start_version() == 2)) {
        read_options.column_predicates.insert(read_options.column_predicates.end(),
                                              read_context->value_predicates->begin(),
                                              read_context->value_predicates->end());
    }
    read_options.use_page_cache = read_context->use_page_cache;

    // create iterator for each segment
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    if (!_context->segments.empty() &&
        _context->segments.find(_rowset->rowset_id()) != _context->segments.end()) {
        if (_context->segments.find(_rowset->rowset_id())->second.size() >
            _rowset->_segments.size()) {
            LOG(WARNING) << "failed to init beta rowset reader, actrual segments number is "
                         << _rowset->_segments.size() << ", excepted is "
                         << _context->segments.find(_rowset->rowset_id())->second.size();
            return OLAP_ERR_ROWSET_READER_INIT;
        }
        for (auto segment_id : _context->segments.find(_rowset->rowset_id())->second) {
            std::unique_ptr<RowwiseIterator> iter;
            auto s = _rowset->_segments[segment_id]->new_iterator(schema, read_options,
                                                                  _parent_tracker, &iter);
            if (!s.ok()) {
                LOG(WARNING) << "failed to create iterator[" << segment_id
                             << "]: " << s.to_string();
                return OLAP_ERR_ROWSET_READER_INIT;
            }
            seg_iterators.push_back(std::move(iter));
        }
    } else {
        for (auto& seg_ptr : _rowset->_segments) {
            std::unique_ptr<RowwiseIterator> iter;
            auto s = seg_ptr->new_iterator(schema, read_options, _parent_tracker, &iter);
            if (!s.ok()) {
                LOG(WARNING) << "failed to create iterator[" << seg_ptr->id()
                             << "]: " << s.to_string();
                return OLAP_ERR_ROWSET_READER_INIT;
            }
            seg_iterators.push_back(std::move(iter));
        }
    }
    std::list<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) {
        // transfer ownership of segment iterator to `_iterator`
        iterators.push_back(owned_it.release());
    }

    // merge or union segment iterator
    RowwiseIterator* final_iterator;
    if (read_context->need_ordered_result && _rowset->rowset_meta()->is_segments_overlapping()) {
        final_iterator = new_merge_iterator(iterators, _parent_tracker);
    } else {
        final_iterator = new_union_iterator(iterators, _parent_tracker);
    }
    auto s = final_iterator->init(read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        return OLAP_ERR_ROWSET_READER_INIT;
    }
    _iterator.reset(final_iterator);

    // init input block
    _input_block.reset(new RowBlockV2(schema, 1024, _parent_tracker));

    // init input/output block and row
    _output_block.reset(new RowBlock(read_context->tablet_schema, _parent_tracker));

    RowBlockInfo output_block_info;
    output_block_info.row_num = 1024;
    output_block_info.null_supported = true;
    // the output block's schema should be seek_columns to conform to v1
    // TODO(hkp): this should be optimized to use return_columns
    output_block_info.column_ids = *(_context->seek_columns);
    _output_block->init(output_block_info);
    _row.reset(new RowCursor());
    RETURN_NOT_OK(_row->init(*(read_context->tablet_schema), *(_context->seek_columns)));

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

} // namespace doris
