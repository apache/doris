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

#include "olap/generic_iterators.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/schema.h"

namespace doris {

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
    : _rowset(std::move(rowset)) {
}

OLAPStatus BetaRowsetReader::init(RowsetReaderContext* read_context) {
    _context = read_context;

    Schema schema(_context->tablet_schema->columns(), *(_context->seek_columns));

    // convert RowsetReaderContext to StorageReadOptions
    StorageReadOptions read_options;
    read_options.conditions = read_context->conditions;
    for (int i = 0; i < read_context->lower_bound_keys->size(); ++i) {
        read_options.key_ranges.emplace_back(
            read_context->lower_bound_keys->at(i),
            read_context->is_lower_keys_included->at(i),
            read_context->upper_bound_keys->at(i),
            read_context->is_upper_keys_included->at(i));
    }

    // create iterator for each segment
    std::vector<std::unique_ptr<segment_v2::SegmentIterator>> seg_iterators;
    for (auto& seg_ptr : _rowset->_segments) {
        seg_iterators.push_back(seg_ptr->new_iterator(schema, read_options));
    }
    std::vector<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) {
        // transfer ownership of segment iterator to `_iterator`
        iterators.push_back(owned_it.release());
    }

    // merge or union segment iterator
    bool is_singleton_rowset = _rowset->start_version() && _rowset->end_version();
    if (read_context->need_ordered_result && is_singleton_rowset && iterators.size() > 1) {
        _iterator.reset(new_merge_iterator(iterators));
    } else {
        _iterator.reset(new_union_iterator(iterators));
    }

    // init input block
    _input_block.reset(new RowBlockV2(schema, 1024));

    // init output block and row
    _output_block.reset(new RowBlock(read_context->tablet_schema));
    RowBlockInfo output_block_info;
    output_block_info.row_num = 1024;
    output_block_info.null_supported = true;
    output_block_info.column_ids = schema.column_ids();
    RETURN_NOT_OK(_output_block->init(output_block_info));
    _row.reset(new RowCursor());
    RETURN_NOT_OK(_row->init(*(read_context->tablet_schema), schema.column_ids()));

    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetReader::next_block(RowBlock** block) {
    // read next input block
    _input_block->clear();
    auto s = _iterator->next_batch(_input_block.get());
    if (!s.ok()) {
        if (s.is_end_of_file()) {
            *block = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
        LOG(WARNING) << "failed to read next block: " << s.to_string();
        return OLAP_ERR_ROWSET_READ_FAILED;
    }
    // convert to output block
    _output_block->clear();
    for (size_t row_idx = 0; row_idx < _input_block->num_rows(); ++row_idx) {
        // shallow copy row from input block to output block
        _output_block->get_row(row_idx, _row.get());
        s = _input_block->copy_to_row_cursor(row_idx, _row.get());
        if (!s.ok()) {
            LOG(WARNING) << "failed to copy row: " << s.to_string();
            return OLAP_ERR_ROWSET_READ_FAILED;
        }
    }
    _output_block->finalize(_input_block->num_rows());
    return OLAP_SUCCESS;
}

} // namespace doris
