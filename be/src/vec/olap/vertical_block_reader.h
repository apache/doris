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

#include <parallel_hashmap/phmap.h>

#include "olap/reader.h"
#include "olap/rowset/rowset_reader.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/olap/vcollect_iterator.h"
#include "vec/olap/vertical_merge_iterator.h"

#pragma once

namespace doris {

namespace vectorized {

class VerticalBlockReader final : public TabletReader {
public:
    VerticalBlockReader(RowSourcesBuffer* row_sources_buffer)
            : _row_sources_buffer(row_sources_buffer) {}

    ~VerticalBlockReader() override;

    // Initialize VerticalBlockReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;

    Status next_block_with_aggregation(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                       bool* eof) override {
        return (this->*_next_block_func)(block, mem_pool, agg_pool, eof);
    }

    Status next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                     bool* eof) override {
        return Status::OK();
    }

    uint64_t merged_rows() const override {
        DCHECK(_vcollect_iter);
        return _vcollect_iter->merged_rows();
    }

private:
    // Directly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    Status _direct_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    Status _agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_block is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    Status _unique_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof);

    Status _init_collect_iter(const ReaderParams& read_params);

    Status _get_segment_iterators(const ReaderParams& read_params,
                                  std::vector<RowwiseIterator*>* segment_iters);

    void _init_agg_state(const ReaderParams& read_params);
    void _append_agg_data(MutableColumns& columns);
    void _update_agg_data(MutableColumns& columns);
    size_t _copy_agg_data();
    void _update_agg_value(MutableColumns& columns, int begin, int end, bool is_close = true);

private:
    std::shared_ptr<RowwiseIterator> _vcollect_iter;
    IteratorRowRef _next_row {{}, -1, false};

    bool _eof = false;

    Status (VerticalBlockReader::*_next_block_func)(Block* block, MemPool* mem_pool,
                                                    ObjectPool* agg_pool, bool* eof) = nullptr;

    RowSourcesBuffer* _row_sources_buffer;
    ColumnPtr _delete_filter_column;

    // for agg mode
    std::vector<AggregateFunctionPtr> _agg_functions;
    std::vector<AggregateDataPtr> _agg_places;

    std::vector<int> _normal_columns_idx;
    std::vector<int> _agg_columns_idx;

    std::vector<int> _agg_data_counters;
    int _last_agg_data_counter = 0;

    MutableColumns _stored_data_columns;
    std::vector<IteratorRowRef> _stored_row_ref;

    std::vector<bool> _stored_has_null_tag;
    std::vector<bool> _stored_has_string_tag;

    phmap::flat_hash_map<const Block*, std::vector<std::pair<int, int>>> _temp_ref_map;
};

} // namespace vectorized
} // namespace doris
