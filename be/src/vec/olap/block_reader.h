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

#include "olap/collect_iterator.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/olap/vcollect_iterator.h"

namespace doris {

namespace vectorized {

class BlockReader final : public TabletReader {
public:
    ~BlockReader() override;

    // Initialize BlockReader with tablet, data version and fetch range.
    OLAPStatus init(const ReaderParams& read_params) override;

    OLAPStatus next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) override {
        return OLAP_ERR_READER_INITIALIZE_ERROR;
    }

    OLAPStatus next_block_with_aggregation(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                           bool* eof) override {
        return (this->*_next_block_func)(block, mem_pool, agg_pool, eof);
    }

private:
    friend class VCollectIterator;
    friend class DeleteHandler;

    // Directly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    OLAPStatus _direct_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof);
    // Just same as _direct_next_block, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    OLAPStatus _direct_agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                          bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    OLAPStatus _agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                   bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_block is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    OLAPStatus _unique_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                      bool* eof);

    OLAPStatus _init_collect_iter(const ReaderParams& read_params,
                                  std::vector<RowsetReaderSharedPtr>* valid_rs_readers);

    void _init_agg_state(const ReaderParams& read_params);

    void _insert_data_normal(MutableColumns& columns);

    void _append_agg_data(MutableColumns& columns);

    void _update_agg_data(MutableColumns& columns);

    size_t _copy_agg_data();

    void _update_agg_value(MutableColumns& columns, int begin, int end, bool is_close = true);

    VCollectIterator _vcollect_iter;
    IteratorRowRef _next_row {{}, -1, false};

    std::vector<AggregateFunctionPtr> _agg_functions;
    std::vector<AggregateDataPtr> _agg_places;

    std::vector<int> _normal_columns_idx; // key column on agg mode, all column on uniq mode
    std::vector<int> _agg_columns_idx;
    std::vector<int> _return_columns_loc;

    std::vector<int> _agg_data_counters;
    int _last_agg_data_counter = 0;

    MutableColumns _stored_data_columns;
    std::vector<IteratorRowRef> _stored_row_ref;

    std::vector<bool> _stored_has_null_tag;
    std::vector<bool> _stored_has_string_tag;

    phmap::flat_hash_map<const Block*, std::vector<std::pair<int16_t, int16_t>>> _temp_ref_map;

    bool _eof = false;

    OLAPStatus (BlockReader::*_next_block_func)(Block* block, MemPool* mem_pool,
                                                ObjectPool* agg_pool, bool* eof) = nullptr;
};

} // namespace vectorized
} // namespace doris
