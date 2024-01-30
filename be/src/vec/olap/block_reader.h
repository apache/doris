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
#include <stddef.h>

#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/utils.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/olap/vcollect_iterator.h"

namespace doris {
class ColumnPredicate;
class FunctionFilter;
class RuntimeProfile;

namespace vectorized {

class BlockReader final : public TabletReader {
public:
    ~BlockReader() override;

    // Initialize BlockReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;

    Status next_block_with_aggregation(Block* block, bool* eof) override {
        auto res = (this->*_next_block_func)(block, eof);
        if (UNLIKELY(!res.ok() && !res.is<ErrorCode::END_OF_FILE>())) {
            _tablet->report_error(res);
        }
        return res;
    }

    std::vector<RowLocation> current_block_row_locations() { return _block_row_locations; }

    bool update_profile(RuntimeProfile* profile) override {
        return _vcollect_iter.update_profile(profile);
    }

private:
    // Directly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    Status _direct_next_block(Block* block, bool* eof);
    // Just same as _direct_next_block, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    Status _direct_agg_key_next_block(Block* block, bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    Status _agg_key_next_block(Block* block, bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_block is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    Status _unique_key_next_block(Block* block, bool* eof);

    Status _init_collect_iter(const ReaderParams& read_params);

    void _init_agg_state(const ReaderParams& read_params);

    Status _insert_data_normal(MutableColumns& columns);

    void _append_agg_data(MutableColumns& columns);

    void _update_agg_data(MutableColumns& columns);

    size_t _copy_agg_data();

    void _update_agg_value(MutableColumns& columns, int begin, int end, bool is_close = true);

    bool _get_next_row_same();

    bool _rowsets_overlapping(const ReaderParams& read_params);

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
    std::vector<bool> _stored_has_variable_length_tag;

    phmap::flat_hash_map<const Block*, std::vector<std::pair<int, int>>> _temp_ref_map;

    bool _eof = false;

    Status (BlockReader::*_next_block_func)(Block* block, bool* eof) = nullptr;

    std::vector<RowLocation> _block_row_locations;

    ColumnPtr _delete_filter_column;

    bool _is_rowsets_overlapping = true;

    Arena _arena;
};

} // namespace vectorized
} // namespace doris
