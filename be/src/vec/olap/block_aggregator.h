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

#include <vector>

#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/olap/column_aggregator.h"

namespace doris::vectorized {

using CompareFunc = void (*)(const vectorized::IColumn& col, uint8_t* flags);

class BlockAggregator {
public:
    BlockAggregator(const TabletSchema* tablet_schema, const std::vector<uint32_t>& output_columns,
                    const std::vector<uint32_t>& return_columns,
                    const std::unordered_set<uint32_t>* null_set, int batch_size);

    void update_source(const Block* block);

    void aggregate();

    bool source_exhausted() { return _current_row == _source_size; }

    bool is_finish() { return agg_block_rows() >= _batch_size; }

    std::shared_ptr<Block> aggregate_result();

    bool has_agg_data() { return _has_agg_data; }

    void aggregate_reset();

    bool is_do_aggregate() { return _do_aggregate; }

    size_t agg_block_rows();

private:
    CompareFunc _get_comparator(const TabletColumn& col);

    const TabletSchema* _tablet_schema;
    std::vector<uint32_t> _output_columns;
    std::vector<uint32_t> _sorted_output_columns;
    std::vector<uint32_t> _return_columns;
    const std::unordered_set<uint32_t>* _null_set;
    std::vector<uint32_t> _output_columns_loc;

    size_t _num_key_columns;
    size_t _num_columns;
    bool _has_agg_data;
    int _batch_size;
    bool _do_aggregate;
    double _agg_ratio;

    uint32_t _current_row;
    uint32_t _source_size;

    std::vector<CompareFunc> _key_comparator;
    std::vector<ColumnAggregatorPtr> _column_aggregator;
    std::vector<uint8_t> _eq_previous;

    std::vector<uint32_t> _agg_index;
    std::vector<uint32_t> _agg_range;

    std::shared_ptr<Block> _result_block;
};

} // namespace doris::vectorized