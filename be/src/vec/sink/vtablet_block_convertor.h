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
#include <stddef.h>
#include <stdint.h>

// IWYU pragma: no_include <bits/chrono.h>

#include <chrono> // IWYU pragma: keep
#include <map>

#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/bitmap.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/autoinc_buffer.h"

namespace doris::vectorized {

class OlapTableBlockConvertor {
public:
    OlapTableBlockConvertor(TupleDescriptor* output_tuple_desc)
            : _output_tuple_desc(output_tuple_desc) {}

    Status validate_and_convert_block(RuntimeState* state, vectorized::Block* input_block,
                                      std::shared_ptr<vectorized::Block>& block,
                                      vectorized::VExprContextSPtrs output_vexpr_ctxs, size_t rows,
                                      bool& has_filtered_rows);

    const char* filter_map() const { return _filter_map.data(); }

    int64_t validate_data_ns() const { return _validate_data_ns; }

    int64_t num_filtered_rows() const { return _num_filtered_rows; }

    void init_autoinc_info(int64_t db_id, int64_t table_id, int batch_size,
                           bool is_partial_update_and_auto_inc = false,
                           int32_t auto_increment_column_unique_id = -1);

    AutoIncIDAllocator& auto_inc_id_allocator() { return _auto_inc_id_allocator; }

private:
    template <bool is_min>
    DecimalV2Value _get_decimalv2_min_or_max(const TypeDescriptor& type);

    template <typename DecimalType, bool IsMin>
    DecimalType _get_decimalv3_min_or_max(const TypeDescriptor& type);

    Status _validate_column(RuntimeState* state, const TypeDescriptor& type, bool is_nullable,
                            vectorized::ColumnPtr column, size_t slot_index, bool* stop_processing,
                            fmt::memory_buffer& error_prefix, const uint32_t row_count,
                            vectorized::IColumn::Permutation* rows = nullptr);

    // make input data valid for OLAP table
    // return number of invalid/filtered rows.
    // invalid row number is set in Bitmap
    // set stop_processing if we want to stop the whole process now.
    Status _validate_data(RuntimeState* state, vectorized::Block* block, const uint32_t rows,
                          int& filtered_rows, bool* stop_processing);

    // some output column of output expr may have different nullable property with dest slot desc
    // so here need to do the convert operation
    void _convert_to_dest_desc_block(vectorized::Block* block);

    Status _fill_auto_inc_cols(vectorized::Block* block, size_t rows);

    Status _partial_update_fill_auto_inc_cols(vectorized::Block* block, size_t rows);

    TupleDescriptor* _output_tuple_desc = nullptr;

    std::map<std::pair<int, int>, DecimalV2Value> _max_decimalv2_val;
    std::map<std::pair<int, int>, DecimalV2Value> _min_decimalv2_val;

    std::map<int, int32_t> _max_decimal32_val;
    std::map<int, int32_t> _min_decimal32_val;
    std::map<int, int64_t> _max_decimal64_val;
    std::map<int, int64_t> _min_decimal64_val;
    std::map<int, int128_t> _max_decimal128_val;
    std::map<int, int128_t> _min_decimal128_val;
    std::map<int, wide::Int256> _max_decimal256_val;
    std::map<int, wide::Int256> _min_decimal256_val;

    std::vector<char> _filter_map;

    int64_t _validate_data_ns = 0;
    int64_t _num_filtered_rows = 0;

    size_t _batch_size;
    std::optional<size_t> _auto_inc_col_idx;
    std::shared_ptr<AutoIncIDBuffer> _auto_inc_id_buffer = nullptr;
    AutoIncIDAllocator _auto_inc_id_allocator;
    bool _is_partial_update_and_auto_inc = false;
};

} // namespace doris::vectorized