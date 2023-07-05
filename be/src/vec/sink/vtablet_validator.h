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
#include "runtime/types.h"
#include "util/bitmap.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"

namespace doris {
namespace stream_load {

class OlapTableValidator {
public:
    OlapTableValidator(TupleDescriptor* output_tuple_desc)
            : _output_tuple_desc(output_tuple_desc) {}

    // make input data valid for OLAP table
    // return number of invalid/filtered rows.
    // invalid row number is set in Bitmap
    // set stop_processing if we want to stop the whole process now.
    Status validate_data(RuntimeState* state, vectorized::Block* block, Bitmap* filter_bitmap,
                         int* filtered_rows, bool* stop_processing);

    // some output column of output expr may have different nullable property with dest slot desc
    // so here need to do the convert operation
    void convert_to_dest_desc_block(vectorized::Block* block);

private:
    template <bool is_min>
    DecimalV2Value _get_decimalv2_min_or_max(const TypeDescriptor& type);

    template <typename DecimalType, bool IsMin>
    DecimalType _get_decimalv3_min_or_max(const TypeDescriptor& type);

    Status _validate_column(RuntimeState* state, const TypeDescriptor& type, bool is_nullable,
                            vectorized::ColumnPtr column, size_t slot_index, Bitmap* filter_bitmap,
                            bool* stop_processing, fmt::memory_buffer& error_prefix,
                            vectorized::IColumn::Permutation* rows = nullptr);

    TupleDescriptor* _output_tuple_desc = nullptr;

    std::map<std::pair<int, int>, DecimalV2Value> _max_decimalv2_val;
    std::map<std::pair<int, int>, DecimalV2Value> _min_decimalv2_val;

    std::map<int, int32_t> _max_decimal32_val;
    std::map<int, int32_t> _min_decimal32_val;
    std::map<int, int64_t> _max_decimal64_val;
    std::map<int, int64_t> _min_decimal64_val;
    std::map<int, int128_t> _max_decimal128_val;
    std::map<int, int128_t> _min_decimal128_val;
};

} // namespace stream_load
} // namespace doris