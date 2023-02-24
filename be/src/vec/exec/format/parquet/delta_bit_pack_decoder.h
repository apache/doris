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

#include "util/bit_stream_utils.h"
#include "vec/columns/column.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris::vectorized {
/**
 *   Format
 *      [header] [block 1] [block 2] ... [block N]
 *   Header
 *      [block size] [_mini_blocks_per_block] [_total_value_count] [first value]
 *   Block
 *      [min delta] [list of bitwidths of the mini blocks] [miniblocks]
 */
template <typename T>
class DeltaBitPackDecoder final : public Decoder {
public:
    using UT = std::make_unsigned_t<T>;

    DeltaBitPackDecoder() = default;
    ~DeltaBitPackDecoder() override = default;
    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override {
        select_vector.num_values();
        return Status::OK();
    }

    Status skip_values(size_t num_values) override { return Status::OK(); }

    void set_data(Slice* slice) override {
        _decoder.reset(new BitReader((const uint8_t*)slice->data, slice->size));
        _data = slice;
        _offset = 0;
    }

private:
    static constexpr int kMaxDeltaBitWidth = static_cast<int>(sizeof(T) * 8);
    Status _init_header();
    Status _init_block();
    Status _init_mini_block(int bit_width);
    Status _get_internal(T* buffer, int max_values, int* out);

    std::shared_ptr<BitReader> _decoder;
    uint32_t _values_per_block;
    uint32_t _mini_blocks_per_block;
    uint32_t _values_per_mini_block;
    uint32_t _total_value_count;

    T _min_delta;
    T _last_value;

    uint32_t _mini_block_idx;
    Slice _delta_bit_widths;
    uint32_t _delta_bit_width;
    // If the page doesn't contain any block, `_block_initialized` will
    // always be false. Otherwise, it will be true when first block initialized.
    bool _block_initialized;

    uint32_t _total_values_remaining;
    // Remaining values in current mini block. If the current block is the last mini block,
    // _values_remaining_current_mini_block may greater than _total_values_remaining.
    uint32_t _values_remaining_current_mini_block;
};
} // namespace doris::vectorized