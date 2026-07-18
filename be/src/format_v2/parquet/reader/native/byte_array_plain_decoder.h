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

#include <string.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "format/format_common.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris {
template <PrimitiveType T>
class ColumnDecimal;
} // namespace doris

namespace doris::format::parquet::native {
class ByteArrayPlainDecoder final : public Decoder {
public:
    ByteArrayPlainDecoder() = default;
    ~ByteArrayPlainDecoder() override = default;

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override;

    Status decode_selected_binary_values(const ParquetSelection& selection,
                                         ParquetBinaryValueConsumer& consumer) override;

    Status skip_values(size_t num_values) override;

    void release_scratch(size_t max_retained_bytes) override {
        release_vector_if_oversized(&_payload_offsets, max_retained_bytes);
        release_vector_if_oversized(&_value_offsets, max_retained_bytes);
        release_vector_if_oversized(&_value_spans, max_retained_bytes);
    }
    size_t retained_scratch_bytes() const override {
        return (_payload_offsets.capacity() + _value_offsets.capacity()) * sizeof(uint32_t) +
               _value_spans.capacity() * sizeof(ParquetSelectionRange);
    }
    size_t active_scratch_bytes() const override {
        return (_payload_offsets.size() + _value_offsets.size()) * sizeof(uint32_t) +
               _value_spans.size() * sizeof(ParquetSelectionRange);
    }

private:
    std::vector<uint32_t> _payload_offsets;
    std::vector<uint32_t> _value_offsets;
    std::vector<ParquetSelectionRange> _value_spans;
};

} // namespace doris::format::parquet::native
