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

#include <vector>

#include "common/status.h"
#include "util/rle_encoding.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris {
namespace vectorized {
class ColumnSelectVector;
} // namespace vectorized
struct Slice;
} // namespace doris

namespace doris::vectorized {
class BoolRLEDecoder final : public Decoder {
public:
    BoolRLEDecoder() = default;
    ~BoolRLEDecoder() override = default;

    void set_data(Slice* slice) override;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status skip_values(size_t num_values) override;

private:
    RleDecoder<uint8_t> _decoder;
    std::vector<uint8_t> _values;
    size_t _num_bytes;
    size_t _current_value_idx = 0;
};
} // namespace doris::vectorized