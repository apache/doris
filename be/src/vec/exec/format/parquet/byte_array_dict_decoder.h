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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "util/bit_util.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris {
namespace vectorized {
class ColumnString;
template <typename T>
class ColumnDecimal;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class ByteArrayDictDecoder final : public BaseDictDecoder {
public:
    ByteArrayDictDecoder() = default;
    ~ByteArrayDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

    Status read_dict_values_to_column(MutableColumnPtr& doris_column) override;

    Status get_dict_codes(const ColumnString* column_string,
                          std::vector<int32_t>* dict_codes) override;

    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) override;

protected:
    // For dictionary encoding
    std::vector<StringRef> _dict_items;
    std::vector<uint8_t> _dict_data;
    size_t _max_value_length;
    std::unordered_map<StringRef, int32_t> _dict_value_to_code;
};
} // namespace doris::vectorized
