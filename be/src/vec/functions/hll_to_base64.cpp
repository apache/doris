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

#include <stddef.h>
#include <stdint.h>

#include "olap/hll.h"
#include "util/url_coding.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameHllToBase64 {
    static constexpr auto name = "hll_to_base64";
};

struct HllToBase64 {
    using ReturnType = DataTypeString;
    static constexpr auto TYPE_INDEX = TypeIndex::HLL;
    using Type = DataTypeHLL::FieldType;
    using ReturnColumnType = ColumnString;
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;

    static Status vector(const std::vector<HyperLogLog>& data, Chars& chars, Offsets& offsets) {
        size_t size = data.size();
        offsets.resize(size);
        size_t output_char_size = 0;
        for (size_t i = 0; i < size; ++i) {
            HyperLogLog& hll_val = const_cast<HyperLogLog&>(data[i]);
            auto ser_size = hll_val.max_serialized_size();
            output_char_size += ser_size * (int)(4.0 * ceil((double)ser_size / 3.0));
        }
        ColumnString::check_chars_length(output_char_size, size);
        chars.resize(output_char_size);
        auto chars_data = chars.data();

        size_t cur_ser_size = 0;
        size_t last_ser_size = 0;
        std::string ser_buff;
        size_t encoded_offset = 0;
        for (size_t i = 0; i < size; ++i) {
            HyperLogLog& hll_val = const_cast<HyperLogLog&>(data[i]);

            cur_ser_size = hll_val.max_serialized_size();
            if (cur_ser_size > last_ser_size) {
                last_ser_size = cur_ser_size;
                ser_buff.resize(cur_ser_size);
            }
            hll_val.serialize(reinterpret_cast<uint8_t*>(ser_buff.data()));
            int outlen = base64_encode((const unsigned char*)ser_buff.data(), cur_ser_size,
                                       chars_data + encoded_offset);
            DCHECK(outlen > 0);

            encoded_offset += (int)(4.0 * ceil((double)cur_ser_size / 3.0));
            offsets[i] = encoded_offset;
        }
        return Status::OK();
    }
};

using FunctionHllToBase64 = FunctionUnaryToType<HllToBase64, NameHllToBase64>;

void register_function_hll_to_base64(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHllToBase64>();
}

} // namespace doris::vectorized