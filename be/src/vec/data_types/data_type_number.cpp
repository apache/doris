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

#include "data_type_number.h"

#include <fmt/format.h>

#include <cmath>
#include <type_traits>

#include "gutil/strings/numbers.h"
#include "util/mysql_global.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/// TODO: Currently, only integers have been considered; other types will be added later.
template <typename T>
size_t DataTypeNumber<T>::number_length() const {
    // The maximum number of decimal digits for an integer represented by n bytes:
    // 1. Each byte = 8 bits, so n bytes = 8n bits.
    // 2. Maximum value of an 8n-bit integer = 2^(8n) - 1.
    // 3. Number of decimal digits d = floor(log10(2^(8n) - 1)) + 1.
    // 4. Approximation: log10(2^(8n)) ≈ 8n * log10(2).
    // 5. log10(2) ≈ 0.30103, so 8n * log10(2) ≈ 2.40824n.
    // 6. Therefore, d ≈ floor(2.408 * n) + 1.
    return size_t(2.408 * sizeof(T)) + 1;
}

template <typename T>
void DataTypeNumber<T>::push_number(ColumnString::Chars& chars, const T& num) const {
    if constexpr (std::is_same<T, UInt128>::value) {
        std::string hex = int128_to_string(num);
        chars.insert(hex.begin(), hex.end());
    } else if constexpr (std::is_same_v<T, float>) {
        char buf[MAX_FLOAT_STR_LENGTH + 2];
        int len = FloatToBuffer(num, MAX_FLOAT_STR_LENGTH + 2, buf);
        chars.insert(buf, buf + len);
    } else if constexpr (std::is_same_v<Int128, T> || std::numeric_limits<T>::is_iec559) {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", num);
        chars.insert(buffer.data(), buffer.data() + buffer.size());
    } else {
        auto f = fmt::format_int(num);
        chars.insert(f.data(), f.data() + f.size());
    }
}

template class DataTypeNumber<UInt8>;
template class DataTypeNumber<UInt16>;
template class DataTypeNumber<UInt32>;
template class DataTypeNumber<UInt64>;
template class DataTypeNumber<UInt128>;
template class DataTypeNumber<Int8>;
template class DataTypeNumber<Int16>;
template class DataTypeNumber<Int32>;
template class DataTypeNumber<Int64>;
template class DataTypeNumber<Int128>;
template class DataTypeNumber<Float32>;
template class DataTypeNumber<Float64>;

} // namespace doris::vectorized
