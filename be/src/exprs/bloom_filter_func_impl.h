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

#include <type_traits>

#include "exprs/bloom_filter_func_adaptor.h"
#include "runtime/primitive_type.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/string_ref.h"

namespace doris {
#include "common/compile_check_begin.h"
struct fixed_len_to_uint32_v2 {
    template <typename T>
    uint32_t operator()(const T& value) {
        if constexpr (sizeof(T) <= sizeof(uint32_t)) {
            if constexpr (std::is_same_v<T, DateV2Value<DateV2ValueType>>) {
                return (uint32_t)value.to_date_int_val();
            } else if constexpr (vectorized::IsDecimalNumber<T>) {
                return (uint32_t)value.value;
            } else {
                return (uint32_t)value;
            }
        } else {
            if constexpr (std::is_same_v<DecimalV2Value, T> ||
                          std::is_same_v<VecDateTimeValue, T>) {
                return uint32_t(std::hash<T>()(value));
            } else if constexpr (std::is_same_v<T, DateV2Value<DateTimeV2ValueType>>) {
                return uint32_t(HashCRC32<DateV2Value<DateTimeV2ValueType>::underlying_value>()(
                        value.to_date_int_val()));
            } else if constexpr (vectorized::IsDecimalNumber<T>) {
                return uint32_t(HashCRC32<typename T::NativeType>()(value.value));
            } else {
                return uint32_t(HashCRC32<T>()(value));
            }
        }
    }
};

template <typename fixed_len_to_uint32_method, typename T, bool need_trim = false>
uint16_t find_batch_olap(const BloomFilterAdaptor& bloom_filter, const char* data,
                         const uint8_t* nullmap, uint16_t* offsets, int number,
                         const bool is_parse_column) {
    auto get_element = [](const char* input_data, int idx) {
        if constexpr (std::is_same_v<T, StringRef> && need_trim) {
            const auto value = ((const StringRef*)(input_data))[idx];
            int64_t size = value.size;
            const char* data = value.data;
            // CHAR type may pad the tail with \0, need to trim
            while (size > 0 && data[size - 1] == '\0') {
                size--;
            }
            return StringRef(value.data, size);
        } else {
            return ((const T*)(input_data))[idx];
        }
    };

    uint16_t new_size = 0;
    if (is_parse_column) {
        if (nullmap == nullptr) {
            for (uint16_t i = 0; i < number; i++) {
                uint16_t idx = offsets[i];
                if (!bloom_filter.test_element<fixed_len_to_uint32_method>(
                            get_element(data, idx))) {
                    continue;
                }
                offsets[new_size++] = idx;
            }
        } else {
            for (uint16_t i = 0; i < number; i++) {
                uint16_t idx = offsets[i];
                if (nullmap[idx]) {
                    if (!bloom_filter.contain_null()) {
                        continue;
                    }
                } else {
                    if (!bloom_filter.test_element<fixed_len_to_uint32_method>(
                                get_element(data, idx))) {
                        continue;
                    }
                }
                offsets[new_size++] = idx;
            }
        }
    } else {
        if (nullmap == nullptr) {
            for (uint16_t i = 0; i < number; i++) {
                if (!bloom_filter.test_element<fixed_len_to_uint32_method>(get_element(data, i))) {
                    continue;
                }
                offsets[new_size++] = i;
            }
        } else {
            for (uint16_t i = 0; i < number; i++) {
                if (nullmap[i]) {
                    if (!bloom_filter.contain_null()) {
                        continue;
                    }
                } else {
                    if (!bloom_filter.test_element<fixed_len_to_uint32_method>(
                                get_element(data, i))) {
                        continue;
                    }
                }
                offsets[new_size++] = i;
            }
        }
    }
    return new_size;
}
#include "common/compile_check_end.h"
} // namespace doris
