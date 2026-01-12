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

#include "util/bit_util.h"
#include "util/simd/expand.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <tparquet::Type::type type>
struct PhysicalTypeTraits {};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT32> {
    using CppType = int32_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT64> {
    using CppType = int64_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT96> {
    using CppType = ParquetInt96;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FLOAT> {
    using CppType = float;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::DOUBLE> {
    using CppType = double;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FIXED_LEN_BYTE_ARRAY> {
    using CppType = Slice;
};

template <tparquet::Type::type PhysicalType>
class FixLengthDictDecoder final : public BaseDictDecoder {
public:
    using cppType = PhysicalTypeTraits<PhysicalType>::CppType;
    FixLengthDictDecoder() = default;
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        if (doris_column->is_column_dictionary() &&
            assert_cast<ColumnDictI32&>(*doris_column).dict_size() == 0) {
            std::vector<StringRef> dict_items;

            char* dict_item_address = (char*)_dict.get();
            dict_items.reserve(_dict_items.size());
            for (int i = 0; i < _dict_items.size(); ++i) {
                dict_items.emplace_back(dict_item_address, _type_length);
                dict_item_address += _type_length;
            }
            assert_cast<ColumnDictI32&>(*doris_column)
                    .insert_many_dict_data(dict_items.data(),
                                           cast_set<uint32_t>(dict_items.size()));
        }
        if constexpr (!has_filter) {
            if (!doris_column->is_column_dictionary() && !is_dict_filter) {
                if constexpr (PhysicalType != tparquet::Type::FIXED_LEN_BYTE_ARRAY &&
                              PhysicalType != tparquet::Type::INT96) {
                    size_t num_values = select_vector.num_values();
                    size_t dict_bytes = _dict_items.size() * _type_length;
                    // Use a default L2 cache size (256KB) as CpuInfo::get_l2_cache_size() has a bug
                    // that causes SIGSEGV when cache_line_sizes is nullptr
                    constexpr size_t DEFAULT_L2_CACHE_SIZE = 256 * 1024;
                    if (dict_bytes > 0 && parquet_cache_aware_dict_decoder_enable &&
                        dict_bytes <= DEFAULT_L2_CACHE_SIZE) {
                        // Prepare destination buffer (same logic as in _decode_fixed_values)
                        size_t primitive_length =
                                remove_nullable(data_type)->get_size_of_value_in_memory();
                        size_t data_index = doris_column->size() * primitive_length;
                        size_t scale_size =
                                (select_vector.num_values() - select_vector.num_filtered()) *
                                (_type_length / primitive_length);
                        doris_column->resize(doris_column->size() + scale_size);
                        char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);
                        cppType* dst = reinterpret_cast<cppType*>(raw_data + data_index);

                        // When there are nulls, use GetBatchWithDict + assign_data_with_nulls
                        if (select_vector.num_nulls() > 0) {
                            const uint8_t* null_map = select_vector.get_null_map_data();
                            if (null_map == nullptr) {
                                return Status::InternalError("null_map is null");
                            }

                            std::vector<cppType> read_data(non_null_size);
                            auto ret = _index_batch_decoder->GetBatchWithDict(
                                    _dict_items.data(), cast_set<int32_t>(_dict_items.size()),
                                    read_data.data(), cast_set<int32_t>(non_null_size));
                            if (UNLIKELY(ret <= 0)) {
                                return Status::InternalError("DictDecoder GetBatchWithDict failed");
                            }

                            assign_data_with_nulls(num_values, non_null_size, null_map,
                                                   read_data.data(), dst);
                            return Status::OK();
                        }

                        // No nulls case: directly use GetBatchWithDict
                        auto ret = _index_batch_decoder->GetBatchWithDict(
                                _dict_items.data(), cast_set<int32_t>(_dict_items.size()), dst,
                                cast_set<int32_t>(num_values));
                        if (ret == 0) {
                            return Status::InternalError(
                                    "DictDecoder GetBatchWithDict failed (truncated input)");
                        }
                        if (ret == static_cast<uint32_t>(-1)) {
                            return Status::InternalError("Index not in dictionary bounds");
                        }
                        return Status::OK();
                    }
                }
            }
        }

        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(_indexes.data(), cast_set<uint32_t>(non_null_size));

        if (doris_column->is_column_dictionary() || is_dict_filter) {
            return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
        }

        return _decode_fixed_values<has_filter>(doris_column, data_type, select_vector);
    }

protected:
    template <bool has_filter>
    Status _decode_fixed_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                ColumnSelectVector& select_vector) {
        size_t primitive_length = remove_nullable(data_type)->get_size_of_value_in_memory();
        size_t data_index = doris_column->size() * primitive_length;
        size_t scale_size = (select_vector.num_values() - select_vector.num_filtered()) *
                            (_type_length / primitive_length);
        doris_column->resize(doris_column->size() + scale_size);
        // doris_column is of type MutableColumnPtr, which uses get_raw_data
        // to return a StringRef, hence the use of const_cast.
        char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);

        // Try cache-aware fast path for numeric fixed-length types without filters
        if constexpr (!has_filter) {
            if constexpr (PhysicalType != tparquet::Type::FIXED_LEN_BYTE_ARRAY &&
                          PhysicalType != tparquet::Type::INT96) {
                // Use SIMD optimized path when there are nulls
                if (select_vector.num_nulls() > 0) {
                    return _decode_fixed_values_simd(doris_column, select_vector, raw_data,
                                                     data_index);
                }
            }
        }

        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                        auto& slice = _dict_items[_indexes[dict_index++]];
                        memcpy(raw_data + data_index, slice.get_data(), _type_length);
                    } else {
                        *(cppType*)(raw_data + data_index) = _dict_items[_indexes[dict_index++]];
                    }
                    data_index += _type_length;
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length * _type_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // do nothing
                break;
            }
            }
        }
        return Status::OK();
    }

    // Assign data with nulls using SIMD optimization (similar to StarRocks)
    template <class DataType>
    void assign_data_with_nulls(size_t count, size_t num_non_nulls, const uint8_t* nulls,
                                const DataType* src_data, DataType* dst_data) {
        // opt branch for process sparse column
        if (num_non_nulls < count / 10) {
            size_t cnt = 0;
            size_t i = 0;
#ifdef __AVX2__
            for (i = 0; i + 32 <= count; i += 32) {
                // Load the next 32 elements of nulls into a mask
                __m256i loaded = _mm256_loadu_si256((__m256i*)&nulls[i]);
                int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(loaded, _mm256_setzero_si256()));
                // Process non-null positions using bitmask iteration
                while (mask != 0) {
                    int idx = __builtin_ctz(mask);
                    dst_data[i + idx] = src_data[cnt++];
                    mask &= (mask - 1);
                }
            }
#endif
            // process tail elements
            for (; i < count; ++i) {
                dst_data[i] = src_data[cnt];
                cnt += !nulls[i];
            }
        } else {
            // size_t cnt = 0;
            // for (size_t i = 0; i < count; ++i) {
            //     dst_data[i] = src_data[cnt];
            //     cnt += !nulls[i];
            // }
            // Use SIMD expand helper to assign values while respecting nulls.
            // The simd::Expand::expand_load dispatches to optimized implementations
            // for 32-bit and 64-bit types, falling back to branchless scalar otherwise.
            simd::Expand::expand_load<DataType>(dst_data, src_data, nulls, count);
        }
    }

    // SIMD optimized decoding for fixed-length numeric types with nulls
    // This is called when NOT in L2 cache path, so _indexes is already populated
    Status _decode_fixed_values_simd(MutableColumnPtr& doris_column,
                                     ColumnSelectVector& select_vector, char* raw_data,
                                     size_t data_index) {
        const size_t num_values = select_vector.num_values();
        const size_t non_null_size = num_values - select_vector.num_nulls();

        // Get null_map directly from ColumnSelectVector (already built during init)
        const uint8_t* null_map = select_vector.get_null_map_data();
        if (null_map == nullptr) {
            return Status::InternalError("null_map is null");
        }

        cppType* dst_data = reinterpret_cast<cppType*>(raw_data + data_index);

        // Batch read dictionary values into a compact temporary buffer using _indexes
        std::vector<cppType> compact_values(non_null_size);
        for (size_t i = 0; i < non_null_size; ++i) {
            compact_values[i] = _dict_items[_indexes[i]];
        }

        // Assign data with nulls handling
        assign_data_with_nulls(num_values, non_null_size, null_map, compact_values.data(),
                               dst_data);

        return Status::OK();
    }

    Status set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                    size_t num_values) override {
        if (num_values * _type_length != length) {
            return Status::Corruption("Wrong dictionary data for fixed length type");
        }
        _dict = std::move(dict);
        if (_dict == nullptr) {
            return Status::Corruption("Wrong dictionary data for byte array type, dict is null.");
        }
        char* dict_item_address = reinterpret_cast<char*>(_dict.get());
        _dict_items.resize(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                _dict_items[i] = Slice {dict_item_address, (size_t)_type_length};
            } else {
                _dict_items[i] = *((cppType*)dict_item_address);
            }
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

    Status read_dict_values_to_column(MutableColumnPtr& doris_column) override {
        size_t dict_items_size = _dict_items.size();
        std::vector<StringRef> dict_values;
        dict_values.reserve(dict_items_size);
        auto* dict_item_address = (const char*)_dict.get();
        for (size_t i = 0; i < dict_items_size; ++i) {
            dict_values.emplace_back(dict_item_address, _type_length);
            dict_item_address += _type_length;
        }
        doris_column->insert_many_strings(&dict_values[0], dict_items_size);
        return Status::OK();
    }

    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) override {
        auto res = ColumnString::create();
        std::vector<StringRef> dict_values;
        dict_values.reserve(dict_column->size());
        const auto& data = dict_column->get_data();
        auto* dict_item_address = (const char*)_dict.get();

        for (size_t i = 0; i < dict_column->size(); ++i) {
            dict_values.emplace_back(dict_item_address + data[i] * _type_length, _type_length);
        }
        res->insert_many_strings(&dict_values[0], dict_values.size());
        return res;
    }
    // For dictionary encoding
    std::vector<typename PhysicalTypeTraits<PhysicalType>::CppType> _dict_items;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
