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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "common/compiler_util.h"
#include "common/config.h"
#include "util/bit_util.h"
#include "util/memcpy_inlined.h"
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
                         ColumnSelectVector& select_vector, bool is_dict_filter,
                         const uint8_t* filter_data = nullptr) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter,
                                        filter_data);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter,
                                         nullptr);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter,
                          const uint8_t* filter_data) {
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

        // When filter_data is provided and has_filter is true, use lazy index decoding:
        // decode indexes per-run and skip FILTERED_CONTENT via SkipBatch.
        if constexpr (has_filter) {
            if (filter_data != nullptr && !doris_column->is_column_dictionary() &&
                !is_dict_filter) {
                return _lazy_decode_fixed_values(doris_column, data_type, select_vector);
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
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    // Optimized path: use memcpy_inlined and reduce address calculations
                    char* dst_ptr = raw_data + data_index;
                    size_t i = 0;
                    // Loop unrolling: process 4 elements at a time
                    for (; i + 4 <= run_length; i += 4) {
                        auto& slice0 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice0.get_data(), _type_length);
                        dst_ptr += _type_length;

                        auto& slice1 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice1.get_data(), _type_length);
                        dst_ptr += _type_length;

                        auto& slice2 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice2.get_data(), _type_length);
                        dst_ptr += _type_length;

                        auto& slice3 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice3.get_data(), _type_length);
                        dst_ptr += _type_length;
                    }
                    // Process remaining elements
                    for (; i < run_length; ++i) {
                        auto& slice = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice.get_data(), _type_length);
                        dst_ptr += _type_length;
                    }
                    data_index = dst_ptr - raw_data;
                } else {
                    if (config::enable_parquet_simd_dict_decode) {
                        // P1-4: SIMD dict gather for scalar types (INT32/INT64/FLOAT/DOUBLE)
                        // P1-5: Software prefetch for large dictionaries
                        _simd_dict_gather(raw_data, data_index, dict_index, run_length);
                    } else if (_dict_exceeds_l2_cache && config::enable_parquet_dict_prefetch) {
                        // P1-5 only: scalar loop with software prefetch for large dicts
                        constexpr size_t PF_DIST = 8;
                        for (size_t i = 0; i < run_length; ++i) {
                            if (i + PF_DIST < run_length) {
                                PREFETCH(&_dict_items[_indexes[dict_index + i + PF_DIST]]);
                            }
                            *(cppType*)(raw_data + data_index) =
                                    _dict_items[_indexes[dict_index++]];
                            data_index += _type_length;
                        }
                    } else {
                        for (size_t i = 0; i < run_length; ++i) {
                            *(cppType*)(raw_data + data_index) =
                                    _dict_items[_indexes[dict_index++]];
                            data_index += _type_length;
                        }
                    }
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

    // Lazy index decoding path: decode indexes per-run, skip FILTERED_CONTENT via SkipBatch.
    Status _lazy_decode_fixed_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector) {
        size_t primitive_length = remove_nullable(data_type)->get_size_of_value_in_memory();
        size_t data_index = doris_column->size() * primitive_length;
        size_t scale_size = (select_vector.num_values() - select_vector.num_filtered()) *
                            (_type_length / primitive_length);
        doris_column->resize(doris_column->size() + scale_size);
        char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);

        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<true>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                // Decode only the indexes needed for this CONTENT run.
                _indexes.resize(run_length);
                _index_batch_decoder->GetBatch(_indexes.data(), cast_set<uint32_t>(run_length));
                if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    char* dst_ptr = raw_data + data_index;
                    for (size_t i = 0; i < run_length; ++i) {
                        auto& slice = _dict_items[_indexes[i]];
                        doris::memcpy_inlined(dst_ptr, slice.get_data(), _type_length);
                        dst_ptr += _type_length;
                    }
                    data_index = dst_ptr - raw_data;
                } else {
                    if (config::enable_parquet_simd_dict_decode) {
                        // P1-4: SIMD dict gather + P1-5: prefetch for lazy decode path
                        size_t local_dict_index = 0;
                        _simd_dict_gather(raw_data, data_index, local_dict_index, run_length);
                    } else if (_dict_exceeds_l2_cache && config::enable_parquet_dict_prefetch) {
                        // P1-5 only: scalar loop with software prefetch for large dicts
                        constexpr size_t PF_DIST = 8;
                        for (size_t i = 0; i < run_length; ++i) {
                            if (i + PF_DIST < run_length) {
                                PREFETCH(&_dict_items[_indexes[i + PF_DIST]]);
                            }
                            *(cppType*)(raw_data + data_index) = _dict_items[_indexes[i]];
                            data_index += _type_length;
                        }
                    } else {
                        for (size_t i = 0; i < run_length; ++i) {
                            *(cppType*)(raw_data + data_index) = _dict_items[_indexes[i]];
                            data_index += _type_length;
                        }
                    }
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length * _type_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                // Skip indexes in the RLE stream without decoding them.
                _index_batch_decoder->SkipBatch(cast_set<uint32_t>(run_length));
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // No indexes to skip for null values.
                break;
            }
            }
        }
        return Status::OK();
    }

    // P1-4: SIMD dict gather + P1-5: software prefetch for scalar types.
    // Uses AVX2 gather instructions for INT32/FLOAT (8 values/op) and INT64/DOUBLE (4 values/op).
    // Falls back to scalar loop with software prefetch for large dictionaries.
    ALWAYS_INLINE void _simd_dict_gather(char* raw_data, size_t& data_index, size_t& dict_index,
                                         size_t run_length) {
        constexpr size_t PREFETCH_DISTANCE = 8;
        const bool use_prefetch = _dict_exceeds_l2_cache && config::enable_parquet_dict_prefetch;

#ifdef __AVX2__
        if constexpr (PhysicalType == tparquet::Type::INT32 ||
                      PhysicalType == tparquet::Type::FLOAT) {
            // 4-byte types: gather 8 values per AVX2 instruction
            size_t i = 0;
            for (; i + 8 <= run_length; i += 8) {
                if (use_prefetch && i + PREFETCH_DISTANCE + 8 <= run_length) {
                    PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE]]);
                    PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE + 4]]);
                }
                __m256i indices = _mm256_loadu_si256(
                        reinterpret_cast<const __m256i*>(&_indexes[dict_index + i]));
                __m256i gathered = _mm256_i32gather_epi32(
                        reinterpret_cast<const int*>(_dict_items.data()), indices, 4);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(raw_data + data_index), gathered);
                data_index += 32; // 8 × 4 bytes
            }
            // Scalar tail
            for (; i < run_length; ++i) {
                if (use_prefetch && i + PREFETCH_DISTANCE < run_length) {
                    PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE]]);
                }
                *(cppType*)(raw_data + data_index) = _dict_items[_indexes[dict_index + i]];
                data_index += _type_length;
            }
            dict_index += run_length;
            return;
        }
        if constexpr (PhysicalType == tparquet::Type::INT64 ||
                      PhysicalType == tparquet::Type::DOUBLE) {
            // 8-byte types: gather 4 values per AVX2 instruction
            // _mm256_i32gather_epi64 takes a __m128i of 4 int32 indices
            size_t i = 0;
            for (; i + 4 <= run_length; i += 4) {
                if (use_prefetch && i + PREFETCH_DISTANCE + 4 <= run_length) {
                    PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE]]);
                    PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE + 2]]);
                }
                __m128i indices = _mm_loadu_si128(
                        reinterpret_cast<const __m128i*>(&_indexes[dict_index + i]));
                __m256i gathered = _mm256_i32gather_epi64(
                        reinterpret_cast<const long long*>(_dict_items.data()), indices, 8);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(raw_data + data_index), gathered);
                data_index += 32; // 4 × 8 bytes
            }
            // Scalar tail
            for (; i < run_length; ++i) {
                if (use_prefetch && i + PREFETCH_DISTANCE < run_length) {
                    PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE]]);
                }
                *(cppType*)(raw_data + data_index) = _dict_items[_indexes[dict_index + i]];
                data_index += _type_length;
            }
            dict_index += run_length;
            return;
        }
#endif
        // Scalar fallback with optional prefetch (also covers INT96 etc.)
        for (size_t i = 0; i < run_length; ++i) {
            if (use_prefetch && i + PREFETCH_DISTANCE < run_length) {
                PREFETCH(&_dict_items[_indexes[dict_index + i + PREFETCH_DISTANCE]]);
            }
            *(cppType*)(raw_data + data_index) = _dict_items[_indexes[dict_index + i]];
            data_index += _type_length;
        }
        dict_index += run_length;
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
        // P1-5: Check if dictionary exceeds L2 cache threshold for prefetch decisions.
        // Typical L2 cache: 256KB-1MB per core. Use 256KB as conservative threshold.
        constexpr size_t L2_CACHE_THRESHOLD = 256 * 1024;
        _dict_exceeds_l2_cache = (num_values * sizeof(cppType)) > L2_CACHE_THRESHOLD;
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
    // P1-5: Whether dictionary size exceeds L2 cache threshold (triggers software prefetching)
    bool _dict_exceeds_l2_cache = false;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
