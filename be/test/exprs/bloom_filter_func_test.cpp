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

#include "exprs/bloom_filter_func.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "exprs/block_bloom_filter.hpp"
#include "exprs/create_predicate_function.h"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"
#include "gtest/gtest.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/column_helper.h"
#include "util/url_coding.h"

namespace doris {
class BloomFilterFuncTest : public testing::Test {
protected:
    BloomFilterFuncTest() = default;
    ~BloomFilterFuncTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BloomFilterFuncTest, Init) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    try {
        bloom_filter_func.contain_null();
        ASSERT_TRUE(false) << "No exception thrown";
    } catch (...) {
    }

    const size_t runtime_length = 1024;
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);

    try {
        bloom_filter_func.contain_null();
        ASSERT_TRUE(false) << "No exception thrown";
    } catch (...) {
    }

    auto st = bloom_filter_func.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func2(true);
    params.null_aware = false;
    params.build_bf_by_runtime_size = true;

    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();
    ASSERT_EQ(bloom_filter_func2._bloom_filter_length, runtime_length);

    bloom_filter_func.light_copy(&bloom_filter_func2);
    bloom_filter_func2.light_copy(&bloom_filter_func);
}

TEST_F(BloomFilterFuncTest, TrackBlockBloomFilterMemory) {
    constexpr int initial_log_space_bytes = 22;
    constexpr int resized_log_space_bytes = 23;
    auto mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "BlockBloomFilterMemoryTest");
    auto switch_mem_tracker = SwitchThreadMemTrackerLimiter(mem_tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t initial_consumption = mem_tracker->consumption();

    BlockBloomFilter bloom_filter;
    ASSERT_TRUE(bloom_filter.init(initial_log_space_bytes, 0).ok());
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(mem_tracker->consumption(), initial_consumption + (1ULL << initial_log_space_bytes));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(bloom_filter.directory().data) % 32, 0);

    ASSERT_TRUE(bloom_filter.init(resized_log_space_bytes, 0).ok());
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(mem_tracker->consumption(), initial_consumption + (1ULL << resized_log_space_bytes));

    bloom_filter.close();
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(mem_tracker->consumption(), initial_consumption);
}

TEST_F(BloomFilterFuncTest, FixedLenToUInt32) {
    fixed_len_to_uint32_v2 fixed_lenv2;
    {
        DateV2Value<DateV2ValueType> date;
        {
            CastParameters p;
            CastToDateV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                    {"2021-01-01", strlen("2021-01-01")}, date, nullptr, p);
        }
        auto min = binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(MIN_DATE_V2);
        auto max = binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(MAX_DATE_V2);

        ASSERT_EQ(fixed_lenv2(date), (uint32_t)date.to_date_int_val());

        ASSERT_EQ(fixed_lenv2(min), (uint32_t)min.to_date_int_val());

        ASSERT_EQ(fixed_lenv2(max), (uint32_t)max.to_date_int_val());
    }
}

// -0.0 == +0.0 and every NaN compares equal, so from NORMALIZE_FLOAT_HASH_KEY_VERSION on a
// bloom filter built from one form must accept the other form; the legacy v2 convention is
// kept for older query versions so mixed-version producers and consumers still agree.
TEST_F(BloomFilterFuncTest, FixedLenToUInt32FloatSpecialValues) {
    fixed_len_to_uint32_v2 fixed_lenv2;
    fixed_len_to_uint32_v3 fixed_lenv3;
    const auto quiet_nan = std::numeric_limits<double>::quiet_NaN();
    const auto payload_nan = std::bit_cast<double>(std::bit_cast<uint64_t>(quiet_nan) | 0x1234ULL);
    ASSERT_NE(fixed_lenv2(-0.0), fixed_lenv2(0.0));
    ASSERT_EQ(fixed_lenv3(-0.0), fixed_lenv3(0.0));
    ASSERT_EQ(fixed_lenv3(payload_nan), fixed_lenv3(quiet_nan));
    ASSERT_EQ(fixed_lenv3(-quiet_nan), fixed_lenv3(quiet_nan));
    ASSERT_NE(fixed_lenv3(1.5), fixed_lenv3(-1.5));
    ASSERT_EQ(fixed_lenv3(1.5), fixed_lenv2(1.5));

    const auto quiet_nan_f = std::numeric_limits<float>::quiet_NaN();
    const auto payload_nan_f = std::bit_cast<float>(std::bit_cast<uint32_t>(quiet_nan_f) | 0x12U);
    ASSERT_EQ(fixed_lenv3(-0.0F), fixed_lenv3(0.0F));
    ASSERT_EQ(fixed_lenv3(payload_nan_f), fixed_lenv3(quiet_nan_f));
    ASSERT_NE(fixed_lenv3(1.5F), fixed_lenv3(2.5F));
    // Non-float types keep the v2 convention.
    ASSERT_EQ(fixed_lenv3(int32_t(42)), fixed_lenv2(int32_t(42)));
    ASSERT_EQ(fixed_lenv3(int64_t(-7)), fixed_lenv2(int64_t(-7)));
}

TEST_F(BloomFilterFuncTest, FindFixedLenDoubleSignedZero) {
    for (bool normalize_float_keys : {true, false}) {
        BloomFilterFunc<PrimitiveType::TYPE_DOUBLE> bloom_filter_func(true);
        RuntimeFilterParams params {1,
                                    RuntimeFilterType::BLOOM_FILTER,
                                    PrimitiveType::TYPE_DOUBLE,
                                    false,
                                    0,
                                    0,
                                    0,
                                    256,
                                    0,
                                    0,
                                    normalize_float_keys};
        bloom_filter_func.init_params(&params);
        auto st = bloom_filter_func.init_with_fixed_length(1024);
        ASSERT_TRUE(st.ok()) << st.to_string();

        const auto quiet_nan = std::numeric_limits<double>::quiet_NaN();
        const auto payload_nan =
                std::bit_cast<double>(std::bit_cast<uint64_t>(quiet_nan) | 0x1234ULL);
        // Build with +0.0 / quiet NaN, probe with -0.0 / payload NaN.
        bloom_filter_func.insert_fixed_len(
                ColumnHelper::create_column<DataTypeFloat64>({0.0, 1.0, quiet_nan}), 0);

        auto probe_column =
                ColumnHelper::create_column<DataTypeFloat64>({-0.0, 1.0, payload_nan, 0.0});
        PODArray<uint16_t> offsets(4);
        std::iota(offsets.begin(), offsets.end(), 0);
        auto find_count = bloom_filter_func.find_fixed_len_olap_engine(*probe_column, nullptr,
                                                                       offsets.data(), 4, false);
        if (normalize_float_keys) {
            ASSERT_EQ(find_count, 4);
            ASSERT_EQ(offsets[0], 0);
            ASSERT_EQ(offsets[2], 2);
        } else {
            // Legacy convention: only the bit-identical values are guaranteed to pass.
            ASSERT_GE(find_count, 2);
        }
        ASSERT_TRUE(bloom_filter_func.test_field(Field::create_field<TYPE_DOUBLE>(1.0)));
        if (normalize_float_keys) {
            ASSERT_TRUE(bloom_filter_func.test_field(Field::create_field<TYPE_DOUBLE>(-0.0)));
            ASSERT_TRUE(
                    bloom_filter_func.test_field(Field::create_field<TYPE_DOUBLE>(payload_nan)));
            std::vector<uint8_t> matches(4, 1);
            st = bloom_filter_func.find_batch_raw_fixed(
                    reinterpret_cast<const uint8_t*>(
                            assert_cast<const ColumnFloat64&>(*probe_column).get_data().data()),
                    4, sizeof(double), matches.data());
            ASSERT_TRUE(st.ok()) << st.to_string();
            ASSERT_EQ(std::vector<uint8_t>({1, 1, 1, 1}), matches);
        }
    }
}

// IN_OR_BLOOM filters are converted from a HybridSet whose raw values keep their sign and
// payload; the conversion must hash them with the same canonical convention.
TEST_F(BloomFilterFuncTest, InsertSetKeepsFloatConvention) {
    BloomFilterFunc<PrimitiveType::TYPE_FLOAT> bloom_filter_func(false);
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_FLOAT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                true};
    bloom_filter_func.init_params(&params);
    ASSERT_TRUE(bloom_filter_func.init_with_fixed_length(1024).ok());

    const auto quiet_nan = std::numeric_limits<float>::quiet_NaN();
    const auto payload_nan = std::bit_cast<float>(std::bit_cast<uint32_t>(quiet_nan) | 0x12U);
    auto set = std::make_shared<HybridSet<PrimitiveType::TYPE_FLOAT>>(false);
    for (float value : {-0.0F, 1.5F, payload_nan}) {
        set->insert(&value);
    }
    bloom_filter_func.insert_set(set);

    auto probe_column = ColumnHelper::create_column<DataTypeFloat32>({0.0F, 1.5F, quiet_nan});
    std::vector<uint8_t> results(3, 0);
    bloom_filter_func.find_fixed_len(probe_column, results.data());
    ASSERT_EQ(std::vector<uint8_t>({1, 1, 1}), results);
}

TEST_F(BloomFilterFuncTest, FindFixedLenFloatSpecialValues) {
    BloomFilterFunc<PrimitiveType::TYPE_FLOAT> bloom_filter_func(true);
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_FLOAT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                true};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(1024);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto quiet_nan = std::numeric_limits<float>::quiet_NaN();
    const auto payload_nan = std::bit_cast<float>(std::bit_cast<uint32_t>(quiet_nan) | 0x12U);
    bloom_filter_func.insert_fixed_len(
            ColumnHelper::create_column<DataTypeFloat32>({0.0F, 1.5F, quiet_nan}), 0);

    auto probe_column =
            ColumnHelper::create_column<DataTypeFloat32>({-0.0F, 1.5F, payload_nan, -quiet_nan});
    PODArray<uint16_t> offsets(4);
    std::iota(offsets.begin(), offsets.end(), 0);
    auto find_count = bloom_filter_func.find_fixed_len_olap_engine(*probe_column, nullptr,
                                                                   offsets.data(), 4, false);
    ASSERT_EQ(find_count, 4);

    std::vector<uint8_t> results(4, 0);
    bloom_filter_func.find_fixed_len(probe_column, results.data());
    ASSERT_EQ(std::vector<uint8_t>({1, 1, 1, 1}), results);

    std::vector<uint8_t> matches(4, 1);
    st = bloom_filter_func.find_batch_raw_fixed(
            reinterpret_cast<const uint8_t*>(
                    assert_cast<const ColumnFloat32&>(*probe_column).get_data().data()),
            4, sizeof(float), matches.data());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(std::vector<uint8_t>({1, 1, 1, 1}), matches);
    ASSERT_TRUE(bloom_filter_func.test_field(Field::create_field<TYPE_FLOAT>(-0.0F)));
    ASSERT_TRUE(bloom_filter_func.test_field(Field::create_field<TYPE_FLOAT>(payload_nan)));
}

// The storage-layer predicate is built from a light copy of the consumer's filter
// (create_bloom_filter_predicate), so the copy must probe with the producer's convention.
TEST_F(BloomFilterFuncTest, LightCopyKeepsFloatConvention) {
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_FLOAT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                true};
    auto producer = std::make_shared<BloomFilterFunc<PrimitiveType::TYPE_FLOAT>>(false);
    producer->init_params(&params);
    ASSERT_TRUE(producer->init_with_fixed_length(1024).ok());
    producer->insert_fixed_len(ColumnHelper::create_column<DataTypeFloat32>({0.0F, 1.5F}), 0);

    BloomFilterFunc<PrimitiveType::TYPE_FLOAT> storage_copy(false);
    storage_copy.light_copy(producer.get());

    auto probe_column = ColumnHelper::create_column<DataTypeFloat32>({-0.0F, 1.5F});
    PODArray<uint16_t> offsets(2);
    std::iota(offsets.begin(), offsets.end(), 0);
    ASSERT_EQ(storage_copy.find_fixed_len_olap_engine(*probe_column, nullptr, offsets.data(), 2,
                                                      false),
              2);
    ASSERT_TRUE(storage_copy.test_field(Field::create_field<TYPE_FLOAT>(-0.0F)));
}

TEST_F(BloomFilterFuncTest, InsertSet) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(bloom_filter_func._null_aware);
    int32_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    bloom_filter_func.insert_set(set);
    auto column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});

    PODArray<uint8_t> result(column->size());
    bloom_filter_func.find_fixed_len(column, result.data());
    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }

    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func2(false);
    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    bloom_filter_func2.insert_set(set);

    bloom_filter_func2.find_fixed_len(column, result.data());
    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }
}

TEST_F(BloomFilterFuncTest, InsertFixedLen) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(true);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});
    auto nullmap_column = ColumnUInt8::create(4, 0);
    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    auto nullable_column = ColumnNullable::create(std::move(column), std::move(nullmap_column));
    ASSERT_TRUE(nullable_column->has_null());
    bloom_filter_func.insert_fixed_len(std::move(nullable_column), 0);
    bloom_filter_func.set_contain_null(true);
    ASSERT_TRUE(bloom_filter_func.contain_null());

    BloomFilterFunc<PrimitiveType::TYPE_STRING> bloom_filter_func2(true);
    params.column_return_type = PrimitiveType::TYPE_STRING;
    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto column_string = ColumnHelper::create_column<DataTypeString>({"aa", "bb", "cc", "dd"});
    nullmap_column = ColumnUInt8::create(4, 0);
    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    nullable_column = ColumnNullable::create(column_string->clone(), nullmap_column->clone());
    ASSERT_TRUE(nullable_column->has_null());

    bloom_filter_func2.insert_fixed_len(std::move(nullable_column), 0);

    ASSERT_TRUE(bloom_filter_func2.contain_null());

    PODArray<uint16_t> offsets(4);
    std::iota(offsets.begin(), offsets.end(), 0);

    auto probe_column = ColumnHelper::create_column<DataTypeString>({"aa", "bb", "cc", "dd"});

    auto find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            *probe_column, nullmap_column->get_data().data(), offsets.data(), 4, false);

    ASSERT_EQ(find_count, 4);

    nullmap_column->get_data()[1] = 0;
    nullmap_column->get_data()[3] = 0;
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            *probe_column, nullmap_column->get_data().data(), offsets.data(), 4, false);

    ASSERT_EQ(find_count, 2);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 2);
}

TEST_F(BloomFilterFuncTest, RawFixedCapabilitiesCoverEveryFixedRuntimeFilterType) {
#define EXPECT_RAW_FIXED(TYPE)                                                   \
    do {                                                                         \
        BloomFilterFunc<TYPE> filter(false);                                     \
        EXPECT_TRUE(filter.supports_raw_fixed_values()) << type_to_string(TYPE); \
        EXPECT_EQ(filter.raw_fixed_value_size(),                                 \
                  sizeof(typename PrimitiveTypeTraits<TYPE>::CppType))           \
                << type_to_string(TYPE);                                         \
    } while (false)
    EXPECT_RAW_FIXED(TYPE_BOOLEAN);
    EXPECT_RAW_FIXED(TYPE_TINYINT);
    EXPECT_RAW_FIXED(TYPE_SMALLINT);
    EXPECT_RAW_FIXED(TYPE_INT);
    EXPECT_RAW_FIXED(TYPE_BIGINT);
    EXPECT_RAW_FIXED(TYPE_LARGEINT);
    EXPECT_RAW_FIXED(TYPE_FLOAT);
    EXPECT_RAW_FIXED(TYPE_DOUBLE);
    EXPECT_RAW_FIXED(TYPE_DATE);
    EXPECT_RAW_FIXED(TYPE_DATETIME);
    EXPECT_RAW_FIXED(TYPE_DATEV2);
    EXPECT_RAW_FIXED(TYPE_DATETIMEV2);
    EXPECT_RAW_FIXED(TYPE_TIMESTAMPTZ);
    EXPECT_RAW_FIXED(TYPE_DECIMAL32);
    EXPECT_RAW_FIXED(TYPE_DECIMAL64);
    EXPECT_RAW_FIXED(TYPE_DECIMALV2);
    EXPECT_RAW_FIXED(TYPE_DECIMAL128I);
    EXPECT_RAW_FIXED(TYPE_DECIMAL256);
    EXPECT_RAW_FIXED(TYPE_IPV4);
    EXPECT_RAW_FIXED(TYPE_IPV6);
#undef EXPECT_RAW_FIXED

    BloomFilterFunc<TYPE_STRING> string_filter(false);
    EXPECT_FALSE(string_filter.supports_raw_fixed_values());
    EXPECT_EQ(string_filter.raw_fixed_value_size(), 0);
}

TEST_F(BloomFilterFuncTest, RawFixedProbeUsesTheSameHashAsColumnProbe) {
    BloomFilterFunc<TYPE_INT> filter(false);
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    filter.init_params(&params);
    ASSERT_TRUE(filter.init_with_fixed_length(1024).ok());
    auto build_column = ColumnHelper::create_column<DataTypeInt32>({2, 4});
    filter.insert_fixed_len(build_column, 0);

    const std::array<int32_t, 4> values {1, 2, 3, 4};
    std::array<uint8_t, 4> raw_matches {1, 1, 1, 1};
    ASSERT_TRUE(filter.find_batch_raw_fixed(reinterpret_cast<const uint8_t*>(values.data()),
                                            values.size(), sizeof(int32_t), raw_matches.data())
                        .ok());

    auto probe_column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});
    std::array<uint8_t, 4> column_matches {};
    filter.find_fixed_len(probe_column, column_matches.data());
    EXPECT_EQ(column_matches, raw_matches);
}

TEST_F(BloomFilterFuncTest, RawFixedProbeMatchesBuildHashForEveryFixedRuntimeFilterType) {
    const auto expect_match = []<PrimitiveType TYPE>() {
        BloomFilterFunc<TYPE> filter(false);
        RuntimeFilterParams params;
        params.filter_type = RuntimeFilterType::BLOOM_FILTER;
        params.column_return_type = TYPE;
        params.bloom_filter_size = 1024;
        filter.init_params(&params);
        ASSERT_TRUE(filter.init_with_fixed_length(1024).ok()) << type_to_string(TYPE);

        using ValueType = typename PrimitiveTypeTraits<TYPE>::CppType;
        ValueType value {};
        auto set = std::make_shared<HybridSet<TYPE>>(false);
        set->insert(&value);
        filter.insert_set(set);

        uint8_t match = 1;
        ASSERT_TRUE(filter.find_batch_raw_fixed(reinterpret_cast<const uint8_t*>(&value), 1,
                                                sizeof(ValueType), &match)
                            .ok())
                << type_to_string(TYPE);
        EXPECT_EQ(match, 1) << type_to_string(TYPE);
        EXPECT_TRUE(filter.test_field(Field::create_field<TYPE>(value))) << type_to_string(TYPE);
    };

#define EXPECT_RAW_FIXED_MATCH(TYPE) expect_match.template operator()<TYPE>()
    EXPECT_RAW_FIXED_MATCH(TYPE_BOOLEAN);
    EXPECT_RAW_FIXED_MATCH(TYPE_TINYINT);
    EXPECT_RAW_FIXED_MATCH(TYPE_SMALLINT);
    EXPECT_RAW_FIXED_MATCH(TYPE_INT);
    EXPECT_RAW_FIXED_MATCH(TYPE_BIGINT);
    EXPECT_RAW_FIXED_MATCH(TYPE_LARGEINT);
    EXPECT_RAW_FIXED_MATCH(TYPE_FLOAT);
    EXPECT_RAW_FIXED_MATCH(TYPE_DOUBLE);
    EXPECT_RAW_FIXED_MATCH(TYPE_DATE);
    EXPECT_RAW_FIXED_MATCH(TYPE_DATETIME);
    EXPECT_RAW_FIXED_MATCH(TYPE_DATEV2);
    EXPECT_RAW_FIXED_MATCH(TYPE_DATETIMEV2);
    EXPECT_RAW_FIXED_MATCH(TYPE_TIMESTAMPTZ);
    EXPECT_RAW_FIXED_MATCH(TYPE_DECIMAL32);
    EXPECT_RAW_FIXED_MATCH(TYPE_DECIMAL64);
    EXPECT_RAW_FIXED_MATCH(TYPE_DECIMALV2);
    EXPECT_RAW_FIXED_MATCH(TYPE_DECIMAL128I);
    EXPECT_RAW_FIXED_MATCH(TYPE_DECIMAL256);
    EXPECT_RAW_FIXED_MATCH(TYPE_IPV4);
    EXPECT_RAW_FIXED_MATCH(TYPE_IPV6);
#undef EXPECT_RAW_FIXED_MATCH
}

TEST_F(BloomFilterFuncTest, DictionaryFieldProbeSupportsEveryStringRuntimeFilterType) {
    const auto expect_match = []<PrimitiveType TYPE>() {
        BloomFilterFunc<TYPE> filter(false);
        RuntimeFilterParams params;
        params.filter_type = RuntimeFilterType::BLOOM_FILTER;
        params.column_return_type = TYPE;
        params.bloom_filter_size = 1024;
        filter.init_params(&params);
        ASSERT_TRUE(filter.init_with_fixed_length(1024).ok()) << type_to_string(TYPE);

        auto column = ColumnString::create();
        column->insert_data("value", 5);
        filter.insert_fixed_len(std::move(column), 0);
        EXPECT_TRUE(filter.test_field(Field::create_field<TYPE_STRING>(std::string("value"))))
                << type_to_string(TYPE);
    };

    expect_match.template operator()<TYPE_CHAR>();
    expect_match.template operator()<TYPE_VARCHAR>();
    expect_match.template operator()<TYPE_STRING>();
}

TEST_F(BloomFilterFuncTest, Merge) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(bloom_filter_func._null_aware);
    int32_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    bloom_filter_func.insert_set(set);

    auto set2 = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(bloom_filter_func._null_aware);

    a = 7;
    set2->insert(&a);
    a = 8;
    set2->insert(&a);

    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func2(false);

    st = bloom_filter_func.merge(nullptr);
    ASSERT_FALSE(st);

    st = bloom_filter_func.merge(&bloom_filter_func2);
    ASSERT_FALSE(st);

    // `bloom_filter_func2` is not initialized, merge should fail
    st = bloom_filter_func2.merge(&bloom_filter_func);
    ASSERT_FALSE(st);

    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();
    bloom_filter_func2.insert_set(set2);

    st = bloom_filter_func2.merge(&bloom_filter_func);
    ASSERT_TRUE(st.ok()) << "Failed to merge bloom filter: " << st.to_string();

    st = bloom_filter_func.merge(&bloom_filter_func2);
    ASSERT_TRUE(st.ok()) << "Failed to merge bloom filter: " << st.to_string();

    auto column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 7, 8});
    PODArray<uint8_t> result(column->size());
    bloom_filter_func2.find_fixed_len(column, result.data());

    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }

    bloom_filter_func.find_fixed_len(column, result.data());
    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }

    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func3(false);
    params.bloom_filter_size = 512;

    bloom_filter_func3.init_params(&params);
    st = bloom_filter_func3.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    st = bloom_filter_func3.merge(&bloom_filter_func);
    ASSERT_FALSE(st);
}

/// The purpose of this case is to detect changes after modifying the Bloom filter algorithm to prevent compatibility issues.
TEST_F(BloomFilterFuncTest, HashAlgorithm) {
    std::string BloomFilterBinary =
            "AAAAQAAAAIAACAAAAABAACAAAAAAQAAAAQAAAACAAAAAACAEAACAAgAACAQEAAAgAASAAAABAgAAEAAIDAAAAA"
            "CQAQAAYgAAAAIBAiBAgAABAgBABAAICAAABCGAAIABBAAAAAAAIABAAAAACAAAAAAAABAAQAAAAAAAIBAAAAAA"
            "AQAiABEAAQBAIgAAgBAQEEAAACACAQAABEgAAggAAQAAAUAQAAEQECCAAABAAIgHAAAACAEAAgAJQABAIAEAAA"
            "gAAEAAAAAAEAAAAAAQAAABAAAQAAAAAEAAAAAEAACAEAAAAAUAAAAAIBAgCAAAQAAIAAAACBAIABAAAAAABg="
            "=";
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);

    ASSERT_TRUE(bloom_filter_func.init_with_fixed_length(runtime_length));

    auto column = ColumnHelper::create_column<DataTypeInt32>(
            {1, 3, 5, 7, 9, 12, 14, 16, 2001, 2002, 2003, 4096, 4097, 4098, 4099, 4100});

    bloom_filter_func.insert_fixed_len(column, 0);

    char* data = nullptr;
    int size;
    bloom_filter_func.get_data(&data, &size);

    std::string encode_string;
    base64_encode(std::string(data, size), &encode_string);
    ASSERT_EQ(strlen(BloomFilterBinary.c_str()), strlen(encode_string.c_str()));
    ASSERT_EQ(memcmp(BloomFilterBinary.data(), encode_string.data(),
                     strlen(BloomFilterBinary.c_str())),
              0);

    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func2(false);
    bloom_filter_func2.init_params(&params);
    ASSERT_TRUE(bloom_filter_func2.init_with_fixed_length(runtime_length));

    bloom_filter_func2.insert_fixed_len(column, 0);
    bloom_filter_func.get_data(&data, &size);
    base64_encode(std::string(data, size), &encode_string);

    ASSERT_EQ(strlen(BloomFilterBinary.c_str()), strlen(encode_string.c_str()));
    ASSERT_EQ(memcmp(BloomFilterBinary.data(), encode_string.data(),
                     strlen(BloomFilterBinary.c_str())),
              0);
}

TEST_F(BloomFilterFuncTest, MergeLargeData) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(bloom_filter_func._null_aware);

    const int32_t count = 1024 * 1024;
    std::vector<int32_t> data1(count);
    for (int32_t i = 0; i != count; ++i) {
        set->insert(&i);
        data1[i] = i;
    }

    bloom_filter_func.insert_set(set);

    auto set2 = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(bloom_filter_func._null_aware);

    const int32_t count2 = 1024 * 512;
    std::vector<int32_t> data2(count2);
    for (int32_t i = 0; i != 1024 * 512; ++i) {
        auto a = i % 10 * i + i;
        set2->insert(&a);
        data2[i] = a;
    }

    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func2(false);

    st = bloom_filter_func.merge(nullptr);
    ASSERT_FALSE(st);

    st = bloom_filter_func.merge(&bloom_filter_func2);
    ASSERT_FALSE(st);

    // `bloom_filter_func2` is not initialized, merge should fail
    st = bloom_filter_func2.merge(&bloom_filter_func);
    ASSERT_FALSE(st);

    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();
    bloom_filter_func2.insert_set(set2);

    st = bloom_filter_func2.merge(&bloom_filter_func);
    ASSERT_TRUE(st.ok()) << "Failed to merge bloom filter: " << st.to_string();

    st = bloom_filter_func.merge(&bloom_filter_func2);
    ASSERT_TRUE(st.ok()) << "Failed to merge bloom filter: " << st.to_string();

    auto column = ColumnHelper::create_column<DataTypeInt32>(data1);
    auto column2 = ColumnHelper::create_column<DataTypeInt32>(data2);
    PODArray<uint8_t> result(column->size());
    bloom_filter_func2.find_fixed_len(column, result.data());

    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }

    bloom_filter_func.find_fixed_len(column, result.data());
    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }

    result.resize(column2->size());
    bloom_filter_func2.find_fixed_len(column2, result.data());

    for (size_t i = 0; i < column2->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }

    bloom_filter_func.find_fixed_len(column2, result.data());
    for (size_t i = 0; i < column2->size(); ++i) {
        ASSERT_TRUE(result[i]);
    }
}

TEST_F(BloomFilterFuncTest, FindDictOlapEngine) {
    const size_t count = 4096;

    std::vector<StringRef> dicts = {StringRef("aa"),  StringRef("bb"),  StringRef("cc"),
                                    StringRef("dd"),  StringRef("aab"), StringRef("bbc"),
                                    StringRef("ccd"), StringRef("dde")};
    auto column = ColumnDictI32::create();
    column->reserve(count);
    std::vector<int32_t> data(count);
    for (size_t i = 0; i != count; ++i) {
        data[i] = i % dicts.size();
    }

    column->insert_many_dict_data(data.data(), 0, dicts.data(), count, dicts.size());
    column->initialize_hash_values_for_runtime_filter();

    BloomFilterFunc<PrimitiveType::TYPE_STRING> bloom_filter_func(false);
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(0);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto string_column = ColumnString::create();
    for (auto& dict : dicts) {
        string_column->insert_data(dict.data, dict.size);
    }

    bloom_filter_func.insert_fixed_len(std::move(string_column), 0);

    PODArray<uint16_t> offsets(count);
    std::iota(offsets.begin(), offsets.end(), 0);

    auto find_count = bloom_filter_func.find_dict_olap_engine<false>(column.get(), nullptr,
                                                                     offsets.data(), count);
    ASSERT_EQ(find_count, count);

    PODArray<uint8_t> nullmap;
    uint8_t flag = 0;
    nullmap.assign(count, flag);
    find_count = bloom_filter_func.find_dict_olap_engine<true>(column.get(), nullmap.data(),
                                                               offsets.data(), count);
    ASSERT_EQ(find_count, count);
}

TEST_F(BloomFilterFuncTest, FindFixedLenOlapEngine) {
    const size_t count = 4096;

    BloomFilterFunc<PrimitiveType::TYPE_DECIMAL256> bloom_filter_func(true);
    RuntimeFilterParams params {
            1, RuntimeFilterType::BLOOM_FILTER, PrimitiveType::TYPE_INT, false, 0, 0, 0, 256, 0, 0};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(0);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto decimal_column = ColumnDecimal256::create(0, 8);
    auto decimal_column2 = ColumnDecimal256::create(0, 8);
    decimal_column->reserve(count);
    decimal_column2->reserve(count);
    for (size_t i = 0; i != count; ++i) {
        Decimal256 value = Decimal256::from_int_frac(wide::Int256(i), 4, 8);
        decimal_column->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        decimal_column2->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    bloom_filter_func.insert_fixed_len(std::move(decimal_column), 0);

    PODArray<uint16_t> offsets(count);
    std::iota(offsets.begin(), offsets.end(), 0);

    PODArray<uint8_t> nullmap;
    uint8_t flag = 0;
    nullmap.assign(count, flag);
    auto find_count = bloom_filter_func.find_fixed_len_olap_engine(*decimal_column2, nullmap.data(),
                                                                   offsets.data(), count, true);
    ASSERT_EQ(find_count, count);

    BloomFilterFunc<PrimitiveType::TYPE_CHAR> bloom_filter_func2(true);
    params.column_return_type = PrimitiveType::TYPE_STRING;
    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(0);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto string_column = ColumnHelper::create_column<DataTypeString>({"aa", "bb", "cc", "dd"});

    bloom_filter_func2.insert_fixed_len(string_column->clone(), 0);

    // CHAR padding is stripped at the page decoder now, so the runtime BF
    // probe sees natural-length StringRefs; no trailing '\0' bytes here.
    auto probe_column = ColumnHelper::create_column<DataTypeString>({"aa", "bb", "cc", "dd", "ef"});

    PODArray<uint16_t> offsets2(5);
    std::iota(offsets2.begin(), offsets2.end(), 0);

    find_count = bloom_filter_func2.find_fixed_len_olap_engine(*probe_column, nullmap.data(),
                                                               offsets2.data(), 5, false);
    ASSERT_EQ(find_count, 4);

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(*probe_column, nullmap.data(),
                                                               offsets2.data(), 5, true);
    ASSERT_EQ(find_count, 4);

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(*probe_column, nullptr,
                                                               offsets2.data(), 5, false);
    ASSERT_EQ(find_count, 4);

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(*probe_column, nullptr,
                                                               offsets2.data(), 5, true);
    ASSERT_EQ(find_count, 4);

    PODArray<uint8_t> nullmap2;
    nullmap2.assign(size_t(5), flag);
    nullmap2[1] = 1;
    nullmap2[2] = 1;

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(*probe_column, nullmap2.data(),
                                                               offsets2.data(), 5, false);
    ASSERT_EQ(find_count, 2);
    ASSERT_EQ(offsets2[0], 0);
    ASSERT_EQ(offsets2[1], 3);
}

} // namespace doris
