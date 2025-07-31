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

#include <cstdint>
#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/create_predicate_function.h"
#include "gtest/gtest.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "util/url_coding.h"
#include "vec/columns/column_decimal.h"
#include "vec/runtime/vdatetime_value.h"

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
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
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

TEST_F(BloomFilterFuncTest, FixedLenToUInt32) {
    fixed_len_to_uint32_v2 fixed_lenv2;
    {
        DateV2Value<DateV2ValueType> date;
        date.from_date_str("2021-01-01", strlen("2021-01-01"));
        auto min = binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(MIN_DATE_V2);
        auto max = binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(MAX_DATE_V2);

        ASSERT_EQ(fixed_lenv2(date), (uint32_t)date.to_date_int_val());

        ASSERT_EQ(fixed_lenv2(min), (uint32_t)min.to_date_int_val());

        ASSERT_EQ(fixed_lenv2(max), (uint32_t)max.to_date_int_val());
    }
}

TEST_F(BloomFilterFuncTest, InsertSet) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
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
    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>({1, 2, 3, 4});

    vectorized::PODArray<uint8_t> result(column->size());
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
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>({1, 2, 3, 4});
    auto nullmap_column = vectorized::ColumnUInt8::create(4, 0);
    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    auto nullable_column =
            vectorized::ColumnNullable::create(std::move(column), std::move(nullmap_column));
    ASSERT_TRUE(nullable_column->has_null());
    bloom_filter_func.insert_fixed_len(std::move(nullable_column), 0);
    bloom_filter_func.set_contain_null(true);
    ASSERT_TRUE(bloom_filter_func.contain_null());

    BloomFilterFunc<PrimitiveType::TYPE_STRING> bloom_filter_func2(true);
    params.column_return_type = PrimitiveType::TYPE_STRING;
    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(runtime_length);
    ASSERT_TRUE(st.ok()) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto column_string = vectorized::ColumnHelper::create_column<vectorized::DataTypeString>(
            {"aa", "bb", "cc", "dd"});
    nullmap_column = vectorized::ColumnUInt8::create(4, 0);
    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    nullable_column =
            vectorized::ColumnNullable::create(column_string->clone(), nullmap_column->clone());
    ASSERT_TRUE(nullable_column->has_null());

    bloom_filter_func2.insert_fixed_len(std::move(nullable_column), 0);

    ASSERT_TRUE(bloom_filter_func2.contain_null());

    vectorized::PODArray<uint16_t> offsets(4);
    std::iota(offsets.begin(), offsets.end(), 0);

    std::vector<StringRef> strings(4);
    strings[0] = StringRef("aa");
    strings[1] = StringRef("bb");
    strings[2] = StringRef("cc");
    strings[3] = StringRef("dd");

    auto find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(strings.data()), nullmap_column->get_data().data(),
            offsets.data(), 4, false);

    ASSERT_EQ(find_count, 4);

    nullmap_column->get_data()[1] = 0;
    nullmap_column->get_data()[3] = 0;
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(strings.data()), nullmap_column->get_data().data(),
            offsets.data(), 4, false);

    ASSERT_EQ(find_count, 2);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 2);
}

TEST_F(BloomFilterFuncTest, Merge) {
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
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

    auto column =
            vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>({1, 2, 3, 4, 7, 8});
    vectorized::PODArray<uint8_t> result(column->size());
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
            "gAAEAAAAAAEAAAAAAQAAABAAAQAAAAAEAAAAAEAACAEAAAAAUAAAAAIBAgCAAAQAAIAAAACBAIABAAAAAABg";
    BloomFilterFunc<PrimitiveType::TYPE_INT> bloom_filter_func(false);
    const size_t runtime_length = 1024;
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
    bloom_filter_func.init_params(&params);

    ASSERT_TRUE(bloom_filter_func.init_with_fixed_length(runtime_length));

    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>(
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
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
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

    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>(data1);
    auto column2 = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>(data2);
    vectorized::PODArray<uint8_t> result(column->size());
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
    auto column = vectorized::ColumnDictI32::create();
    column->reserve(count);
    std::vector<int32_t> data(count);
    for (size_t i = 0; i != count; ++i) {
        data[i] = i % dicts.size();
    }

    column->insert_many_dict_data(data.data(), 0, dicts.data(), count, dicts.size());
    column->initialize_hash_values_for_runtime_filter();

    BloomFilterFunc<PrimitiveType::TYPE_STRING> bloom_filter_func(false);
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(0);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto string_column = vectorized::ColumnString::create();
    for (auto& dict : dicts) {
        string_column->insert_data(dict.data, dict.size);
    }

    bloom_filter_func.insert_fixed_len(std::move(string_column), 0);

    vectorized::PODArray<uint16_t> offsets(count);
    std::iota(offsets.begin(), offsets.end(), 0);

    auto find_count = bloom_filter_func.find_dict_olap_engine<false>(column.get(), nullptr,
                                                                     offsets.data(), count);
    ASSERT_EQ(find_count, count);

    vectorized::PODArray<uint8_t> nullmap;
    uint8_t flag = 0;
    nullmap.assign(count, flag);
    find_count = bloom_filter_func.find_dict_olap_engine<true>(column.get(), nullmap.data(),
                                                               offsets.data(), count);
    ASSERT_EQ(find_count, count);
}

TEST_F(BloomFilterFuncTest, FindFixedLenOlapEngine) {
    const size_t count = 4096;

    BloomFilterFunc<PrimitiveType::TYPE_DECIMAL256> bloom_filter_func(true);
    RuntimeFilterParams params {1,
                                RuntimeFilterType::BLOOM_FILTER,
                                PrimitiveType::TYPE_INT,
                                false,
                                0,
                                0,
                                0,
                                256,
                                0,
                                0,
                                false};
    bloom_filter_func.init_params(&params);
    auto st = bloom_filter_func.init_with_fixed_length(0);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto decimal_column = vectorized::ColumnDecimal256::create(0, 8);
    auto decimal_column2 = vectorized::ColumnDecimal256::create(0, 8);
    decimal_column->reserve(count);
    decimal_column2->reserve(count);
    for (size_t i = 0; i != count; ++i) {
        vectorized::Decimal256 value = vectorized::Decimal256::from_int_frac(wide::Int256(i), 4, 8);
        decimal_column->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        decimal_column2->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    bloom_filter_func.insert_fixed_len(std::move(decimal_column), 0);

    vectorized::PODArray<uint16_t> offsets(count);
    std::iota(offsets.begin(), offsets.end(), 0);

    vectorized::PODArray<uint8_t> nullmap;
    uint8_t flag = 0;
    nullmap.assign(count, flag);
    auto find_count = bloom_filter_func.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(decimal_column2->get_data().data()), nullmap.data(),
            offsets.data(), count, true);
    ASSERT_EQ(find_count, count);

    BloomFilterFunc<PrimitiveType::TYPE_CHAR> bloom_filter_func2(true);
    params.column_return_type = PrimitiveType::TYPE_STRING;
    bloom_filter_func2.init_params(&params);
    st = bloom_filter_func2.init_with_fixed_length(0);
    ASSERT_TRUE(st) << "Failed to init bloom filter with fixed length: " << st.to_string();

    auto string_column = vectorized::ColumnHelper::create_column<vectorized::DataTypeString>(
            {"aa", "bb", "cc", "dd"});

    bloom_filter_func2.insert_fixed_len(string_column->clone(), 0);

    StringRef strings[] = {StringRef("aa"), StringRef("bb"), StringRef("cc"),
                           StringRef("dd\0\0", 4), StringRef("ef\0\0", 4)};

    vectorized::PODArray<uint16_t> offsets2(5);
    std::iota(offsets2.begin(), offsets2.end(), 0);

    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(&strings[0]), nullmap.data(), offsets2.data(), 5, false);
    ASSERT_EQ(find_count, 4);

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(&strings[0]), nullmap.data(), offsets2.data(), 5, true);
    ASSERT_EQ(find_count, 4);

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(&strings[0]), nullptr, offsets2.data(), 5, false);
    ASSERT_EQ(find_count, 4);

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(&strings[0]), nullptr, offsets2.data(), 5, true);
    ASSERT_EQ(find_count, 4);

    vectorized::PODArray<uint8_t> nullmap2;
    nullmap2.assign(size_t(5), flag);
    nullmap2[1] = 1;
    nullmap2[2] = 1;

    std::iota(offsets2.begin(), offsets2.end(), 0);
    find_count = bloom_filter_func2.find_fixed_len_olap_engine(
            reinterpret_cast<const char*>(&strings[0]), nullmap2.data(), offsets2.data(), 5, false);
    ASSERT_EQ(find_count, 2);
    ASSERT_EQ(offsets2[0], 0);
    ASSERT_EQ(offsets2[1], 3);
}

} // namespace doris