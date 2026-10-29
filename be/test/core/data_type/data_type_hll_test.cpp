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

#include "core/data_type/data_type_hll.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstring>
#include <iostream>
#include <limits>
#include <tuple>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/common_data_type_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "core/types.h"
#include "util/unaligned.h"

// this test is gonna to be a data type test template for all DataType which should make ut test to coverage the function defined
// for example DataTypeHLL should test this function:
// 1. datatype meta info:
//         get_type_id, get_type_as_type_descriptor, get_storage_field_type, have_subtypes, get_pdata_type (const IDataType *data_type), to_pb_column_meta (PColumnMeta *col_meta)
//         get_family_name, get_is_parametric,
//         have_maximum_size_of_value, get_maximum_size_of_value_in_memory, get_size_of_value_in_memory
//         get_precision, get_scale
//         is_null_literal
// 2. datatype creation with column : create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size), get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to), from_string (ReadBuffer &rb, IColumn *column)
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)

namespace doris {

class DataTypeHLLTest : public ::testing::TestWithParam<int> {
protected:
    void SetUp() override {
        rows_value = GetParam();
        helper = std::make_unique<CommonDataTypeTest>();
    }

public:
    std::unique_ptr<CommonDataTypeTest> helper;
    int rows_value;
    DataTypePtr dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
};

TEST_P(DataTypeHLLTest, MetaInfoTest) {
    auto hll_type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_HLL, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_HLL);
    CommonDataTypeTest::DataTypeMetaInfo hll_meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_HLL,
            .type_as_type_descriptor = hll_type_descriptor,
            .family_name = "HLL",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_HLL,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = size_t(-1),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .pColumnMeta = col_meta.get(),
            .default_field = Field::create_field<TYPE_HLL>(HyperLogLog::empty()),
    };
    helper->meta_info_assert(dt_hll, hll_meta_info_to_assert);
}

TEST_P(DataTypeHLLTest, CreateColumnTest) {
    Field default_field_hll = Field::create_field<TYPE_HLL>(HyperLogLog::empty());
    helper->create_column_assert(dt_hll, default_field_hll, 17);
}

void insert_data_hll(MutableColumns* hll_cols, DataTypePtr datetype_hll, int rows_value,
                     std::vector<std::string>* data_strs = nullptr) {
    auto serde_hll = datetype_hll->get_serde(1);
    auto column_hll = datetype_hll->create_column();

    hll_cols->push_back(column_hll->get_ptr());
    DataTypeSerDeSPtrs serde = {datetype_hll->get_serde()};
    auto& data = assert_cast<ColumnHLL*>((*hll_cols)[0].get())->get_data();
    for (size_t i = 0; i != rows_value; ++i) {
        HyperLogLog hll_value;
        for (size_t j = 0; j <= i; ++j) {
            hll_value.update(j);
        }
        if (data_strs) {
            data_strs->push_back(hll_value.to_string());
        }
        std::string memory_buffer(hll_value.max_serialized_size(), '0');
        hll_value.serialize(reinterpret_cast<uint8_t*>(memory_buffer.data()));
        data.emplace_back(std::move(hll_value));
    }
    std::cout << "finish insert data" << std::endl;
}

// serialize / deserialize
TEST_P(DataTypeHLLTest, SerializeDeserializeTest) {
    MutableColumns hll_cols;
    insert_data_hll(&hll_cols, dt_hll, rows_value);

    auto* column = assert_cast<ColumnHLL*>(hll_cols[0].get());
    auto size = dt_hll->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result =
            dt_hll->serialize(*column, buf.get(), BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = dt_hll->create_column();
    dt_hll->deserialize(buf.get(), &column2, BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnHLL*>(column2.get());
        ASSERT_EQ(column->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }

    auto column3 = dt_hll->create_column();
    dt_hll->deserialize(buf.get(), buf.get() + size, &column3,
                        BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnHLL*>(column3.get());
        ASSERT_EQ(column->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }
    helper->serialize_deserialize_assert(hll_cols, {dt_hll});
    std::cout << "finish serialize deserialize test" << std::endl;
}

// serialize / deserialize as stream
TEST_P(DataTypeHLLTest, SerializeDeserializeAsStreamTest) {
    MutableColumns hll_cols;
    insert_data_hll(&hll_cols, dt_hll, rows_value);

    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto* column_data = assert_cast<ColumnHLL*>(hll_cols[0].get());
    auto c = dt_hll->create_column();
    auto* column_res = assert_cast<ColumnHLL*>(c.get());
    column_res->resize(rows_value);
    for (size_t i = 0; i != rows_value; ++i) {
        doris::DataTypeHLL::serialize_as_stream(column_data->get_element(i), buffer_writer);
        buffer_writer.commit();
        BufferReadable buffer_readable(ser_col->get_data_at(i));
        doris::DataTypeHLL::deserialize_as_stream(column_res->get_element(i), buffer_readable);
        ASSERT_EQ(column_data->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }
    std::cout << "finish serialize deserialize as stream test" << std::endl;
}

TEST(DataTypeHLLBoundsTest, RejectTruncatedValue) {
    auto dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
    std::vector<char> buf(sizeof(bool) + 2 * sizeof(size_t) + 2 * sizeof(size_t) + 1);
    char* ptr = buf.data();
    bool is_const = false;
    memcpy(ptr, &is_const, sizeof(is_const));
    ptr += sizeof(is_const);
    size_t row_num = 1;
    memcpy(ptr, &row_num, sizeof(row_num));
    ptr += sizeof(row_num);
    size_t saved_num = 1;
    memcpy(ptr, &saved_num, sizeof(saved_num));
    ptr += sizeof(saved_num);
    size_t hll_size = HLL_COLUMN_DEFAULT_LEN;
    memcpy(ptr, &hll_size, sizeof(hll_size));
    ptr += sizeof(hll_size);
    *ptr = HLL_DATA_FULL;

    auto column = dt_hll->create_column();
    EXPECT_THROW(dt_hll->deserialize(buf.data(), buf.data() + buf.size(), &column,
                                     BeExecVersionManager::get_newest_version()),
                 doris::Exception);
}

TEST(DataTypeHLLBoundsTest, RejectTruncatedLegacyValue) {
    auto dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
    std::vector<char> buf(2 * sizeof(size_t) + 1);
    size_t row_num = 1;
    memcpy(buf.data(), &row_num, sizeof(row_num));
    size_t hll_size = HLL_COLUMN_DEFAULT_LEN;
    memcpy(buf.data() + sizeof(size_t), &hll_size, sizeof(hll_size));
    buf.back() = HLL_DATA_FULL;

    auto column = dt_hll->create_column();
    EXPECT_THROW(
            dt_hll->deserialize(buf.data(), buf.data() + buf.size(), &column, USE_CONST_SERDE - 1),
            doris::Exception);
}

TEST(DataTypeHLLBoundsTest, RejectInconsistentRowCount) {
    auto dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
    std::vector<char> buf(sizeof(bool) + 2 * sizeof(size_t));
    char* ptr = buf.data();
    bool is_const = false;
    memcpy(ptr, &is_const, sizeof(is_const));
    ptr += sizeof(is_const);
    size_t row_num = 2;
    memcpy(ptr, &row_num, sizeof(row_num));
    ptr += sizeof(row_num);
    size_t saved_num = 1;
    memcpy(ptr, &saved_num, sizeof(saved_num));

    auto column = dt_hll->create_column();
    EXPECT_THROW(dt_hll->deserialize(buf.data(), buf.data() + buf.size(), &column,
                                     BeExecVersionManager::get_newest_version()),
                 doris::Exception);
}

TEST(DataTypeHLLBoundsTest, AcceptCompressedNullableHLL) {
    auto dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
    auto dt_nullable = std::make_shared<DataTypeNullable>(dt_hll);

    for (int be_exec_version : {USE_CONST_SERDE - 1, BeExecVersionManager::get_newest_version()}) {
        auto nested = dt_nullable->get_nested_type()->create_column();
        nested->insert_many_defaults(257);
        auto nullable = ColumnNullable::create(std::move(nested), ColumnUInt8::create(257, 0));

        std::vector<char> buf(
                dt_nullable->get_uncompressed_serialized_bytes(*nullable, be_exec_version));
        char* end = dt_nullable->serialize(*nullable, buf.data(), be_exec_version);

        auto decoded = dt_nullable->create_column();
        dt_nullable->deserialize(buf.data(), end, &decoded, be_exec_version);
        EXPECT_EQ(decoded->size(), 257);
    }
}

TEST(DataTypeHLLBoundsTest, RejectInvalidCompressedNullMap) {
    auto dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
    auto dt_nullable = std::make_shared<DataTypeNullable>(dt_hll);

    for (int be_exec_version : {USE_CONST_SERDE - 1, BeExecVersionManager::get_newest_version()}) {
        auto nested = dt_nullable->get_nested_type()->create_column();
        nested->insert_many_defaults(257);
        auto nullable = ColumnNullable::create(std::move(nested), ColumnUInt8::create(257, 0));

        std::vector<char> buf(
                dt_nullable->get_uncompressed_serialized_bytes(*nullable, be_exec_version));
        char* end = dt_nullable->serialize(*nullable, buf.data(), be_exec_version);
        size_t header_size = be_exec_version >= USE_CONST_SERDE ? sizeof(bool) + 2 * sizeof(size_t)
                                                                : sizeof(uint32_t);
        size_t invalid_encode_size = 1;
        memcpy(buf.data() + header_size, &invalid_encode_size, sizeof(invalid_encode_size));

        auto column = dt_nullable->create_column();
        EXPECT_THROW(dt_nullable->deserialize(buf.data(), end, &column, be_exec_version),
                     doris::Exception);
    }
}

TEST(DataTypeHLLBoundsTest, RejectTruncatedNullableHLL) {
    auto dt_nullable = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeHLL>());
    for (int version : {USE_CONST_SERDE - 1, BeExecVersionManager::get_newest_version()}) {
        SCOPED_TRACE(version);
        auto column = dt_nullable->create_column();
        column->insert_default();
        std::vector<char> buf(dt_nullable->get_uncompressed_serialized_bytes(*column, version));
        auto* end = dt_nullable->serialize(*column, buf.data(), version);
        buf.resize(end - buf.data());
        size_t outer_header =
                version >= USE_CONST_SERDE ? sizeof(bool) + 2 * sizeof(size_t) : sizeof(uint32_t);
        size_t child_header =
                version >= USE_CONST_SERDE ? sizeof(bool) + 2 * sizeof(size_t) : sizeof(size_t);
        size_t child_start = outer_header + 1;
        size_t value_start = child_start + child_header + sizeof(size_t);
        unaligned_store<size_t>(buf.data() + child_start + child_header, HLL_COLUMN_DEFAULT_LEN);
        buf[value_start] = HLL_DATA_FULL;

        for (size_t cut : {child_start, child_start + child_header - 1, child_start + child_header,
                           value_start - 1, value_start, value_start + 1}) {
            SCOPED_TRACE(cut);
            auto decoded = dt_nullable->create_column();
            EXPECT_THROW(dt_nullable->deserialize(buf.data(), buf.data() + cut, &decoded, version),
                         doris::Exception);
        }
    }
}

class DataTypeHLLHeaderBoundsTest : public ::testing::TestWithParam<std::tuple<int, bool>> {};

TEST_P(DataTypeHLLHeaderBoundsTest, RejectMalformedHeadersAndLengths) {
    const int version = std::get<0>(GetParam());
    const bool nullable = std::get<1>(GetParam());
    DataTypePtr type = std::make_shared<DataTypeHLL>();
    if (nullable) {
        type = std::make_shared<DataTypeNullable>(type);
    }
    auto column = type->create_column();
    column->insert_default();
    std::vector<char> buf(type->get_uncompressed_serialized_bytes(*column, version));
    auto* end = type->serialize(*column, buf.data(), version);
    auto reject = [&type, version](const char* begin, const char* limit) {
        auto decoded = type->create_column();
        EXPECT_THROW(type->deserialize(begin, limit, &decoded, version), doris::Exception);
    };
    reject(buf.data() + 1, buf.data());
    size_t legacy_header = nullable ? sizeof(uint32_t) : sizeof(size_t);
    size_t header = version >= USE_CONST_SERDE ? sizeof(bool) + 2 * sizeof(size_t) : legacy_header;
    reject(buf.data(), buf.data());
    reject(buf.data(), buf.data() + header - 1);
    reject(buf.data(), buf.data() + header);
    if (version >= USE_CONST_SERDE) {
        buf[0] = 2;
        reject(buf.data(), end);
        buf[0] = 0;
        unaligned_store<size_t>(buf.data() + sizeof(bool) + sizeof(size_t), 2);
        reject(buf.data(), end);
        unaligned_store<size_t>(buf.data() + sizeof(bool) + sizeof(size_t), 1);
    }
    if (!nullable) {
        reject(buf.data(), buf.data() + header + sizeof(size_t));
        unaligned_store<size_t>(buf.data() + header, HLL_COLUMN_DEFAULT_LEN + 1);
        reject(buf.data(), end);
        unaligned_store<size_t>(buf.data() + header, 1);
        buf[header + sizeof(size_t)] = 4;
        reject(buf.data(), end);
    }
}

INSTANTIATE_TEST_SUITE_P(
        WireLayouts, DataTypeHLLHeaderBoundsTest,
        ::testing::Combine(::testing::Values(USE_CONST_SERDE - 1,
                                             BeExecVersionManager::get_newest_version()),
                           ::testing::Bool()));

TEST(DataTypeHLLBoundsTest, RejectMalformedCompressedNullMaps) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeHLL>());
    for (int version : {USE_CONST_SERDE - 1, BeExecVersionManager::get_newest_version()}) {
        SCOPED_TRACE(version);
        auto nested = type->get_nested_type()->create_column();
        nested->insert_many_defaults(512);
        auto column = ColumnNullable::create(std::move(nested), ColumnUInt8::create(512, 0));
        std::vector<char> original(type->get_uncompressed_serialized_bytes(*column, version));
        auto* end = type->serialize(*column, original.data(), version);
        original.resize(end - original.data());
        size_t header =
                version >= USE_CONST_SERDE ? sizeof(bool) + 2 * sizeof(size_t) : sizeof(uint32_t);
        auto encoded_size = unaligned_load<size_t>(original.data() + header);
        auto reject = [&](const std::vector<char>& buf, size_t input_size) {
            auto decoded = type->create_column();
            EXPECT_THROW(type->deserialize(buf.data(), buf.data() + input_size, &decoded, version),
                         doris::Exception);
        };
        reject(original, header + sizeof(size_t) - 1);
        reject(original, header + sizeof(size_t) + encoded_size);
        // A 512-byte map has 128 integers and 32 control bytes. Reject inconsistent
        // control/data sizes before invoking the SIMD decoder or allocating the null map.
        for (size_t bad_size :
             {std::numeric_limits<size_t>::max(), size_t(31), size_t(32), encoded_size + 1}) {
            SCOPED_TRACE(bad_size);
            auto buf = original;
            unaligned_store<size_t>(buf.data() + header, bad_size);
            reject(buf, buf.size());
        }
        auto buf = original;
        buf[header + sizeof(size_t)] = static_cast<char>(0xff);
        reject(buf, buf.size());
        if (version >= USE_CONST_SERDE) {
            buf = original;
            size_t oversized = size_t(std::numeric_limits<uint32_t>::max()) + 1;
            unaligned_store<size_t>(buf.data() + sizeof(bool), oversized);
            unaligned_store<size_t>(buf.data() + sizeof(bool) + sizeof(size_t), oversized);
            reject(buf, buf.size());
        }
    }
}

class NullableTinyIntSerdeTest : public ::testing::TestWithParam<std::tuple<int, size_t, size_t>> {
};

TEST_P(NullableTinyIntSerdeTest, CompressedRoundTrip) {
    auto [version, rows, null_byte] = GetParam();
    auto dt_nullable = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>());
    auto nested = dt_nullable->get_nested_type()->create_column();
    nested->insert_many_defaults(rows);
    auto null_map = ColumnUInt8::create(rows, 0);
    for (size_t i = 0; i < rows; ++i) {
        null_map->get_data()[i] = i % 4 == null_byte;
    }
    auto column = ColumnNullable::create(std::move(nested), std::move(null_map));
    Block block({ColumnWithTypeAndName(std::move(column), dt_nullable, "tinyint")});
    PBlock pblock;
    size_t uncompressed = 0;
    size_t compressed = 0;
    int64_t compress_time = 0;
    ASSERT_TRUE(block.serialize(version, &pblock, &uncompressed, &compressed, &compress_time,
                                segment_v2::NO_COMPRESSION));
    if (null_byte == 4) {
        // The uncompressed null map is larger than the entire encoded block.
        EXPECT_LT(pblock.column_values().size(), rows);
    }
    auto trusted_decoded = dt_nullable->create_column();
    dt_nullable->deserialize(pblock.column_values().data(), &trusted_decoded, version);
    EXPECT_EQ(trusted_decoded->size(), rows);
    Block decoded;
    size_t decoded_bytes = 0;
    int64_t decompress_time = 0;
    ASSERT_TRUE(decoded.deserialize(pblock, &decoded_bytes, &decompress_time));
    ASSERT_EQ(decoded.rows(), rows);
    const auto& result = assert_cast<const ColumnNullable&>(*decoded.get_by_position(0).column);
    for (size_t i = 0; i < rows; ++i) {
        EXPECT_EQ(result.is_null_at(i), i % 4 == null_byte);
        EXPECT_EQ(result.get_nested_column().get_int(i), 0);
    }
}

// Exercise one-, two-, three- and four-byte StreamVByte values and a partial tail.
INSTANTIATE_TEST_SUITE_P(
        WireLayouts, NullableTinyIntSerdeTest,
        ::testing::Combine(
                ::testing::Values(USE_CONST_SERDE - 1, BeExecVersionManager::get_newest_version()),
                ::testing::Values(size_t {257}, size_t {512}, size_t {513}),
                ::testing::Values(size_t {4}, size_t {0}, size_t {1}, size_t {2}, size_t {3})));

class NullableHLLConstSerdeTest : public ::testing::TestWithParam<std::tuple<int, size_t, bool>> {};

TEST_P(NullableHLLConstSerdeTest, RoundTrip) {
    auto [version, rows, is_null] = GetParam();
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeHLL>());
    auto nested = type->get_nested_type()->create_column();
    nested->insert_default();
    auto nullable = ColumnNullable::create(std::move(nested), ColumnUInt8::create(1, is_null));
    auto column = ColumnConst::create(std::move(nullable), rows);
    std::vector<char> buf(type->get_uncompressed_serialized_bytes(*column, version));
    auto* end = type->serialize(*column, buf.data(), version);
    auto decoded = type->create_column();
    EXPECT_EQ(type->deserialize(buf.data(), end, &decoded, version), end);
    EXPECT_EQ(decoded->size(), rows);
    EXPECT_EQ(is_column_const(*decoded), version >= USE_CONST_SERDE);
    auto full = decoded->convert_to_full_column_if_const();
    const auto& result = assert_cast<const ColumnNullable&>(*full);
    for (size_t row = 0; row < rows; ++row) {
        EXPECT_EQ(result.is_null_at(row), is_null);
    }
}

INSTANTIATE_TEST_SUITE_P(
        WireLayouts, NullableHLLConstSerdeTest,
        ::testing::Combine(::testing::Values(USE_CONST_SERDE - 1,
                                             BeExecVersionManager::get_newest_version()),
                           ::testing::Values(size_t {0}, size_t {1}, size_t {512}),
                           ::testing::Bool()));

class NestedHLLSerdeTest : public ::testing::TestWithParam<std::tuple<int, int>> {
public:
    static void reject(const DataTypePtr& type, int version, const char* begin, const char* end) {
        auto decoded = type->create_column();
        EXPECT_THROW(type->deserialize(begin, end, &decoded, version), doris::Exception);
    }

    static MutableColumnPtr nullable_hll_column() {
        auto column = ColumnHLL::create();
        column->insert_default();
        return ColumnNullable::create(std::move(column), ColumnUInt8::create(1, 0));
    }

    static std::pair<DataTypePtr, MutableColumnPtr> make_column(int wrapper) {
        DataTypePtr hll = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeHLL>());
        if (wrapper == 0) {
            return {std::make_shared<DataTypeArray>(hll),
                    ColumnArray::create(nullable_hll_column(),
                                        ColumnArray::ColumnOffsets::create(1, 1))};
        }
        if (wrapper == 1) {
            return {std::make_shared<DataTypeMap>(hll, hll),
                    ColumnMap::create(nullable_hll_column(), nullable_hll_column(),
                                      ColumnMap::COffsets::create(1, 1))};
        }
        DataTypePtr type = std::make_shared<DataTypeStruct>(DataTypes {hll, hll});
        auto column = type->create_column();
        column->insert_default();
        if (wrapper == 2) {
            return {type, std::move(column)};
        }
        type = std::make_shared<DataTypeNullable>(type);
        column = ColumnNullable::create(std::move(column), ColumnUInt8::create(1, 0));
        type = std::make_shared<DataTypeMap>(hll, type);
        column = ColumnMap::create(nullable_hll_column(), std::move(column),
                                   ColumnMap::COffsets::create(1, 1));
        column = ColumnNullable::create(std::move(column), ColumnUInt8::create(1, 0));
        type = std::make_shared<DataTypeArray>(type);
        column = ColumnArray::create(std::move(column), ColumnArray::ColumnOffsets::create(1, 1));
        type = std::make_shared<DataTypeNullable>(type);
        column = ColumnNullable::create(std::move(column), ColumnUInt8::create(1, 0));
        return {type, std::move(column)};
    }

    static void check_const_round_trips(const DataTypePtr& type, const MutableColumnPtr& column,
                                        int version) {
        for (size_t rows : {size_t {0}, size_t {1}, size_t {512}}) {
            SCOPED_TRACE(rows);
            auto constant = ColumnConst::create(column->get_ptr(), rows);
            std::vector<char> buf(type->get_uncompressed_serialized_bytes(*constant, version));
            const char* end = type->serialize(*constant, buf.data(), version);
            auto decoded = type->create_column();
            EXPECT_EQ(type->deserialize(buf.data(), end, &decoded, version), end);
            EXPECT_EQ(decoded->size(), rows);
            std::vector<char> restored(type->get_uncompressed_serialized_bytes(*decoded, version));
            const char* restored_end = type->serialize(*decoded, restored.data(), version);
            EXPECT_EQ(std::string(buf.data(), end - buf.data()),
                      std::string(restored.data(), restored_end - restored.data()));
            auto trusted = type->create_column();
            EXPECT_EQ(type->deserialize(buf.data(), &trusted, version), end);
            EXPECT_EQ(trusted->size(), rows);
        }
    }
};

TEST_P(NestedHLLSerdeTest, BoundedRoundTripAndTruncatedHLL) {
    const int version = std::get<0>(GetParam());
    const int wrapper = std::get<1>(GetParam());
    auto [type, column] = make_column(wrapper);
    check_const_round_trips(type, column, version);
    std::vector<char> buf(type->get_uncompressed_serialized_bytes(*column, version));
    auto* end = type->serialize(*column, buf.data(), version);
    buf.resize(end - buf.data());
    for (size_t cut = 0; cut < buf.size(); ++cut) {
        SCOPED_TRACE(cut);
        reject(type, version, buf.data(), buf.data() + cut);
    }
    reject(type, version, buf.data() + 1, buf.data());
    if (wrapper < 2) {
        auto oversized = buf;
        size_t offset = version >= USE_CONST_SERDE ? sizeof(bool) : 0;
        unaligned_store<size_t>(oversized.data() + offset, std::numeric_limits<size_t>::max());
        if (version >= USE_CONST_SERDE) {
            unaligned_store<size_t>(oversized.data() + offset + sizeof(size_t),
                                    std::numeric_limits<size_t>::max());
        }
        reject(type, version, oversized.data(), oversized.data() + oversized.size());
    }
    PBlock pblock;
    pblock.set_be_exec_version(version);
    auto* meta = pblock.add_column_metas();
    type->to_pb_column_meta(meta);
    meta->set_name("nested");
    pblock.set_column_values(std::string(buf.data(), buf.size()));
    size_t bytes = 0;
    int64_t decompress_time = 0;
    Block valid;
    ASSERT_TRUE(valid.deserialize(pblock, &bytes, &decompress_time).ok());
    EXPECT_EQ(valid.rows(), 1);
    // Every fixture ends in one empty HLL. Claim a FULL value while providing only its tag.
    ASSERT_GE(buf.size(), sizeof(size_t) + 1);
    unaligned_store<size_t>(buf.data() + buf.size() - 1 - sizeof(size_t), HLL_COLUMN_DEFAULT_LEN);
    buf.back() = HLL_DATA_FULL;
    pblock.set_column_values(std::string(buf.data(), buf.size()));
    Block malformed;
    EXPECT_FALSE(malformed.deserialize(pblock, &bytes, &decompress_time).ok());
}

INSTANTIATE_TEST_SUITE_P(
        WireLayouts, NestedHLLSerdeTest,
        ::testing::Combine(::testing::Values(USE_CONST_SERDE - 1,
                                             BeExecVersionManager::get_newest_version()),
                           ::testing::Values(0, 1, 2, 3)));

INSTANTIATE_TEST_SUITE_P(Params, DataTypeHLLTest, ::testing::Values(0, 1, 10, 100));

} // namespace doris
