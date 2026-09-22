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

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/lance/lance_reader_helper.h"

namespace doris::format::lance {
namespace {

class LanceNestedNullTest : public testing::Test {
protected:
    void SetUp() override {
        _old_validation = config::enable_arrow_input_validation;
        config::enable_arrow_input_validation = true;
    }
    void TearDown() override { config::enable_arrow_input_validation = _old_validation; }

    DataTypePtr null_type() {
        return DataTypeFactory::instance().create_data_type(TYPE_NULL, true);
    }

    void expect_nulls(const IColumn& column, size_t count) {
        const auto& nullable = assert_cast<const ColumnNullable&>(column);
        ASSERT_EQ(count, nullable.size());
        ASSERT_EQ(count, nullable.get_nested_column().size());
        for (size_t row = 0; row < count; ++row) {
            EXPECT_TRUE(nullable.is_null_at(row));
        }
    }

    void read(const std::shared_ptr<arrow::Array>& array, const DataTypePtr& type,
              MutableColumnPtr* column) {
        std::shared_ptr<arrow::Array> normalized;
        ASSERT_TRUE(normalize_lance_arrow_array_for_test(arrow::field("value", array->type()),
                                                         array, &normalized,
                                                         arrow::default_memory_pool())
                            .ok());
        *column = type->create_column();
        ASSERT_TRUE(type->get_serde()
                            ->read_column_from_arrow(**column, normalized.get(), 0,
                                                     normalized->length(), cctz::time_zone {})
                            .ok());
    }

private:
    bool _old_validation;
};

TEST_F(LanceNestedNullTest, MapsNestedNullSchemas) {
    const auto null_field = arrow::field("value", arrow::null());
    const auto schema = arrow::schema({
            arrow::field("list", arrow::list(null_field)),
            arrow::field("large_list", arrow::large_list(null_field)),
            arrow::field("fixed_list", arrow::fixed_size_list(null_field, 2)),
            arrow::field("struct", arrow::struct_({null_field})),
            arrow::field("map", arrow::map(arrow::utf8(), null_field)),
            arrow::field("nested", arrow::list(arrow::struct_({null_field}))),
            arrow::field("list_list", arrow::list(arrow::list(null_field))),
            arrow::field("struct_list",
                         arrow::struct_({arrow::field("list", arrow::list(null_field))})),
            arrow::field("list_map", arrow::list(arrow::map(arrow::utf8(), null_field))),
    });
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(convert_arrow_schema_to_doris(schema, &names, &types).ok());
    ASSERT_EQ(9, types.size());
    for (const auto& type : types) {
        EXPECT_NE(INVALID_TYPE, type->get_primitive_type());
    }
    for (size_t i = 0; i < 3; ++i) {
        ASSERT_EQ(TYPE_ARRAY, types[i]->get_primitive_type());
        EXPECT_TRUE(assert_cast<const DataTypeArray&>(*remove_nullable(types[i]))
                            .get_nested_type()
                            ->is_null_literal());
    }
}

TEST_F(LanceNestedNullTest, RejectsNonNullableNullLeaves) {
    const auto leaf = arrow::field("item", arrow::null(), false);
    for (const auto& type : std::vector<std::shared_ptr<arrow::DataType>> {
                 arrow::null(), arrow::list(leaf), arrow::large_list(leaf),
                 arrow::fixed_size_list(leaf, 2), arrow::struct_({leaf}),
                 arrow::map(arrow::null(), arrow::utf8()), arrow::map(arrow::utf8(), leaf),
                 arrow::list(arrow::map(arrow::utf8(), leaf))}) {
        SCOPED_TRACE(type->ToString());
        std::vector<std::string> names;
        std::vector<DataTypePtr> types;
        ASSERT_TRUE(convert_arrow_schema_to_doris(
                            arrow::schema({arrow::field("value", type, false)}), &names, &types)
                            .ok());
        ASSERT_EQ(1, types.size());
        EXPECT_EQ(INVALID_TYPE, types[0]->get_primitive_type());
    }
}

TEST_F(LanceNestedNullTest, PreservesNullTypeInNestedSchemaRpc) {
    const auto null = null_type();
    const auto type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {null, make_nullable(std::make_shared<DataTypeArray>(null)),
                       make_nullable(std::make_shared<DataTypeMap>(
                               DataTypeFactory::instance().create_data_type(TYPE_STRING, true),
                               null))},
            Strings {"scalar", "list", "map"}));
    PTypeDesc descriptor;
    type->to_protobuf(&descriptor);
    ASSERT_EQ(7, descriptor.types_size());
    for (int index : {1, 3, 6}) {
        EXPECT_EQ(TPrimitiveType::NULL_TYPE, descriptor.types(index).scalar_type().type());
    }
}

TEST_F(LanceNestedNullTest, ReadsNullLeafRangesWithoutPhysicalBuffers) {
    const auto type = null_type();
    const auto source = std::make_shared<arrow::NullArray>(7)->Slice(2, 4);
    for (bool validation : {false, true}) {
        config::enable_arrow_input_validation = validation;
        auto column = type->create_column();
        column->insert_default();
        ASSERT_TRUE(
                type->get_serde()
                        ->read_column_from_arrow(*column, source.get(), 1, 3, cctz::time_zone {})
                        .ok());
        expect_nulls(*column, 3);
        ASSERT_TRUE(
                type->get_serde()
                        ->read_column_from_arrow(*column, source.get(), 4, 4, cctz::time_zone {})
                        .ok());
        expect_nulls(*column, 3);
    }
}

TEST_F(LanceNestedNullTest, RejectsInvalidNullRangesBeforeAppending) {
    const auto type = null_type();
    const arrow::NullArray source(3);
    for (bool validation : {false, true}) {
        config::enable_arrow_input_validation = validation;
        auto column = type->create_column();
        column->insert_default();
        for (const auto& [start, end] :
             std::vector<std::pair<int64_t, int64_t>> {{-1, 1}, {2, 1}, {0, 4}}) {
            EXPECT_FALSE(type->get_serde()
                                 ->read_column_from_arrow(*column, &source, start, end,
                                                          cctz::time_zone {})
                                 .ok());
            expect_nulls(*column, 1);
        }
    }
}

TEST_F(LanceNestedNullTest, ReadsNullStructParents) {
    const std::vector<uint8_t> validity {0x05};
    const auto source = std::make_shared<arrow::StructArray>(
            arrow::struct_({arrow::field("empty", arrow::null())}), 3,
            arrow::ArrayVector {std::make_shared<arrow::NullArray>(3)},
            arrow::Buffer::Wrap(validity), 1);
    const auto type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {null_type()}, Strings {"empty"}));
    MutableColumnPtr column;
    read(source->Slice(1, 2), type, &column);
    ASSERT_NE(nullptr, column.get());
    const auto& parent = assert_cast<const ColumnNullable&>(*column);
    EXPECT_EQ((NullMap {1, 0}), parent.get_null_map_data());
    const auto& structure = assert_cast<const ColumnStruct&>(parent.get_nested_column());
    expect_nulls(structure.get_column(0), 2);
}

TEST_F(LanceNestedNullTest, ReadsNullListElementsAndPreservesParentShape) {
    const auto null = null_type();
    const auto type = make_nullable(std::make_shared<DataTypeArray>(null));
    const auto values = std::make_shared<arrow::NullArray>(5);
    const std::vector<int32_t> offsets {0, 2, 2, 2, 5};
    const std::vector<int64_t> large_offsets {0, 2, 2, 2, 5};
    const std::vector<uint8_t> validity {0x0d};
    const std::vector<std::shared_ptr<arrow::Array>> arrays {
            std::make_shared<arrow::ListArray>(arrow::list(arrow::null()), 4,
                                               arrow::Buffer::Wrap(offsets), values,
                                               arrow::Buffer::Wrap(validity), 1),
            std::make_shared<arrow::LargeListArray>(arrow::large_list(arrow::null()), 4,
                                                    arrow::Buffer::Wrap(large_offsets), values,
                                                    arrow::Buffer::Wrap(validity), 1),
    };
    for (const auto& source : arrays) {
        for (bool sliced : {false, true}) {
            MutableColumnPtr column;
            read(sliced ? source->Slice(1, 3) : source, type, &column);
            ASSERT_NE(nullptr, column.get());
            const auto& parent = assert_cast<const ColumnNullable&>(*column);
            EXPECT_EQ((sliced ? NullMap {1, 0, 0} : NullMap {0, 1, 0, 0}),
                      parent.get_null_map_data());
            const auto& list = assert_cast<const ColumnArray&>(parent.get_nested_column());
            EXPECT_EQ((sliced ? ColumnArray::Offsets64 {0, 0, 3}
                              : ColumnArray::Offsets64 {2, 2, 2, 5}),
                      list.get_offsets());
            expect_nulls(list.get_data(), sliced ? 3 : 5);
        }
    }
}

TEST_F(LanceNestedNullTest, ReadsFixedNullLists) {
    const std::vector<uint8_t> validity {0x05};
    const auto source = std::make_shared<arrow::FixedSizeListArray>(
            arrow::fixed_size_list(arrow::null(), 2), 3, std::make_shared<arrow::NullArray>(6),
            arrow::Buffer::Wrap(validity), 1);
    const auto type = make_nullable(std::make_shared<DataTypeArray>(null_type()));
    MutableColumnPtr column;
    read(source->Slice(1, 2), type, &column);
    ASSERT_NE(nullptr, column.get());
    const auto& parent = assert_cast<const ColumnNullable&>(*column);
    EXPECT_EQ((NullMap {1, 0}), parent.get_null_map_data());
    const auto& list = assert_cast<const ColumnArray&>(parent.get_nested_column());
    EXPECT_EQ((ColumnArray::Offsets64 {2, 4}), list.get_offsets());
    expect_nulls(list.get_data(), 4);
}

TEST_F(LanceNestedNullTest, ReadsNullStructFieldsInsideLists) {
    const auto nulls = std::make_shared<arrow::NullArray>(3);
    const std::vector<int32_t> ids {10, 20, 30};
    const auto id_array = std::make_shared<arrow::Int32Array>(3, arrow::Buffer::Wrap(ids));
    const auto struct_type = arrow::struct_(
            {arrow::field("empty", arrow::null()), arrow::field("id", arrow::int32())});
    const auto structs = std::make_shared<arrow::StructArray>(struct_type, 3,
                                                              arrow::ArrayVector {nulls, id_array});
    const std::vector<int32_t> offsets {0, 1, 3};
    const auto source = std::make_shared<arrow::ListArray>(arrow::list(struct_type), 2,
                                                           arrow::Buffer::Wrap(offsets), structs);
    const auto doris_struct = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {null_type(), DataTypeFactory::instance().create_data_type(TYPE_INT, true)},
            Strings {"empty", "id"}));
    MutableColumnPtr column;
    read(source->Slice(1, 1), make_nullable(std::make_shared<DataTypeArray>(doris_struct)),
         &column);
    ASSERT_NE(nullptr, column.get());
    const auto& list = assert_cast<const ColumnArray&>(
            assert_cast<const ColumnNullable&>(*column).get_nested_column());
    const auto& result = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(list.get_data()).get_nested_column());
    expect_nulls(result.get_column(0), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(result.get_column(1)).get_nested_column());
    EXPECT_EQ((ColumnInt32::Container {20, 30}), id_column.get_data());
}

TEST_F(LanceNestedNullTest, ReadsNullMapValues) {
    const std::vector<int32_t> keys {10, 20, 30};
    const std::vector<int32_t> offsets {0, 1, 1, 3};
    const std::vector<uint8_t> validity {0x05};
    const auto source = std::make_shared<arrow::MapArray>(
            arrow::map(arrow::int32(), arrow::null()), 3, arrow::Buffer::Wrap(offsets),
            std::make_shared<arrow::Int32Array>(3, arrow::Buffer::Wrap(keys)),
            std::make_shared<arrow::NullArray>(3), arrow::Buffer::Wrap(validity), 1);
    const auto type = make_nullable(std::make_shared<DataTypeMap>(
            DataTypeFactory::instance().create_data_type(TYPE_INT, true), null_type()));
    MutableColumnPtr column;
    read(source->Slice(1, 2), type, &column);
    ASSERT_NE(nullptr, column.get());
    const auto& parent = assert_cast<const ColumnNullable&>(*column);
    EXPECT_EQ((NullMap {1, 0}), parent.get_null_map_data());
    const auto& map = assert_cast<const ColumnMap&>(parent.get_nested_column());
    EXPECT_EQ((ColumnArray::Offsets64 {0, 2}), map.get_offsets());
    expect_nulls(map.get_values(), 2);
    const auto& key_column = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(map.get_keys()).get_nested_column());
    EXPECT_EQ((ColumnInt32::Container {20, 30}), key_column.get_data());
}

} // namespace
} // namespace doris::format::lance
