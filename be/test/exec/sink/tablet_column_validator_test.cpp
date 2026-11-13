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

#include "exec/sink/tablet_column_validator.h"

#include <gtest/gtest.h>

#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"

namespace doris {
namespace {

// Capture leaf storage without keeping owning pointers. Complex shells may change.
struct StorageSnapshot {
    const IColumn* column;
    const void* data;
};

std::vector<StorageSnapshot> snapshot(const IColumn& column) {
    std::vector<StorageSnapshot> result;
    const auto visit = [&](const auto& self, const IColumn& current) -> void {
        const void* data = nullptr;
        if (const auto* ints = check_and_get_column<ColumnInt32>(current)) {
            data = ints->get_data().data();
        } else if (const auto* bytes = check_and_get_column<ColumnUInt8>(current)) {
            data = bytes->get_data().data();
        } else if (const auto* offsets =
                           check_and_get_column<ColumnArray::ColumnOffsets>(current)) {
            data = offsets->get_data().data();
        } else if (const auto* strings = check_and_get_column<ColumnString>(current)) {
            data = strings->get_chars().data();
        } else if (const auto* decimals = check_and_get_column<ColumnDecimal128V2>(current)) {
            data = decimals->get_data().data();
        }
        result.push_back({&current, data});
        if (const auto* strings = check_and_get_column<ColumnString>(current)) {
            result.push_back({&current, strings->get_offsets().data()});
        } else if (const auto* nullable = check_and_get_column<ColumnNullable>(current)) {
            self(self, nullable->get_nested_column());
            self(self, nullable->get_null_map_column());
        } else if (const auto* array = check_and_get_column<ColumnArray>(current)) {
            self(self, array->get_data());
            self(self, array->get_offsets_column());
        } else if (const auto* map = check_and_get_column<ColumnMap>(current)) {
            self(self, map->get_keys());
            self(self, map->get_values());
            self(self, *map->get_offsets_ptr());
        } else if (const auto* tuple = check_and_get_column<ColumnStruct>(current)) {
            for (size_t i = 0; i < tuple->tuple_size(); ++i) {
                self(self, tuple->get_column(i));
            }
        }
    };
    visit(visit, column);
    return result;
}

void expect_same_storage(const std::vector<StorageSnapshot>& before, const IColumn& column) {
    const auto after = snapshot(column);
    ASSERT_EQ(before.size(), after.size());
    for (size_t i = 0; i < before.size(); ++i) {
        if (before[i].data) {
            EXPECT_EQ(before[i].column, after[i].column) << "leaf " << i;
            EXPECT_EQ(before[i].data, after[i].data) << "leaf " << i;
        }
    }
}

ColumnInt32::MutablePtr ints(std::initializer_list<Int32> values) {
    auto column = ColumnInt32::create();
    for (auto value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnArray::ColumnOffsets::MutablePtr offsets(std::initializer_list<UInt64> values) {
    auto column = ColumnArray::ColumnOffsets::create();
    for (auto value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnString::MutablePtr strings(std::initializer_list<std::string> values) {
    auto column = ColumnString::create();
    for (const auto& value : values) {
        column->insert_data(value.data(), value.size());
    }
    return column;
}

DataTypePtr decimal_v2_type() {
    return DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL, 9, 2);
}

TQueryOptions options(bool strict) {
    TQueryOptions options;
    options.__set_enable_insert_strict(strict);
    options.__set_be_exec_version(BeExecVersionManager::get_newest_version());
    return options;
}

} // namespace

class TabletColumnValidatorTest : public testing::Test {
protected:
    TabletColumnValidatorTest() : state(options(true), TQueryGlobals {}), context(filter_map) {
        context.state = &state;
    }

    Result<ColumnPtr> validate(const ColumnPtr& column, const DataTypePtr& type) {
        filter_map.assign(column->size(), 0);
        return tablet_validate_column(column, type, context);
    }

    RuntimeState state;
    std::vector<char> filter_map;
    TabletValidateContext context;
};

TEST_F(TabletColumnValidatorTest, UniqueScalarKeepsObjectBufferAndReferenceCount) {
    ColumnPtr column = ints({7, 8});
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, std::make_shared<DataTypeInt32>()));
    expect_same_storage(before, *column);
    EXPECT_EQ(column->use_count(), 1);
}

TEST_F(TabletColumnValidatorTest, NullableArrayStructReusesLeafStorage) {
    ColumnPtr field = ColumnNullable::create(ints({7, 8}), ColumnUInt8::create(2, 0));
    auto tuple = ColumnStruct::create(Columns {std::move(field), strings({"a", "b"})});
    auto array = ColumnArray::create(std::move(tuple), offsets({0, 2}));
    ColumnPtr column = ColumnNullable::create(std::move(array), ColumnUInt8::create(2, 0));
    auto tuple_type = std::make_shared<DataTypeStruct>(DataTypes {
            make_nullable(std::make_shared<DataTypeInt32>()), std::make_shared<DataTypeString>()});
    auto type = make_nullable(std::make_shared<DataTypeArray>(tuple_type));
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, type));
    expect_same_storage(before, *column);
    EXPECT_EQ(filter_map, (std::vector<char> {0, 0}));
}

TEST_F(TabletColumnValidatorTest, MapWithoutDuplicatesReusesLeafStorage) {
    ColumnPtr column = ColumnMap::create(ints({1, 2}), ints({10, 20}), offsets({2}));
    auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                              std::make_shared<DataTypeInt32>());
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, type));
    expect_same_storage(before, *column);
}

TEST_F(TabletColumnValidatorTest, StructReusesSharedReadOnlyFields) {
    ColumnPtr shared = ints({7});
    ColumnPtr column = ColumnStruct::create(Columns {shared, ints({8}), strings({"a"})});
    const auto& tuple_before = assert_cast<const ColumnStruct&>(*column);
    auto second_before = snapshot(tuple_before.get_column(1));
    auto third_before = snapshot(tuple_before.get_column(2));
    const auto* parent_before = column.get();
    const auto* shared_data = assert_cast<const ColumnInt32&>(*shared).get_data().data();
    auto type = std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeInt32>(),
                                                            std::make_shared<DataTypeInt32>(),
                                                            std::make_shared<DataTypeString>()});

    column = TEST_TRY(validate(column, type));
    const auto& tuple = assert_cast<const ColumnStruct&>(*column);
    EXPECT_NE(column.get(), parent_before);
    EXPECT_EQ(&tuple.get_column(0), shared.get());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(tuple.get_column(0)).get_data().data(), shared_data);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(tuple.get_column(0)).get_data()[0], 7);
    EXPECT_EQ(shared->use_count(), 2);
    expect_same_storage(second_before, tuple.get_column(1));
    expect_same_storage(third_before, tuple.get_column(2));
}

TEST_F(TabletColumnValidatorTest, DecimalV2PreservesLegacyScaleAndUniqueStorage) {
    auto decimals = ColumnDecimal128V2::create(0, 9);
    decimals->get_data().push_back(DecimalV2Value(std::string("1.235")).value());
    ColumnPtr column = std::move(decimals);
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, decimal_v2_type()));
    expect_same_storage(before, *column);
    EXPECT_EQ(assert_cast<const ColumnDecimal128V2&>(*column).get_data()[0].value(),
              DecimalV2Value(std::string("1.235")).value());
}

TEST_F(TabletColumnValidatorTest, DecimalV2PrecisionEqualsScalePreservesLegacyValue) {
    auto decimals = ColumnDecimal128V2::create(0, 9);
    decimals->get_data().push_back(DecimalV2Value(std::string("1.235")).value());
    ColumnPtr column = std::move(decimals);
    auto before = snapshot(*column);
    // DECIMAL(2,2) still uses DecimalV2 execution precision 27 and scale 9.
    auto type =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL, 2, 2);
    column = TEST_TRY(validate(column, type));
    expect_same_storage(before, *column);
    EXPECT_EQ(assert_cast<const ColumnDecimal128V2&>(*column).get_data()[0].value(),
              DecimalV2Value(std::string("1.235")).value());
}

TEST_F(TabletColumnValidatorTest, SharedNullableDecimalV2PreservesLegacyValue) {
    auto decimals = ColumnDecimal128V2::create(0, 9);
    decimals->get_data().push_back(DecimalV2Value(std::string("1.235")).value());
    ColumnPtr column = ColumnNullable::create(std::move(decimals), ColumnUInt8::create(1, 0));
    ColumnPtr original = column;
    auto before = snapshot(*original);
    column = TEST_TRY(validate(column, make_nullable(decimal_v2_type())));
    const auto& result = assert_cast<const ColumnNullable&>(*column);
    EXPECT_EQ(assert_cast<const ColumnDecimal128V2&>(result.get_nested_column())
                      .get_data()[0]
                      .value(),
              DecimalV2Value(std::string("1.235")).value());
    const auto& old = assert_cast<const ColumnNullable&>(*original);
    EXPECT_EQ(assert_cast<const ColumnDecimal128V2&>(old.get_nested_column()).get_data()[0].value(),
              DecimalV2Value(std::string("1.235")).value());
    expect_same_storage(before, *original);
}

TEST_F(TabletColumnValidatorTest, SharedMapColumnIsIsolated) {
    ColumnPtr column = ColumnMap::create(ints({7, 7}), ints({10, 20}), offsets({2}));
    ColumnPtr original = column;
    auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                              std::make_shared<DataTypeInt32>());
    column = TEST_TRY(validate(column, type));
    const auto& result = assert_cast<const ColumnMap&>(*column);
    const auto& old = assert_cast<const ColumnMap&>(*original);
    EXPECT_EQ(result.get_keys().size(), 1);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(result.get_values()).get_data()[0], 20);
    EXPECT_EQ(old.get_keys().size(), 2);
    EXPECT_EQ(old.get_values().size(), 2);
    EXPECT_EQ(old.get_offsets()[0], 2);
}

TEST_F(TabletColumnValidatorTest, StructInsideArrayReportsOriginalRow) {
    auto tuple = ColumnStruct::create(Columns {strings({""})});
    ColumnPtr column = ColumnArray::create(std::move(tuple), offsets({0, 1}));
    auto tuple_type =
            std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeJsonb>()});
    column = TEST_TRY(validate(column, std::make_shared<DataTypeArray>(tuple_type)));
    EXPECT_EQ(filter_map, (std::vector<char> {0, 1}));
    EXPECT_EQ(context.error_prefix.size(), 0);
}

TEST_F(TabletColumnValidatorTest, NullArraySkipsInvalidElementPayload) {
    auto array = ColumnArray::create(strings({"", ""}), offsets({1, 2}));
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(1);
    nulls->insert_value(0);
    ColumnPtr column = ColumnNullable::create(std::move(array), std::move(nulls));
    auto type = make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>()));
    column = TEST_TRY(validate(column, type));
    EXPECT_EQ(filter_map, (std::vector<char> {0, 1}));
}

TEST_F(TabletColumnValidatorTest, ParentAndChildNullMapsAreCombined) {
    ColumnPtr child = ColumnNullable::create(strings({"", "", ""}), ColumnUInt8::create(3, 0));
    auto& child_nulls =
            assert_cast<ColumnNullable&>(child->assert_mutable_ref()).get_null_map_data();
    child_nulls[1] = 1;
    auto tuple = ColumnStruct::create(Columns {std::move(child)});
    auto parent_nulls = ColumnUInt8::create();
    parent_nulls->insert_value(1);
    parent_nulls->insert_value(0);
    parent_nulls->insert_value(0);
    ColumnPtr column = ColumnNullable::create(std::move(tuple), std::move(parent_nulls));
    auto type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeJsonb>())}));
    column = TEST_TRY(validate(column, type));
    EXPECT_EQ(filter_map, (std::vector<char> {0, 0, 1}));
}

TEST_F(TabletColumnValidatorTest, NullParentSkipsNotNullChildViolation) {
    ColumnPtr child = ColumnNullable::create(ints({0}), ColumnUInt8::create(1, 1));
    auto tuple = ColumnStruct::create(Columns {std::move(child)});
    ColumnPtr column = ColumnNullable::create(std::move(tuple), ColumnUInt8::create(1, 1));
    auto type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeInt32>()}));
    column = TEST_TRY(validate(column, type));
    EXPECT_EQ(filter_map, (std::vector<char> {0}));
}

TEST_F(TabletColumnValidatorTest, NonStrictTruncationReplacesOnlyNestedString) {
    RuntimeState non_strict(options(false), TQueryGlobals {});
    context.state = &non_strict;
    ColumnPtr column = ColumnNullable::create(strings({"abcd"}), ColumnUInt8::create(1, 0));
    const auto* original_column = column.get();
    const auto& old = assert_cast<const ColumnNullable&>(*column);
    const auto* original_nulls = old.get_null_map_data().data();
    auto type = make_nullable(std::make_shared<DataTypeString>(2, TYPE_VARCHAR));
    column = TEST_TRY(validate(column, type));
    const auto& result = assert_cast<const ColumnNullable&>(*column);
    EXPECT_NE(column.get(), original_column);
    EXPECT_EQ(result.get_null_map_data().data(), original_nulls);
    EXPECT_EQ(result.get_nested_column().get_data_at(0).to_string(), "ab");
    EXPECT_EQ(filter_map, (std::vector<char> {0}));
}

TEST_F(TabletColumnValidatorTest, StrictStringLengthFailurePreservesStorage) {
    ColumnPtr column = strings({"abcd"});
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, std::make_shared<DataTypeString>(2, TYPE_VARCHAR)));
    expect_same_storage(before, *column);
    EXPECT_EQ(filter_map, (std::vector<char> {1}));
}

TEST_F(TabletColumnValidatorTest, EmptyNestedColumnsAreValid) {
    ColumnPtr column = ColumnArray::create(strings({}), offsets({0, 0}));
    auto before = snapshot(*column);
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>());
    column = TEST_TRY(validate(column, type));
    expect_same_storage(before, *column);
    EXPECT_EQ(filter_map, (std::vector<char> {0, 0}));
}

TEST_F(TabletColumnValidatorTest, NestedNullableMapReusesLeafStorage) {
    auto inner = ColumnMap::create(ints({3, 4}), ints({30, 40}), offsets({2}));
    auto nullable = ColumnNullable::create(std::move(inner), ColumnUInt8::create(1, 0));
    ColumnPtr column = ColumnMap::create(ints({1}), std::move(nullable), offsets({1}));
    auto inner_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                                    std::make_shared<DataTypeInt32>());
    auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                              make_nullable(inner_type));
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, type));
    expect_same_storage(before, *column);
}

TEST_F(TabletColumnValidatorTest, SharedNestedNullableMapDeduplicatesWithoutChangingSource) {
    auto inner = ColumnMap::create(ints({3, 3}), ints({30, 40}), offsets({2}));
    auto nullable = ColumnNullable::create(std::move(inner), ColumnUInt8::create(1, 0));
    ColumnPtr column = ColumnMap::create(ints({1}), std::move(nullable), offsets({1}));
    ColumnPtr original = column;
    auto inner_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                                    std::make_shared<DataTypeInt32>());
    auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                              make_nullable(inner_type));
    column = TEST_TRY(validate(column, type));
    const auto& result = assert_cast<const ColumnMap&>(*column);
    const auto& result_nullable = assert_cast<const ColumnNullable&>(result.get_values());
    const auto& result_inner = assert_cast<const ColumnMap&>(result_nullable.get_nested_column());
    EXPECT_EQ(result_inner.get_keys().size(), 1);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(result_inner.get_values()).get_data()[0], 40);
    const auto& old = assert_cast<const ColumnMap&>(*original);
    const auto& old_nullable = assert_cast<const ColumnNullable&>(old.get_values());
    EXPECT_EQ(assert_cast<const ColumnMap&>(old_nullable.get_nested_column()).get_keys().size(), 2);
}

TEST_F(TabletColumnValidatorTest, MapDeduplicationRebuildsElementRowMapping) {
    ColumnPtr column =
            ColumnMap::create(ints({1, 1, 2}), strings({"", "valid", ""}), offsets({2, 3}));
    auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(),
                                              std::make_shared<DataTypeJsonb>());
    column = TEST_TRY(validate(column, type));
    EXPECT_EQ(filter_map, (std::vector<char> {0, 1}));
    const auto& result = assert_cast<const ColumnMap&>(*column);
    EXPECT_EQ(result.get_offsets()[0], 1);
    EXPECT_EQ(result.get_offsets()[1], 2);
}

TEST_F(TabletColumnValidatorTest, NotNullRejectsNullRow) {
    ColumnPtr column = ColumnNullable::create(ints({0}), ColumnUInt8::create(1, 1));
    column = TEST_TRY(validate(column, std::make_shared<DataTypeInt32>()));
    EXPECT_EQ(filter_map, (std::vector<char> {1}));
}

TEST_F(TabletColumnValidatorTest, DecimalV3RangeChecksAllWidths) {
    const auto check = [this]<PrimitiveType T>() {
        using ColumnType = ColumnDecimal<T>;
        using Decimal = typename PrimitiveTypeTraits<T>::CppType;
        auto values = ColumnType::create(0, 0);
        values->get_data().push_back(Decimal(9));
        values->get_data().push_back(Decimal(10));
        values->get_data().push_back(Decimal(-10));
        ColumnPtr column = std::move(values);
        const auto* object = column.get();
        const auto* data = assert_cast<const ColumnType&>(*column).get_data().data();
        auto result = validate(column, std::make_shared<DataTypeDecimal<T>>(1, 0));
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result.value().get(), object);
        EXPECT_EQ(assert_cast<const ColumnType&>(*result.value()).get_data().data(), data);
        EXPECT_EQ(filter_map, (std::vector<char> {0, 1, 1}));
    };
    check.template operator()<TYPE_DECIMAL32>();
    check.template operator()<TYPE_DECIMAL64>();
    check.template operator()<TYPE_DECIMAL128I>();
    check.template operator()<TYPE_DECIMAL256>();
}

TEST_F(TabletColumnValidatorTest, NonNullableInputAcceptsNullableTargetType) {
    auto tuple = ColumnStruct::create(Columns {strings({"abc"})});
    ColumnPtr column = ColumnArray::create(std::move(tuple), offsets({1}));
    auto tuple_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeString>(2, TYPE_VARCHAR))}));
    auto type = make_nullable(std::make_shared<DataTypeArray>(tuple_type));
    auto before = snapshot(*column);
    column = TEST_TRY(validate(column, type));
    expect_same_storage(before, *column);
    EXPECT_EQ(filter_map, (std::vector<char> {1}));
}

TEST_F(TabletColumnValidatorTest, NestedReplacementBuildsShellsAndPreservesSource) {
    RuntimeState non_strict(options(false), TQueryGlobals {});
    context.state = &non_strict;
    auto nullable = ColumnNullable::create(strings({"abcd"}), ColumnUInt8::create(1, 0));
    auto tuple = ColumnStruct::create(Columns {std::move(nullable), ints({7})});
    ColumnPtr column = ColumnArray::create(std::move(tuple), offsets({1}));
    const auto& array_before = assert_cast<const ColumnArray&>(*column);
    const auto& tuple_before = assert_cast<const ColumnStruct&>(array_before.get_data());
    const auto& nullable_before = assert_cast<const ColumnNullable&>(tuple_before.get_column(0));
    ColumnPtr original = column;
    const auto* array_address = column.get();
    const auto* tuple_address = &tuple_before;
    const auto* nullable_address = &nullable_before;
    const auto* offsets_address = array_before.get_offsets().data();
    const auto* nulls_address = nullable_before.get_null_map_data().data();
    auto sibling_before = snapshot(tuple_before.get_column(1));
    auto tuple_type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeString>(2, TYPE_VARCHAR)),
                       std::make_shared<DataTypeInt32>()});

    column = TEST_TRY(validate(column, std::make_shared<DataTypeArray>(tuple_type)));

    const auto& array = assert_cast<const ColumnArray&>(*column);
    const auto& checked_tuple = assert_cast<const ColumnStruct&>(array.get_data());
    const auto& checked_nullable = assert_cast<const ColumnNullable&>(checked_tuple.get_column(0));
    EXPECT_NE(column.get(), array_address);
    EXPECT_NE(&checked_tuple, tuple_address);
    EXPECT_NE(&checked_nullable, nullable_address);
    EXPECT_EQ(array.get_offsets().data(), offsets_address);
    EXPECT_EQ(checked_nullable.get_null_map_data().data(), nulls_address);
    EXPECT_EQ(checked_nullable.get_nested_column().get_data_at(0).to_string(), "ab");
    EXPECT_EQ(checked_nullable.get_nested_column().use_count(), 1);
    expect_same_storage(sibling_before, checked_tuple.get_column(1));
    EXPECT_EQ(filter_map, (std::vector<char> {0}));
    EXPECT_EQ(nullable_before.get_nested_column().get_data_at(0).to_string(), "abcd");
    column->sanity_check();
}

TEST_F(TabletColumnValidatorTest, SharedReadOnlyLeavesKeepStorage) {
    const auto check = [this](ColumnPtr original, const DataTypePtr& type) {
        auto before = snapshot(*original);
        auto result = TEST_TRY(validate(original, type));
        EXPECT_EQ(result.get(), original.get());
        expect_same_storage(before, *result);
        EXPECT_EQ(original->use_count(), 2);
    };
    check(ints({7, 8}), std::make_shared<DataTypeInt32>());
    check(strings({"valid"}), std::make_shared<DataTypeJsonb>());
    auto values = ColumnDecimal32::create(0, 0);
    values->get_data().push_back(Decimal32(1));
    ColumnPtr original = std::move(values);
    const auto* data = assert_cast<const ColumnDecimal32&>(*original).get_data().data();
    auto result =
            TEST_TRY(validate(original, std::make_shared<DataTypeDecimal<TYPE_DECIMAL32>>(1, 0)));
    EXPECT_EQ(result.get(), original.get());
    EXPECT_EQ(assert_cast<const ColumnDecimal32&>(*result).get_data().data(), data);
}

TEST_F(TabletColumnValidatorTest, SharedComplexTreeReusesReadOnlyLeaves) {
    auto tuple = ColumnStruct::create(Columns {ints({7}), strings({"abc"})});
    ColumnPtr original = ColumnNullable::create(ColumnArray::create(std::move(tuple), offsets({1})),
                                                ColumnUInt8::create(1, 0));
    auto tuple_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()});
    auto before = snapshot(*original);
    auto result = TEST_TRY(
            validate(original, make_nullable(std::make_shared<DataTypeArray>(tuple_type))));
    EXPECT_NE(result.get(), original.get());
    expect_same_storage(before, *result);
    expect_same_storage(before, *original);
}

} // namespace doris
