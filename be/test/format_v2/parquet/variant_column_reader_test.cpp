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

#include "format_v2/parquet/reader/variant_column_reader.h"

#include <gtest/gtest.h>

#include <array>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function_variant_element_v2.h"
#include "format_v2/parquet/parquet_column_schema.h"

namespace doris::format::parquet {
namespace {

MutableColumnPtr nullable_strings(const std::vector<StringRef>& values,
                                  const std::vector<uint8_t>& nulls) {
    auto data = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (size_t row = 0; row < values.size(); ++row) {
        data->insert_data(values[row].data, values[row].size);
        null_map->get_data().push_back(nulls[row]);
    }
    return ColumnNullable::create(std::move(data), std::move(null_map));
}

ParquetColumnSchema unshredded_schema() {
    ParquetColumnSchema schema;
    schema.name = "payload";
    schema.kind = ParquetColumnSchemaKind::VARIANT;
    schema.type = make_nullable(std::make_shared<DataTypeVariantV2>());
    const auto binary = make_nullable(std::make_shared<DataTypeString>());
    schema.variant_physical_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {binary, binary}, Strings {"metadata", "value"}));

    auto metadata = std::make_unique<ParquetColumnSchema>();
    metadata->name = "metadata";
    metadata->kind = ParquetColumnSchemaKind::PRIMITIVE;
    metadata->type = binary;
    auto value = std::make_unique<ParquetColumnSchema>();
    value->name = "value";
    value->kind = ParquetColumnSchemaKind::PRIMITIVE;
    value->type = binary;
    schema.children.push_back(std::move(metadata));
    schema.children.push_back(std::move(value));
    return schema;
}

ParquetColumnSchema shredded_int64_schema() {
    auto schema = unshredded_schema();
    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::PRIMITIVE;
    typed->type = make_nullable(std::make_shared<DataTypeInt64>());
    typed->type_descriptor.integer_bit_width = 64;
    schema.children.push_back(std::move(typed));
    const auto binary = make_nullable(std::make_shared<DataTypeString>());
    schema.variant_physical_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {binary, binary, make_nullable(std::make_shared<DataTypeInt64>())},
            Strings {"metadata", "value", "typed_value"}));
    return schema;
}

ParquetColumnSchema shredded_object_schema() {
    auto schema = unshredded_schema();
    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::STRUCT;

    auto field = std::make_unique<ParquetColumnSchema>();
    field->name = "a";
    field->kind = ParquetColumnSchemaKind::STRUCT;
    auto field_typed = std::make_unique<ParquetColumnSchema>();
    field_typed->name = "typed_value";
    field_typed->kind = ParquetColumnSchemaKind::PRIMITIVE;
    field_typed->type = make_nullable(std::make_shared<DataTypeInt64>());
    field_typed->type_descriptor.integer_bit_width = 64;
    field->children.push_back(std::move(field_typed));
    typed->children.push_back(std::move(field));
    schema.children.push_back(std::move(typed));
    return schema;
}

ParquetColumnSchema shredded_binary_object_schema() {
    auto schema = shredded_object_schema();
    auto* leaf = schema.children.back()->children[0]->children[0].get();
    leaf->type = make_nullable(std::make_shared<DataTypeString>());
    return schema;
}

ParquetColumnSchema shredded_array_schema() {
    auto schema = unshredded_schema();
    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::LIST;
    auto element = std::make_unique<ParquetColumnSchema>();
    element->name = "element";
    element->kind = ParquetColumnSchemaKind::STRUCT;
    auto element_typed = std::make_unique<ParquetColumnSchema>();
    element_typed->name = "typed_value";
    element_typed->kind = ParquetColumnSchemaKind::PRIMITIVE;
    element_typed->type = make_nullable(std::make_shared<DataTypeInt64>());
    element_typed->type_descriptor.integer_bit_width = 64;
    element->children.push_back(std::move(element_typed));
    typed->children.push_back(std::move(element));
    schema.children.push_back(std::move(typed));
    return schema;
}

MutableColumnPtr shredded_int64_physical(const std::vector<int64_t>& values) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    std::vector<StringRef> metadata_rows(values.size(), metadata);
    std::vector<StringRef> empty_values(values.size(), {ignored.data(), 0});
    std::vector<uint8_t> present(values.size(), 0);
    std::vector<uint8_t> absent(values.size(), 1);
    MutableColumns fields;
    fields.push_back(nullable_strings(metadata_rows, present));
    fields.push_back(nullable_strings(empty_values, absent));
    auto integers = ColumnInt64::create();
    integers->get_data().assign(values.begin(), values.end());
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().resize_fill(values.size(), 0);
    fields.push_back(ColumnNullable::create(std::move(integers), std::move(integer_nulls)));
    auto structure = ColumnStruct::create(std::move(fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().resize_fill(values.size(), 0);
    return ColumnNullable::create(std::move(structure), std::move(root_nulls));
}

MutableColumnPtr projected_shredded_object_physical(const std::vector<int64_t>& values,
                                                    const IColumn** decoded_leaf = nullptr) {
    auto integers = ColumnInt64::create();
    integers->get_data().assign(values.begin(), values.end());
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().resize_fill(values.size(), 0);
    MutableColumnPtr leaf = ColumnNullable::create(std::move(integers), std::move(integer_nulls));
    if (decoded_leaf != nullptr) {
        *decoded_leaf = leaf.get();
    }

    MutableColumns wrapper_fields;
    wrapper_fields.push_back(std::move(leaf));
    auto wrapper = ColumnStruct::create(std::move(wrapper_fields));
    MutableColumns object_fields;
    object_fields.push_back(
            ColumnNullable::create(std::move(wrapper), ColumnUInt8::create(values.size(), 0)));
    auto object = ColumnStruct::create(std::move(object_fields));
    MutableColumns root_fields;
    root_fields.push_back(
            ColumnNullable::create(std::move(object), ColumnUInt8::create(values.size(), 0)));
    auto root = ColumnStruct::create(std::move(root_fields));
    return ColumnNullable::create(std::move(root), ColumnUInt8::create(values.size(), 0));
}

MutableColumnPtr projected_two_field_object_physical(const std::vector<int64_t>& first,
                                                     const std::vector<int64_t>& second) {
    DORIS_CHECK(first.size() == second.size());
    auto wrapper = [](const std::vector<int64_t>& values) {
        auto integers = ColumnInt64::create();
        integers->get_data().assign(values.begin(), values.end());
        auto leaf =
                ColumnNullable::create(std::move(integers), ColumnUInt8::create(values.size(), 0));
        MutableColumns fields;
        fields.push_back(std::move(leaf));
        return ColumnNullable::create(ColumnStruct::create(std::move(fields)),
                                      ColumnUInt8::create(values.size(), 0));
    };

    MutableColumns object_fields;
    object_fields.push_back(wrapper(first));
    object_fields.push_back(wrapper(second));
    auto object = ColumnStruct::create(std::move(object_fields));
    MutableColumns root_fields;
    root_fields.push_back(
            ColumnNullable::create(std::move(object), ColumnUInt8::create(first.size(), 0)));
    auto root = ColumnStruct::create(std::move(root_fields));
    return ColumnNullable::create(std::move(root), ColumnUInt8::create(first.size(), 0));
}

MutableColumnPtr projected_wide_object_physical(size_t field_count, int64_t value) {
    MutableColumns object_fields;
    object_fields.reserve(field_count);
    for (size_t field = 0; field < field_count; ++field) {
        auto integers = ColumnInt64::create();
        integers->insert_value(value + field);
        MutableColumns wrapper_fields;
        wrapper_fields.push_back(
                ColumnNullable::create(std::move(integers), ColumnUInt8::create(1, 0)));
        object_fields.push_back(ColumnNullable::create(
                ColumnStruct::create(std::move(wrapper_fields)), ColumnUInt8::create(1, 0)));
    }
    MutableColumns root_fields;
    root_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(object_fields)),
                                                 ColumnUInt8::create(1, 0)));
    return ColumnNullable::create(ColumnStruct::create(std::move(root_fields)),
                                  ColumnUInt8::create(1, 0));
}

} // namespace

TEST(VariantColumnReaderTest, UnshreddedRowsPreserveSqlNullAndVariantNull) {
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns fields;
    fields.push_back(nullable_strings({metadata, metadata, metadata}, {0, 0, 0}));
    fields.push_back(nullable_strings(
            {{int_seven.data(), int_seven.size()}, {ignored.data(), 0}, {ignored.data(), 0}},
            {0, 1, 1}));
    auto physical_struct = ColumnStruct::create(std::move(fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().assign({0, 1, 0});
    auto physical = ColumnNullable::create(std::move(physical_struct), std::move(root_nulls));

    auto output_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    auto output = output_type->create_column();
    const auto status = materialize_variant_rows(unshredded_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output->size(), 3);

    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 1, 0}));
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_TRUE(variants.is_shredded());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 7);
    EXPECT_TRUE(variants.get_value_ref(2).is_null());
}

TEST(VariantColumnReaderTest, RequiredPhysicalGroupAppendsToNullableExternalSlot) {
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns fields;
    fields.push_back(nullable_strings({metadata}, {0}));
    fields.push_back(nullable_strings({{int_seven.data(), int_seven.size()}}, {0}));
    auto physical = ColumnStruct::create(std::move(fields));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(unshredded_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0}));
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 7);
}

TEST(VariantColumnReaderTest, ShreddedIntegerKeepsDeclaredPhysicalWidth) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns fields;
    fields.push_back(nullable_strings({metadata}, {0}));
    fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    auto integers = ColumnInt64::create();
    integers->get_data().push_back(42);
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().push_back(0);
    fields.push_back(ColumnNullable::create(std::move(integers), std::move(integer_nulls)));
    auto structure = ColumnStruct::create(std::move(fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().push_back(0);
    auto physical = ColumnNullable::create(std::move(structure), std::move(root_nulls));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(shredded_int64_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 42);
    EXPECT_EQ(variants.get_value_ref(0).primitive_id(), VariantPrimitiveId::INT64);
}

TEST(VariantColumnReaderTest, DifferentMetadataDictionariesRemainIndependent) {
    VariantBatchBuilder first_builder;
    auto first_row = first_builder.begin_row();
    auto first_object = first_row.start_object();
    first_object.add_key(StringRef("alpha"));
    first_row.add_int(1);
    first_object.finish();
    first_row.finish();
    auto first = first_builder.finish_batch();

    VariantBatchBuilder second_builder;
    auto second_row = second_builder.begin_row();
    auto second_object = second_row.start_object();
    second_object.add_key(StringRef("beta"));
    second_row.add_int(2);
    second_object.finish();
    second_row.finish();
    auto second = second_builder.finish_batch();

    const VariantRef first_value = first.value_at(0);
    const VariantRef second_value = second.value_at(0);
    MutableColumns fields;
    fields.push_back(nullable_strings({{first_value.metadata.data, first_value.metadata.size},
                                       {second_value.metadata.data, second_value.metadata.size}},
                                      {0, 0}));
    fields.push_back(nullable_strings({{first_value.value.data, first_value.value.size},
                                       {second_value.value.data, second_value.value.size}},
                                      {0, 0}));
    auto structure = ColumnStruct::create(std::move(fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().assign({0, 0});
    auto physical = ColumnNullable::create(std::move(structure), std::move(root_nulls));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(unshredded_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    VariantRef field;
    ASSERT_TRUE(variants.get_value_ref(0).object_find(StringRef("alpha"), &field));
    EXPECT_EQ(field.get_int(), 1);
    ASSERT_TRUE(variants.get_value_ref(1).object_find(StringRef("beta"), &field));
    EXPECT_EQ(field.get_int(), 2);
}

TEST(VariantColumnReaderTest, ShreddedObjectFieldMayOmitResidualValueColumn) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());

    auto integer = ColumnInt64::create();
    integer->get_data().push_back(9);
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().push_back(0);
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(ColumnNullable::create(std::move(integer), std::move(integer_nulls)));
    auto wrapper = ColumnStruct::create(std::move(wrapper_fields));
    auto wrapper_nulls = ColumnUInt8::create();
    wrapper_nulls->get_data().push_back(0);
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(std::move(wrapper), std::move(wrapper_nulls)));
    auto object = ColumnStruct::create(std::move(object_fields));
    auto object_nulls = ColumnUInt8::create();
    object_nulls->get_data().push_back(0);

    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata}, {0}));
    root_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    root_fields.push_back(ColumnNullable::create(std::move(object), std::move(object_nulls)));
    auto root = ColumnStruct::create(std::move(root_fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().push_back(0);
    auto physical = ColumnNullable::create(std::move(root), std::move(root_nulls));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(shredded_object_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    VariantRef field;
    ASSERT_TRUE(variants.get_value_ref(0).object_find(StringRef("a"), &field));
    EXPECT_EQ(field.get_int(), 9);
    EXPECT_EQ(field.primitive_id(), VariantPrimitiveId::INT64);
}

TEST(VariantColumnReaderTest, ShreddedTypedPathReusesDecodedLeafColumn) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());

    auto integers = ColumnInt64::create();
    integers->get_data().push_back(9);
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().push_back(0);
    MutableColumnPtr typed_leaf =
            ColumnNullable::create(std::move(integers), std::move(integer_nulls));
    const IColumn* const decoded_typed_leaf = typed_leaf.get();

    MutableColumns wrapper_fields;
    wrapper_fields.push_back(std::move(typed_leaf));
    auto wrapper = ColumnStruct::create(std::move(wrapper_fields));
    auto wrapper_nulls = ColumnUInt8::create();
    wrapper_nulls->get_data().push_back(0);
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(std::move(wrapper), std::move(wrapper_nulls)));
    auto object = ColumnStruct::create(std::move(object_fields));
    auto object_nulls = ColumnUInt8::create();
    object_nulls->get_data().push_back(0);

    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata}, {0}));
    root_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    root_fields.push_back(ColumnNullable::create(std::move(object), std::move(object_nulls)));
    auto root = ColumnStruct::create(std::move(root_fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().push_back(0);
    auto physical = ColumnNullable::create(std::move(root), std::move(root_nulls));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(shredded_object_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    ASSERT_TRUE(variants.is_shredded());

    const std::array shredded_path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    const auto match = variants.find_shredded_typed_value(shredded_path);
    ASSERT_TRUE(match.has_value());
    EXPECT_EQ(match->column.get(), decoded_typed_leaf);

    const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &path).ok());
    ColumnPtr extracted;
    ASSERT_TRUE(
            extract_variant_element_v2(variants, *path, nullable.get_null_map_data(), &extracted)
                    .ok());

    const auto& extracted_nullable = assert_cast<const ColumnNullable&>(*extracted);
    const auto& extracted_variant =
            assert_cast<const ColumnVariantV2&>(extracted_nullable.get_nested_column());
    ASSERT_TRUE(extracted_variant.is_typed());
    EXPECT_EQ(&extracted_variant.typed_column(), decoded_typed_leaf);
    const auto& extracted_typed =
            assert_cast<const ColumnNullable&>(extracted_variant.typed_column());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(extracted_typed.get_nested_column()).get_data()[0],
              9);
    EXPECT_TRUE(variants.is_shredded());
}

TEST(VariantColumnReaderTest, AppendsProjectedShreddedBatchesWithoutMaterializing) {
    auto schema = shredded_object_schema();
    schema.local_id = 0;
    schema.children[0]->local_id = 0;
    schema.children[1]->local_id = 1;
    schema.children[2]->local_id = 2;
    schema.children[2]->children[0]->local_id = 0;
    schema.children[2]->children[0]->children[0]->local_id = 0;

    auto projection = format::LocalColumnIndex::partial_local(schema.local_id);
    projection.children.push_back(
            format::LocalColumnIndex::partial_local(schema.children[2]->local_id));
    projection.children.back().children.push_back(
            format::LocalColumnIndex::partial_local(schema.children[2]->children[0]->local_id));
    projection.children.back().children.back().children.push_back(format::LocalColumnIndex::local(
            schema.children[2]->children[0]->children[0]->local_id));
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);
    ASSERT_EQ(plan.variant_state_schema.use_count(), 1);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const IColumn* first_decoded_leaf = nullptr;
    ASSERT_TRUE(
            materialize_variant_columns(
                    plan, projected_shredded_object_physical({10, 20}, &first_decoded_leaf), output)
                    .ok());
    EXPECT_EQ(plan.variant_state_schema.use_count(), 2);
    const auto append_status =
            materialize_variant_columns(plan, projected_shredded_object_physical({30}), output);
    ASSERT_TRUE(append_status.ok()) << append_status;

    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    ASSERT_TRUE(variants.is_shredded());
    ASSERT_EQ(variants.size(), 3);
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    const auto match = variants.find_shredded_typed_value(path);
    ASSERT_TRUE(match.has_value());
    EXPECT_EQ(match->column.get(), first_decoded_leaf);
    const auto& values = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(*match->column).get_nested_column());
    EXPECT_EQ(values.get_data(), ColumnInt64::Container({10, 20, 30}));

    IColumn::Filter filter {1, 0, 1};
    ColumnPtr filtered = output->filter(filter, 2);
    EXPECT_EQ(filtered->size(), 2);
    EXPECT_EQ(plan.variant_state_schema.use_count(), 3);
}

TEST(VariantColumnReaderTest, WideProjectionSharesSchemaAcrossBatchesAndSelections) {
    constexpr size_t width = 64;
    constexpr size_t batch_count = 16;
    auto schema = unshredded_schema();
    schema.local_id = 0;
    schema.children[0]->local_id = 0;
    schema.children[1]->local_id = 1;

    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::STRUCT;
    typed->local_id = 2;
    auto projection = format::LocalColumnIndex::partial_local(schema.local_id);
    projection.children.push_back(format::LocalColumnIndex::partial_local(typed->local_id));
    for (size_t field = 0; field < width; ++field) {
        auto wrapper = std::make_unique<ParquetColumnSchema>();
        wrapper->name = "field_" + std::to_string(field);
        wrapper->kind = ParquetColumnSchemaKind::STRUCT;
        wrapper->local_id = static_cast<int>(field);
        auto leaf = std::make_unique<ParquetColumnSchema>();
        leaf->name = "typed_value";
        leaf->kind = ParquetColumnSchemaKind::PRIMITIVE;
        leaf->local_id = 0;
        leaf->type = make_nullable(std::make_shared<DataTypeInt64>());
        leaf->type_descriptor.integer_bit_width = 64;
        wrapper->children.push_back(std::move(leaf));
        typed->children.push_back(std::move(wrapper));

        auto wrapper_projection = format::LocalColumnIndex::partial_local(static_cast<int>(field));
        wrapper_projection.children.push_back(format::LocalColumnIndex::local(0));
        projection.children.back().children.push_back(std::move(wrapper_projection));
    }
    schema.children.push_back(std::move(typed));

    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    for (size_t batch = 0; batch < batch_count; ++batch) {
        ASSERT_TRUE(materialize_variant_columns(
                            plan, projected_wide_object_physical(width, batch * width), output)
                            .ok());
    }
    ASSERT_EQ(output->size(), batch_count);
    ASSERT_EQ(plan.variant_state_schema.use_count(), 2);

    // Holding derived slices makes schema ownership observable: every state must retain the same
    // reader-scoped schema instead of allocating a width-sized clone for each row selection.
    std::vector<ColumnPtr> slices;
    slices.reserve(batch_count);
    for (size_t row = 0; row < batch_count; ++row) {
        slices.push_back(output->cut(row, 1));
    }
    EXPECT_EQ(plan.variant_state_schema.use_count(), 2 + batch_count);
}

TEST(VariantColumnReaderTest, RetainedSchemaFollowsDecodedProjectionOrder) {
    auto schema = unshredded_schema();
    schema.local_id = 0;
    schema.children[0]->local_id = 0;
    schema.children[1]->local_id = 1;

    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::STRUCT;
    typed->local_id = 2;
    auto make_wrapper = [](std::string name, int local_id) {
        auto wrapper = std::make_unique<ParquetColumnSchema>();
        wrapper->name = std::move(name);
        wrapper->kind = ParquetColumnSchemaKind::STRUCT;
        wrapper->local_id = local_id;
        auto leaf = std::make_unique<ParquetColumnSchema>();
        leaf->name = "typed_value";
        leaf->kind = ParquetColumnSchemaKind::PRIMITIVE;
        leaf->local_id = 0;
        leaf->type = make_nullable(std::make_shared<DataTypeInt64>());
        leaf->type_descriptor.integer_bit_width = 64;
        wrapper->children.push_back(std::move(leaf));
        return wrapper;
    };
    typed->children.push_back(make_wrapper("z", 0));
    typed->children.push_back(make_wrapper("a", 1));
    schema.children.push_back(std::move(typed));

    auto projection = format::LocalColumnIndex::partial_local(schema.local_id);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    for (int local_id : {1, 0}) {
        projection.children.back().children.push_back(
                format::LocalColumnIndex::partial_local(local_id));
        projection.children.back().children.back().children.push_back(
                format::LocalColumnIndex::local(0));
    }
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(plan, projected_two_field_object_physical({11}, {22}),
                                            output)
                        .ok());
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    const std::array a_path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    const std::array z_path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("z")}};
    const auto a = variants.find_shredded_typed_value(a_path);
    const auto z = variants.find_shredded_typed_value(z_path);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(z.has_value());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(
                      assert_cast<const ColumnNullable&>(*a->column).get_nested_column())
                      .get_data()[0],
              11);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(
                      assert_cast<const ColumnNullable&>(*z->column).get_nested_column())
                      .get_data()[0],
              22);
}

TEST(VariantColumnReaderTest, AmbiguousTypedIdentityRequiresCanonicalMaterialization) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(nullable_strings({StringRef("abc")}, {0}));
    auto wrapper = ColumnStruct::create(std::move(wrapper_fields));
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(std::move(wrapper), ColumnUInt8::create(1, 0)));
    auto object = ColumnStruct::create(std::move(object_fields));
    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata}, {0}));
    root_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    root_fields.push_back(ColumnNullable::create(std::move(object), ColumnUInt8::create(1, 0)));
    auto root = ColumnStruct::create(std::move(root_fields));
    auto physical = ColumnNullable::create(std::move(root), ColumnUInt8::create(1, 0));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_binary_object_schema(), *physical, output).ok());
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    EXPECT_FALSE(variants.find_shredded_typed_value(path).has_value());
    VariantRef field;
    ASSERT_TRUE(variants.get_value_ref(0).object_find(StringRef("a"), &field));
    EXPECT_EQ(field.get_binary(), StringRef("abc"));
}

TEST(VariantColumnReaderTest, MaterializedCacheParticipatesInMemoryAccounting) {
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_int64_schema(), shredded_int64_physical({42, 43}),
                                         output)
                        .ok());
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    const size_t physical_bytes = variants.byte_size();
    const size_t physical_allocated = variants.allocated_bytes();

    EXPECT_EQ(variants.get_value_ref(0).get_int(), 42);
    EXPECT_GT(variants.byte_size(), physical_bytes);
    EXPECT_GT(variants.allocated_bytes(), physical_allocated);
}

TEST(VariantColumnReaderTest, MaterializedShreddedCopiesDetachBeforeMutation) {
    auto first_output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_int64_schema(), shredded_int64_physical({10, 20}),
                                         first_output)
                        .ok());
    const auto& first = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*first_output).get_nested_column());
    EXPECT_EQ(first.get_value_ref(0).get_int(), 10);

    auto cloned = first.clone_resized(first.size());
    EXPECT_NO_THROW(cloned->pop_back(1));
    ASSERT_EQ(cloned->size(), 1);
    EXPECT_EQ(assert_cast<const ColumnVariantV2&>(*cloned).get_value_ref(0).get_int(), 10);

    auto second_output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_int64_schema(), shredded_int64_physical({30}),
                                         second_output)
                        .ok());
    const auto& second = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*second_output).get_nested_column());
    auto appended = ColumnVariantV2::create();
    appended->insert_range_from(first, 0, first.size());
    EXPECT_NO_THROW(appended->insert_range_from(second, 0, second.size()));
    ASSERT_EQ(appended->size(), 3);
    EXPECT_EQ(appended->get_value_ref(2).get_int(), 30);
}

TEST(VariantColumnReaderTest, MissingShreddedObjectWrapperMeansAbsentField) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());

    auto integer = ColumnInt64::create();
    integer->get_data().push_back(0);
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().push_back(1);
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(ColumnNullable::create(std::move(integer), std::move(integer_nulls)));
    auto wrapper = ColumnStruct::create(std::move(wrapper_fields));
    auto wrapper_nulls = ColumnUInt8::create();
    wrapper_nulls->get_data().push_back(1);
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(std::move(wrapper), std::move(wrapper_nulls)));
    auto object = ColumnStruct::create(std::move(object_fields));
    auto object_nulls = ColumnUInt8::create();
    object_nulls->get_data().push_back(0);

    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata}, {0}));
    root_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    root_fields.push_back(ColumnNullable::create(std::move(object), std::move(object_nulls)));
    auto root = ColumnStruct::create(std::move(root_fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().push_back(0);
    auto physical = ColumnNullable::create(std::move(root), std::move(root_nulls));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(shredded_object_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).num_elements(), 0);
}

TEST(VariantColumnReaderTest, MaterializesShreddedArrayElements) {
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());

    auto integers = ColumnInt64::create();
    integers->get_data().assign({3, 4});
    auto integer_nulls = ColumnUInt8::create();
    integer_nulls->get_data().assign({0, 0});
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(ColumnNullable::create(std::move(integers), std::move(integer_nulls)));
    auto wrappers = ColumnStruct::create(std::move(wrapper_fields));
    auto wrapper_nulls = ColumnUInt8::create();
    wrapper_nulls->get_data().assign({0, 0});
    auto elements = ColumnNullable::create(std::move(wrappers), std::move(wrapper_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto array = ColumnArray::create(std::move(elements), std::move(offsets));
    auto array_nulls = ColumnUInt8::create();
    array_nulls->get_data().push_back(0);

    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata}, {0}));
    root_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    root_fields.push_back(ColumnNullable::create(std::move(array), std::move(array_nulls)));
    auto root = ColumnStruct::create(std::move(root_fields));
    auto root_nulls = ColumnUInt8::create();
    root_nulls->get_data().push_back(0);
    auto physical = ColumnNullable::create(std::move(root), std::move(root_nulls));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status = materialize_variant_rows(shredded_array_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const VariantRef value = variants.get_value_ref(0);
    ASSERT_EQ(value.num_elements(), 2);
    EXPECT_EQ(value.array_at(0).get_int(), 3);
    EXPECT_EQ(value.array_at(1).get_int(), 4);
}

TEST(VariantColumnReaderTest, MaterializesVariantNestedInStruct) {
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns variant_fields;
    variant_fields.push_back(nullable_strings({metadata}, {0}));
    variant_fields.push_back(nullable_strings({{int_seven.data(), int_seven.size()}}, {0}));
    auto physical_variant = ColumnStruct::create(std::move(variant_fields));
    auto variant_nulls = ColumnUInt8::create();
    variant_nulls->get_data().push_back(0);
    MutableColumns root_fields;
    root_fields.push_back(
            ColumnNullable::create(std::move(physical_variant), std::move(variant_nulls)));
    auto physical = ColumnStruct::create(std::move(root_fields));

    ParquetColumnSchema root_schema;
    root_schema.name = "root";
    root_schema.kind = ParquetColumnSchemaKind::STRUCT;
    root_schema.children.push_back(std::make_unique<ParquetColumnSchema>(unshredded_schema()));
    VariantMaterializationNode plan;
    plan.schema = &root_schema;
    plan.contains_variant = true;
    auto child_plan = std::make_unique<VariantMaterializationNode>();
    child_plan->schema = root_schema.children[0].get();
    child_plan->contains_variant = true;
    plan.children.push_back(std::move(child_plan));

    auto output = std::make_shared<DataTypeStruct>(
                          DataTypes {make_nullable(std::make_shared<DataTypeVariantV2>())},
                          Strings {"payload"})
                          ->create_column();
    const auto status = materialize_variant_columns(plan, *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& output_struct = assert_cast<const ColumnStruct&>(*output);
    const auto& nullable = assert_cast<const ColumnNullable&>(output_struct.get_column(0));
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 7);
}

TEST(VariantColumnReaderTest, AlignsNestedPrimitiveNullabilityAroundVariant) {
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());

    MutableColumns physical_fields;
    physical_fields.push_back(nullable_strings({StringRef("required")}, {0}));
    MutableColumns variant_fields;
    variant_fields.push_back(nullable_strings({metadata}, {0}));
    variant_fields.push_back(nullable_strings({{int_seven.data(), int_seven.size()}}, {0}));
    auto physical_variant = ColumnStruct::create(std::move(variant_fields));
    auto variant_nulls = ColumnUInt8::create();
    variant_nulls->get_data().push_back(0);
    physical_fields.push_back(
            ColumnNullable::create(std::move(physical_variant), std::move(variant_nulls)));
    auto physical = ColumnStruct::create(std::move(physical_fields));

    ParquetColumnSchema root_schema;
    root_schema.name = "root";
    root_schema.kind = ParquetColumnSchemaKind::STRUCT;
    auto label_schema = std::make_unique<ParquetColumnSchema>();
    label_schema->name = "label";
    label_schema->kind = ParquetColumnSchemaKind::PRIMITIVE;
    label_schema->type = make_nullable(std::make_shared<DataTypeString>());
    root_schema.children.push_back(std::move(label_schema));
    root_schema.children.push_back(std::make_unique<ParquetColumnSchema>(unshredded_schema()));

    VariantMaterializationNode plan;
    plan.schema = &root_schema;
    plan.contains_variant = true;
    for (const auto& child_schema : root_schema.children) {
        auto child_plan = std::make_unique<VariantMaterializationNode>();
        child_plan->schema = child_schema.get();
        child_plan->contains_variant = child_schema->kind == ParquetColumnSchemaKind::VARIANT;
        plan.children.push_back(std::move(child_plan));
    }

    auto output = std::make_shared<DataTypeStruct>(
                          DataTypes {std::make_shared<DataTypeString>(),
                                     make_nullable(std::make_shared<DataTypeVariantV2>())},
                          Strings {"label", "payload"})
                          ->create_column();
    const auto status = materialize_variant_columns(plan, *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& output_struct = assert_cast<const ColumnStruct&>(*output);
    EXPECT_EQ(output_struct.get_column(0).get_data_at(0).to_string(), "required");
    const auto& nullable = assert_cast<const ColumnNullable&>(output_struct.get_column(1));
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 7);
}

TEST(VariantColumnReaderTest, NestedMaterializationMovesUnaffectedSiblingBuffers) {
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());

    auto label = ColumnString::create();
    label->insert_data("large-sibling", 13);
    const IColumn* decoded_label = label.get();
    MutableColumns variant_fields;
    variant_fields.push_back(nullable_strings({metadata}, {0}));
    variant_fields.push_back(nullable_strings({{int_seven.data(), int_seven.size()}}, {0}));
    auto physical_variant = ColumnStruct::create(std::move(variant_fields));
    auto variant_nulls = ColumnUInt8::create();
    variant_nulls->get_data().push_back(0);
    MutableColumns root_fields;
    root_fields.push_back(std::move(label));
    root_fields.push_back(
            ColumnNullable::create(std::move(physical_variant), std::move(variant_nulls)));
    ColumnPtr physical = ColumnStruct::create(std::move(root_fields));

    ParquetColumnSchema root_schema;
    root_schema.name = "root";
    root_schema.kind = ParquetColumnSchemaKind::STRUCT;
    auto label_schema = std::make_unique<ParquetColumnSchema>();
    label_schema->name = "label";
    label_schema->kind = ParquetColumnSchemaKind::PRIMITIVE;
    label_schema->type = std::make_shared<DataTypeString>();
    root_schema.children.push_back(std::move(label_schema));
    root_schema.children.push_back(std::make_unique<ParquetColumnSchema>(unshredded_schema()));

    VariantMaterializationNode plan;
    plan.schema = &root_schema;
    plan.contains_variant = true;
    for (const auto& child_schema : root_schema.children) {
        auto child_plan = std::make_unique<VariantMaterializationNode>();
        child_plan->schema = child_schema.get();
        child_plan->contains_variant = child_schema->kind == ParquetColumnSchemaKind::VARIANT;
        plan.children.push_back(std::move(child_plan));
    }

    auto output = std::make_shared<DataTypeStruct>(
                          DataTypes {std::make_shared<DataTypeString>(),
                                     make_nullable(std::make_shared<DataTypeVariantV2>())},
                          Strings {"label", "payload"})
                          ->create_column();
    const IColumn* empty_output = output.get();
    ASSERT_TRUE(materialize_variant_columns(plan, std::move(physical), output).ok());
    EXPECT_NE(output.get(), empty_output);
    const auto& output_struct = assert_cast<const ColumnStruct&>(*output);
    EXPECT_EQ(&output_struct.get_column(0), decoded_label);
}

} // namespace doris::format::parquet
