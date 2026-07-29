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

} // namespace doris::format::parquet
