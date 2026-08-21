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
#include <cmath>
#include <functional>
#include <initializer_list>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/timestamptz_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function_variant_element_v2.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_profile.h"
#include "runtime/runtime_profile.h"

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

Status extract_object_key(const ColumnVariantV2& source, const ColumnNullable& nullable,
                          StringRef key, ColumnPtr* result) {
    const std::array segments {VariantElementV2PathSegment::object_key(key)};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    RETURN_IF_ERROR(resolve_variant_element_v2_path(segments, &path));
    const auto& null_map = nullable.get_null_map_data();
    return extract_variant_element_v2(
            source, *path, std::span<const uint8_t>(null_map.data(), null_map.size()), result);
}

std::string single_key_metadata_bytes(std::string_view key) {
    EXPECT_LE(key.size(), std::numeric_limits<uint8_t>::max());
    std::string metadata;
    metadata.push_back(
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK));
    metadata.push_back(1);
    metadata.push_back(0);
    metadata.push_back(static_cast<char>(key.size()));
    metadata.append(key);
    return metadata;
}

ParquetColumnSchema unshredded_schema() {
    ParquetColumnSchema schema;
    schema.name = "payload";
    schema.kind = ParquetColumnSchemaKind::VARIANT;
    schema.contains_variant = true;
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

ParquetColumnSchema shredded_primitive_schema(DataTypePtr type) {
    auto schema = unshredded_schema();
    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::PRIMITIVE;
    typed->type = make_nullable(std::move(type));
    schema.children.push_back(std::move(typed));
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

ParquetColumnSchema shredded_named_object_schema(std::string field_name) {
    auto schema = shredded_object_schema();
    schema.children.back()->children[0]->name = std::move(field_name);
    return schema;
}

ParquetColumnSchema shredded_deep_object_schema() {
    auto schema = unshredded_schema();
    auto root_typed = std::make_unique<ParquetColumnSchema>();
    root_typed->name = "typed_value";
    root_typed->kind = ParquetColumnSchemaKind::STRUCT;

    auto profile = std::make_unique<ParquetColumnSchema>();
    profile->name = "profile";
    profile->kind = ParquetColumnSchemaKind::STRUCT;
    auto profile_typed = std::make_unique<ParquetColumnSchema>();
    profile_typed->name = "typed_value";
    profile_typed->kind = ParquetColumnSchemaKind::STRUCT;

    auto address = std::make_unique<ParquetColumnSchema>();
    address->name = "address";
    address->kind = ParquetColumnSchemaKind::STRUCT;
    auto address_typed = std::make_unique<ParquetColumnSchema>();
    address_typed->name = "typed_value";
    address_typed->kind = ParquetColumnSchemaKind::PRIMITIVE;
    address_typed->type = make_nullable(std::make_shared<DataTypeInt64>());
    address_typed->type_descriptor.integer_bit_width = 64;

    address->children.push_back(std::move(address_typed));
    profile_typed->children.push_back(std::move(address));
    profile->children.push_back(std::move(profile_typed));
    root_typed->children.push_back(std::move(profile));
    schema.children.push_back(std::move(root_typed));
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

ParquetColumnSchema shredded_fallback_only_array_schema() {
    auto schema = unshredded_schema();
    auto typed = std::make_unique<ParquetColumnSchema>();
    typed->name = "typed_value";
    typed->kind = ParquetColumnSchemaKind::LIST;
    auto element = std::make_unique<ParquetColumnSchema>();
    element->name = "element";
    element->kind = ParquetColumnSchemaKind::STRUCT;
    auto value = std::make_unique<ParquetColumnSchema>();
    value->name = "value";
    value->kind = ParquetColumnSchemaKind::PRIMITIVE;
    value->type = std::make_shared<DataTypeString>();
    element->children.push_back(std::move(value));
    typed->children.push_back(std::move(element));
    schema.children.push_back(std::move(typed));
    return schema;
}

ParquetColumnSchema shredded_mixed_array_schema() {
    auto schema = shredded_array_schema();
    auto* element = schema.children.back()->children[0].get();
    auto value = std::make_unique<ParquetColumnSchema>();
    value->name = "value";
    value->kind = ParquetColumnSchemaKind::PRIMITIVE;
    value->type = make_nullable(std::make_shared<DataTypeString>());
    element->children.insert(element->children.begin(), std::move(value));
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

MutableColumnPtr shredded_primitive_physical(MutableColumnPtr typed) {
    const size_t rows = typed->size();
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns fields;
    fields.push_back(nullable_strings(std::vector<StringRef>(rows, metadata),
                                      std::vector<uint8_t>(rows, 0)));
    fields.push_back(nullable_strings(std::vector<StringRef>(rows, {ignored.data(), 0}),
                                      std::vector<uint8_t>(rows, 1)));
    fields.push_back(std::move(typed));
    return ColumnNullable::create(ColumnStruct::create(std::move(fields)),
                                  ColumnUInt8::create(rows, 0));
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

MutableColumnPtr projected_shredded_deep_object_physical(const std::vector<int64_t>& values) {
    auto integers = ColumnInt64::create();
    integers->get_data().assign(values.begin(), values.end());
    MutableColumns address_wrapper_fields;
    address_wrapper_fields.push_back(
            ColumnNullable::create(std::move(integers), ColumnUInt8::create(values.size(), 0)));
    auto address_wrapper = ColumnStruct::create(std::move(address_wrapper_fields));

    MutableColumns profile_object_fields;
    profile_object_fields.push_back(ColumnNullable::create(std::move(address_wrapper),
                                                           ColumnUInt8::create(values.size(), 0)));
    auto profile_object = ColumnStruct::create(std::move(profile_object_fields));

    MutableColumns profile_wrapper_fields;
    profile_wrapper_fields.push_back(ColumnNullable::create(std::move(profile_object),
                                                            ColumnUInt8::create(values.size(), 0)));
    auto profile_wrapper = ColumnStruct::create(std::move(profile_wrapper_fields));

    MutableColumns root_object_fields;
    root_object_fields.push_back(ColumnNullable::create(std::move(profile_wrapper),
                                                        ColumnUInt8::create(values.size(), 0)));
    auto root_object = ColumnStruct::create(std::move(root_object_fields));
    MutableColumns root_fields;
    root_fields.push_back(
            ColumnNullable::create(std::move(root_object), ColumnUInt8::create(values.size(), 0)));
    return ColumnNullable::create(ColumnStruct::create(std::move(root_fields)),
                                  ColumnUInt8::create(values.size(), 0));
}

MutableColumnPtr projected_shredded_int32_object_physical(const std::vector<int32_t>& values) {
    auto integers = ColumnInt32::create();
    integers->get_data().assign(values.begin(), values.end());
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(
            ColumnNullable::create(std::move(integers), ColumnUInt8::create(values.size(), 0)));
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

MutableColumnPtr projected_shredded_binary_object_physical(
        const std::vector<std::string_view>& values) {
    std::vector<StringRef> refs;
    refs.reserve(values.size());
    for (const auto value : values) {
        refs.emplace_back(value.data(), value.size());
    }
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(nullable_strings(refs, std::vector<uint8_t>(values.size(), 0)));
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

MutableColumnPtr root_wrapper(MutableColumns fields, NullMap root_nulls = {0});
MutableColumnPtr nullable_int64(const std::vector<int64_t>& values,
                                const std::vector<uint8_t>& nulls);

MutableColumnPtr complete_shredded_object_physical(std::string_view residual_key,
                                                   int64_t residual_value, int64_t typed_value,
                                                   uint8_t residual_width = 0) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key(StringRef(residual_key.data(), residual_key.size()));
    row.add_scalar(VariantScalarRef::integer(residual_value, residual_width));
    object.finish();
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantRef residual = batch.value_at(0);

    MutableColumns wrapper_fields;
    wrapper_fields.push_back(nullable_int64({typed_value}, {0}));
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(wrapper_fields)),
                                                   ColumnUInt8::create(1, 0)));
    MutableColumns root_fields;
    root_fields.push_back(
            nullable_strings({StringRef(residual.metadata.data, residual.metadata.size)}, {0}));
    root_fields.push_back(
            nullable_strings({StringRef(residual.value.data, residual.value.size)}, {0}));
    root_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(object_fields)),
                                                 ColumnUInt8::create(1, 0)));
    return root_wrapper(std::move(root_fields));
}

MutableColumnPtr complete_shredded_decimal_object_physical(std::string_view residual_key,
                                                           __int128 residual_value,
                                                           uint8_t residual_scale,
                                                           uint8_t residual_width,
                                                           int64_t typed_value) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key(StringRef(residual_key.data(), residual_key.size()));
    row.add_scalar(VariantScalarRef::decimal(residual_value, residual_scale, residual_width));
    object.finish();
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantRef residual = batch.value_at(0);

    MutableColumns wrapper_fields;
    wrapper_fields.push_back(nullable_int64({typed_value}, {0}));
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(wrapper_fields)),
                                                   ColumnUInt8::create(1, 0)));
    MutableColumns root_fields;
    root_fields.push_back(
            nullable_strings({StringRef(residual.metadata.data, residual.metadata.size)}, {0}));
    root_fields.push_back(
            nullable_strings({StringRef(residual.value.data, residual.value.size)}, {0}));
    root_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(object_fields)),
                                                 ColumnUInt8::create(1, 0)));
    return root_wrapper(std::move(root_fields));
}

MutableColumnPtr projected_shredded_decimal_object_physical(__int128 value, uint32_t scale) {
    auto decimals = ColumnDecimal128V3::create(0, scale);
    decimals->insert_value(Decimal128V3 {value});
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(
            ColumnNullable::create(std::move(decimals), ColumnUInt8::create(1, 0)));
    MutableColumns object_fields;
    object_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(wrapper_fields)),
                                                   ColumnUInt8::create(1, 0)));
    MutableColumns root_fields;
    root_fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(object_fields)),
                                                 ColumnUInt8::create(1, 0)));
    return root_wrapper(std::move(root_fields));
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

std::string materialization_error(const ParquetColumnSchema& schema, ColumnPtr physical) {
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const Status status = materialize_variant_rows(schema, std::move(physical), output);
    if (!status.ok()) {
        return status.to_string();
    }
    try {
        const auto& variants = assert_cast<const ColumnVariantV2&>(
                assert_cast<const ColumnNullable&>(*output).get_nested_column());
        (void)variants.get_value_ref(0);
    } catch (const Exception& exception) {
        return exception.what();
    }
    return {};
}

MutableColumnPtr root_wrapper(MutableColumns fields, NullMap root_nulls) {
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(root_nulls.begin(), root_nulls.end());
    return ColumnNullable::create(ColumnStruct::create(std::move(fields)), std::move(null_map));
}

MutableColumnPtr nullable_int64(const std::vector<int64_t>& values,
                                const std::vector<uint8_t>& nulls) {
    auto data = ColumnInt64::create();
    data->get_data().assign(values.begin(), values.end());
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(nulls.begin(), nulls.end());
    return ColumnNullable::create(std::move(data), std::move(null_map));
}

MutableColumnPtr binary_round_trip(const ColumnVariantV2& source) {
    DataTypeVariantV2 type;
    const int64_t maximum_size = type.get_uncompressed_serialized_bytes(source, 10);
    std::vector<char> bytes(maximum_size);
    char* end = type.serialize(source, bytes.data(), 10);
    bytes.resize(end - bytes.data());
    MutableColumnPtr destination = type.create_column();
    EXPECT_EQ(type.deserialize(bytes.data(), &destination, 10), bytes.data() + bytes.size());
    return destination;
}

template <typename ColumnType, typename Value>
MutableColumnPtr nullable_fixed(std::initializer_list<Value> values,
                                std::initializer_list<uint8_t> nulls) {
    auto data = ColumnType::create();
    for (const Value& value : values) {
        data->insert_value(value);
    }
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(nulls.begin(), nulls.end());
    return ColumnNullable::create(std::move(data), std::move(null_map));
}

template <typename ColumnType, typename Value>
MutableColumnPtr nullable_decimal(uint32_t scale, std::initializer_list<Value> values) {
    auto data = ColumnType::create(0, scale);
    for (const Value& value : values) {
        data->insert_value(value);
    }
    return ColumnNullable::create(std::move(data), ColumnUInt8::create(values.size(), 0));
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

TEST(VariantColumnReaderTest, UnshreddedElementChainSeeksWithoutRootReconstruction) {
    VariantBatchBuilder builder;
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("commit"));
        auto commit = row.start_object();
        commit.add_key(StringRef("collection"));
        row.add_string(StringRef("app.bsky.feed.post"));
        commit.add_key(StringRef("operation"));
        row.add_string(StringRef("create"));
        commit.finish();
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("commit"));
        auto commit = row.start_object();
        commit.add_key(StringRef("collection"));
        row.add_null();
        commit.add_key(StringRef("operation"));
        row.add_string(StringRef("create"));
        commit.finish();
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("commit"));
        auto commit = row.start_object();
        commit.finish();
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("other"));
        row.add_int(1);
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_int(7);
        row.finish();
    }
    VariantBatchBuilder batch = builder.finish_batch();
    std::vector<StringRef> metadata;
    std::vector<StringRef> values;
    for (size_t row = 0; row < batch.num_rows(); ++row) {
        const VariantRef value = batch.value_at(row);
        metadata.emplace_back(value.metadata.data, value.metadata.size);
        values.push_back(value.value);
    }

    MutableColumns fields;
    fields.push_back(nullable_strings(metadata, std::vector<uint8_t>(batch.num_rows(), 0)));
    fields.push_back(nullable_strings(values, std::vector<uint8_t>(batch.num_rows(), 0)));
    auto physical = root_wrapper(std::move(fields), {0, 0, 0, 0, 1});

    RuntimeProfile runtime_profile("unshredded-direct-path");
    ParquetProfile parquet_profile;
    parquet_profile.init(&runtime_profile);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const Status materialize_status = materialize_variant_rows(
            unshredded_schema(), *physical, output, parquet_profile.column_reader_profile());
    ASSERT_TRUE(materialize_status.ok()) << materialize_status;
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());

    auto extract_key = [](const ColumnVariantV2& source, const ColumnNullable& nullable,
                          StringRef key) {
        const std::array segments {VariantElementV2PathSegment::object_key(key)};
        std::unique_ptr<ResolvedVariantElementV2Path> path;
        EXPECT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
        ColumnPtr result;
        const auto& null_map = nullable.get_null_map_data();
        const Status status = extract_variant_element_v2(
                source, *path, std::span<const uint8_t>(null_map.data(), null_map.size()), &result);
        EXPECT_TRUE(status.ok()) << status;
        return result;
    };

    ColumnPtr commit_result = extract_key(root_variants, root_nullable, StringRef("commit"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 5);
    const auto& commit_nullable = assert_cast<const ColumnNullable&>(*commit_result);
    const auto& commit_variants =
            assert_cast<const ColumnVariantV2&>(commit_nullable.get_nested_column());
    EXPECT_TRUE(commit_variants.is_shredded());
    ColumnPtr collection_result =
            extract_key(commit_variants, commit_nullable, StringRef("collection"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 10);
    const auto& collection_nullable = assert_cast<const ColumnNullable&>(*collection_result);
    const auto& collection_variants =
            assert_cast<const ColumnVariantV2&>(collection_nullable.get_nested_column());

    EXPECT_EQ(collection_nullable.get_null_map_data(), (NullMap {0, 0, 1, 1, 1}));
    ASSERT_TRUE(collection_variants.is_typed());
    EXPECT_TRUE(collection_variants.typed_type()->equals(DataTypeString()));
    const auto& typed_collection =
            assert_cast<const ColumnNullable&>(collection_variants.typed_column());
    EXPECT_EQ(typed_collection.get_null_map_data(), (NullMap {0, 1, 1, 1, 1}));
    const auto& collection_strings =
            assert_cast<const ColumnString&>(typed_collection.get_nested_column());
    EXPECT_EQ(collection_strings.get_data_at(0), StringRef("app.bsky.feed.post"));

    ColumnPtr repeated_commit_result =
            extract_key(root_variants, root_nullable, StringRef("commit"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 15);
    const auto& repeated_commit_nullable =
            assert_cast<const ColumnNullable&>(*repeated_commit_result);
    const auto& repeated_commit_variants =
            assert_cast<const ColumnVariantV2&>(repeated_commit_nullable.get_nested_column());
    ColumnPtr repeated_collection_result = extract_key(
            repeated_commit_variants, repeated_commit_nullable, StringRef("collection"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 20);
    const auto& repeated_collection_nullable =
            assert_cast<const ColumnNullable&>(*repeated_collection_result);
    const auto& repeated_collection_variants =
            assert_cast<const ColumnVariantV2&>(repeated_collection_nullable.get_nested_column());
    const auto& repeated_typed_collection =
            assert_cast<const ColumnNullable&>(repeated_collection_variants.typed_column());
    const auto& repeated_collection_strings =
            assert_cast<const ColumnString&>(repeated_typed_collection.get_nested_column());
    EXPECT_EQ(repeated_collection_nullable.get_null_map_data(), (NullMap {0, 0, 1, 1, 1}));
    EXPECT_EQ(repeated_typed_collection.get_null_map_data(), (NullMap {0, 1, 1, 1, 1}));
    EXPECT_EQ(repeated_collection_strings.get_data_at(0), StringRef("app.bsky.feed.post"));

    ASSERT_NE(runtime_profile.get_counter("VariantReconstructedRows"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantReconstructedRows")->value(), 0);
    ASSERT_NE(runtime_profile.get_counter("VariantUnshreddedDirectImportRows"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectImportRows")->value(), 0);
    ASSERT_NE(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 20);
    ASSERT_NE(runtime_profile.get_counter("VariantUnshreddedDirectSeekBytes"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekBytes")->value(), 36);
    ASSERT_NE(runtime_profile.get_counter("VariantUnshreddedPrefixReuseRows"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedPrefixReuseRows")->value(), 10);
    ASSERT_NE(runtime_profile.get_counter("VariantDirectLeafRows"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantDirectLeafRows")->value(), 10);
    ASSERT_NE(runtime_profile.get_counter("VariantDirectSubtreeRows"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("VariantDirectSubtreeRows")->value(), 10);

    IColumn::Filter keep_first_two {1, 1, 0, 0, 0};
    ColumnPtr filtered_root = root_nullable.filter(keep_first_two, 2);
    const auto& filtered_nullable = assert_cast<const ColumnNullable&>(*filtered_root);
    const auto& filtered_variants =
            assert_cast<const ColumnVariantV2&>(filtered_nullable.get_nested_column());
    ColumnPtr filtered_commit_result =
            extract_key(filtered_variants, filtered_nullable, StringRef("commit"));
    const auto& filtered_commit_nullable =
            assert_cast<const ColumnNullable&>(*filtered_commit_result);
    const auto& filtered_commit_variants =
            assert_cast<const ColumnVariantV2&>(filtered_commit_nullable.get_nested_column());
    ColumnPtr filtered_collection_result = extract_key(
            filtered_commit_variants, filtered_commit_nullable, StringRef("collection"));
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*filtered_collection_result).get_null_map_data(),
              (NullMap {0, 0}));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 24);

    ColumnPtr filtered_operation_result =
            extract_key(filtered_commit_variants, filtered_commit_nullable, StringRef("operation"));
    const auto& filtered_operation_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*filtered_operation_result).get_nested_column());
    const auto& filtered_operation_strings = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(filtered_operation_variants.typed_column())
                    .get_nested_column());
    EXPECT_EQ(filtered_operation_strings.get_data_at(0), StringRef("create"));
    EXPECT_EQ(filtered_operation_strings.get_data_at(1), StringRef("create"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 26);
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedPrefixReuseRows")->value(), 14);

    ColumnPtr ranged_root = root_nullable.cut(0, 2);
    const auto& ranged_nullable = assert_cast<const ColumnNullable&>(*ranged_root);
    const auto& ranged_variants =
            assert_cast<const ColumnVariantV2&>(ranged_nullable.get_nested_column());
    ColumnPtr ranged_commit_result =
            extract_key(ranged_variants, ranged_nullable, StringRef("commit"));
    const auto& ranged_commit_nullable = assert_cast<const ColumnNullable&>(*ranged_commit_result);
    const auto& ranged_commit_variants =
            assert_cast<const ColumnVariantV2&>(ranged_commit_nullable.get_nested_column());
    (void)extract_key(ranged_commit_variants, ranged_commit_nullable, StringRef("collection"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 30);

    auto gathered_root = root_nullable.clone_empty();
    const std::array<uint32_t, 2> reversed_rows {1, 0};
    gathered_root->insert_indices_from(root_nullable, reversed_rows.begin(), reversed_rows.end());
    const auto& gathered_nullable = assert_cast<const ColumnNullable&>(*gathered_root);
    const auto& gathered_variants =
            assert_cast<const ColumnVariantV2&>(gathered_nullable.get_nested_column());
    ColumnPtr gathered_commit_result =
            extract_key(gathered_variants, gathered_nullable, StringRef("commit"));
    const auto& gathered_commit_nullable =
            assert_cast<const ColumnNullable&>(*gathered_commit_result);
    const auto& gathered_commit_variants =
            assert_cast<const ColumnVariantV2&>(gathered_commit_nullable.get_nested_column());
    (void)extract_key(gathered_commit_variants, gathered_commit_nullable, StringRef("collection"));
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekRows")->value(), 34);
}

TEST(VariantColumnReaderTest, UnshreddedIntegerLeafSeeksAsTypedBigInt) {
    VariantBatchBuilder builder;
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("time_us"));
        row.add_null();
        root.finish();
        row.finish();
    }
    for (const int64_t value :
         {int64_t {7}, int64_t {1} << 8, int64_t {1} << 20, int64_t {1} << 40}) {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("time_us"));
        row.add_int(value);
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("other"));
        row.add_int(1);
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("time_us"));
        row.add_int(9);
        root.finish();
        row.finish();
    }
    VariantBatchBuilder batch = builder.finish_batch();
    std::vector<StringRef> metadata;
    std::vector<StringRef> values;
    for (size_t row = 0; row < batch.num_rows(); ++row) {
        const VariantRef value = batch.value_at(row);
        metadata.emplace_back(value.metadata.data, value.metadata.size);
        values.push_back(value.value);
    }

    MutableColumns fields;
    fields.push_back(nullable_strings(metadata, std::vector<uint8_t>(batch.num_rows(), 0)));
    fields.push_back(nullable_strings(values, std::vector<uint8_t>(batch.num_rows(), 0)));
    auto physical = root_wrapper(std::move(fields), {0, 0, 0, 0, 0, 0, 1});

    RuntimeProfile runtime_profile("unshredded-integer-direct-path");
    ParquetProfile parquet_profile;
    parquet_profile.init(&runtime_profile);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output,
                                         parquet_profile.column_reader_profile())
                        .ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("time_us"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
    ColumnPtr result;
    const Status extract_status = extract_variant_element_v2(
            root_variants, *path,
            std::span<const uint8_t>(root_nullable.get_null_map_data().data(),
                                     root_nullable.get_null_map_data().size()),
            &result);
    ASSERT_TRUE(extract_status.ok()) << extract_status;

    const auto& result_nullable = assert_cast<const ColumnNullable&>(*result);
    EXPECT_EQ(result_nullable.get_null_map_data(), (NullMap {0, 0, 0, 0, 0, 1, 1}));
    const auto& result_variants =
            assert_cast<const ColumnVariantV2&>(result_nullable.get_nested_column());
    ASSERT_TRUE(result_variants.is_typed());
    EXPECT_TRUE(result_variants.typed_type()->equals(DataTypeInt64()));
    const auto& typed = assert_cast<const ColumnNullable&>(result_variants.typed_column());
    EXPECT_EQ(typed.get_null_map_data(), (NullMap {1, 0, 0, 0, 0, 1, 1}));
    const auto& integers = assert_cast<const ColumnInt64&>(typed.get_nested_column());
    EXPECT_EQ(integers.get_data()[1], 7);
    EXPECT_EQ(integers.get_data()[2], int64_t {1} << 8);
    EXPECT_EQ(integers.get_data()[3], int64_t {1} << 20);
    EXPECT_EQ(integers.get_data()[4], int64_t {1} << 40);
    EXPECT_EQ(runtime_profile.get_counter("VariantReconstructedRows")->value(), 0);
    EXPECT_EQ(runtime_profile.get_counter("VariantDirectLeafRows")->value(), 7);
    EXPECT_EQ(runtime_profile.get_counter("VariantDirectSubtreeRows")->value(), 0);
    EXPECT_EQ(runtime_profile.get_counter("VariantUnshreddedDirectSeekBytes")->value(), 32);
}

TEST(VariantColumnReaderTest, UnshreddedExplicitIntegerWidthStaysEncoded) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto root = row.start_object();
    root.add_key(StringRef("value"));
    row.add_scalar(VariantScalarRef::integer(7, 8));
    root.finish();
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantRef encoded = batch.value_at(0);

    MutableColumns fields;
    fields.push_back(
            nullable_strings({StringRef(encoded.metadata.data, encoded.metadata.size)}, {0}));
    fields.push_back(nullable_strings({encoded.value}, {0}));
    auto physical = root_wrapper(std::move(fields));
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("value"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
    ColumnPtr result;
    ASSERT_TRUE(extract_variant_element_v2(
                        root_variants, *path,
                        std::span<const uint8_t>(root_nullable.get_null_map_data().data(),
                                                 root_nullable.get_null_map_data().size()),
                        &result)
                        .ok());

    const auto& result_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*result).get_nested_column());
    ASSERT_TRUE(result_variants.is_shredded());
    EXPECT_EQ(result_variants.get_value_ref(0).primitive_id(), VariantPrimitiveId::INT64);
    EXPECT_EQ(result_variants.get_value_ref(0).get_int(), 7);
}

TEST(VariantColumnReaderTest, UnshreddedDirectPathsSeparateMetadataDictionaries) {
    VariantBatchBuilder first_builder;
    auto first_row = first_builder.begin_row();
    auto first_root = first_row.start_object();
    first_root.add_key(StringRef("a"));
    first_row.add_int(1);
    first_root.add_key(StringRef("target"));
    first_row.add_int(11);
    first_root.finish();
    first_row.finish();
    VariantBatchBuilder first_batch = first_builder.finish_batch();

    VariantBatchBuilder second_builder;
    auto second_row = second_builder.begin_row();
    auto second_root = second_row.start_object();
    second_root.add_key(StringRef("target"));
    second_row.add_int(22);
    second_root.add_key(StringRef("z"));
    second_row.add_int(2);
    second_root.finish();
    second_row.finish();
    VariantBatchBuilder second_batch = second_builder.finish_batch();

    VariantBatchBuilder missing_builder;
    auto missing_row = missing_builder.begin_row();
    auto missing_root = missing_row.start_object();
    missing_root.add_key(StringRef("other"));
    missing_row.add_int(3);
    missing_root.finish();
    missing_row.finish();
    VariantBatchBuilder missing_batch = missing_builder.finish_batch();

    const VariantRef first = first_batch.value_at(0);
    const VariantRef second = second_batch.value_at(0);
    const VariantRef missing = missing_batch.value_at(0);
    const std::vector<VariantRef> rows {first, second, first, missing, second};
    std::vector<StringRef> metadata;
    std::vector<StringRef> values;
    for (const VariantRef value : rows) {
        metadata.emplace_back(value.metadata.data, value.metadata.size);
        values.push_back(value.value);
    }
    MutableColumns fields;
    fields.push_back(nullable_strings(metadata, std::vector<uint8_t>(rows.size(), 0)));
    fields.push_back(nullable_strings(values, std::vector<uint8_t>(rows.size(), 0)));
    auto physical = root_wrapper(std::move(fields), NullMap(rows.size(), 0));
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());

    ColumnPtr target_result;
    ASSERT_TRUE(
            extract_object_key(root_variants, root_nullable, StringRef("target"), &target_result)
                    .ok());
    const auto& target_nullable = assert_cast<const ColumnNullable&>(*target_result);
    EXPECT_EQ(target_nullable.get_null_map_data(), (NullMap {0, 0, 0, 1, 0}));
    const auto& target_variants =
            assert_cast<const ColumnVariantV2&>(target_nullable.get_nested_column());
    ASSERT_TRUE(target_variants.is_typed());
    const auto& target_values = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(target_variants.typed_column()).get_nested_column());
    EXPECT_EQ(target_values.get_data()[0], 11);
    EXPECT_EQ(target_values.get_data()[1], 22);
    EXPECT_EQ(target_values.get_data()[2], 11);
    EXPECT_EQ(target_values.get_data()[4], 22);

    ColumnPtr a_result;
    ASSERT_TRUE(extract_object_key(root_variants, root_nullable, StringRef("a"), &a_result).ok());
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*a_result).get_null_map_data(),
              (NullMap {0, 1, 0, 1, 1}));
}

TEST(VariantColumnReaderTest, UnshreddedCachedMissingStillValidatesEveryObject) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto root = row.start_object();
    root.add_key(StringRef("other"));
    row.add_int(1);
    root.finish();
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantRef valid = batch.value_at(0);
    std::string truncated(valid.value.data, valid.value.size - 1);

    MutableColumns fields;
    fields.push_back(nullable_strings({StringRef(valid.metadata.data, valid.metadata.size),
                                       StringRef(valid.metadata.data, valid.metadata.size)},
                                      {0, 0}));
    fields.push_back(nullable_strings({valid.value, StringRef(truncated)}, {0, 0}));
    auto physical = root_wrapper(std::move(fields), {0, 0});
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());

    ColumnPtr result;
    const Status status =
            extract_object_key(root_variants, root_nullable, StringRef("missing"), &result);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(status.to_string().find("Truncated Variant value"), std::string::npos);
    EXPECT_FALSE(result);
}

TEST(VariantColumnReaderTest, UnshreddedIntegerLeafKeepsCorruptMetadataOnFallback) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto root = row.start_object();
    root.add_key(StringRef("time_us"));
    row.add_int(7);
    root.add_key(StringRef("zz_unused"));
    row.add_int(1);
    root.finish();
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantRef encoded = batch.value_at(0);

    std::string invalid_metadata(encoded.metadata.data, encoded.metadata.size);
    const size_t invalid_key = invalid_metadata.find("zz_unused");
    ASSERT_NE(invalid_key, std::string::npos);
    invalid_metadata[invalid_key] = static_cast<char>(0xff);

    MutableColumns fields;
    fields.push_back(nullable_strings({StringRef(invalid_metadata)}, {0}));
    fields.push_back(nullable_strings({encoded.value}, {0}));
    auto physical = root_wrapper(std::move(fields));
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("time_us"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
    ColumnPtr result;
    ASSERT_TRUE(extract_variant_element_v2(
                        root_variants, *path,
                        std::span<const uint8_t>(root_nullable.get_null_map_data().data(),
                                                 root_nullable.get_null_map_data().size()),
                        &result)
                        .ok());

    const auto& result_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*result).get_nested_column());
    ASSERT_TRUE(result_variants.is_shredded());
    EXPECT_THROW((void)result_variants.get_value_ref(0), Exception);
}

TEST(VariantColumnReaderTest, UnshreddedIntegerLeafValidatesLeadingJsonNull) {
    VariantBatchBuilder invalid_builder;
    auto invalid_row = invalid_builder.begin_row();
    auto invalid_root = invalid_row.start_object();
    invalid_root.add_key(StringRef("time_us"));
    invalid_row.add_null();
    invalid_root.add_key(StringRef("zz_unused"));
    invalid_row.add_int(1);
    invalid_root.finish();
    invalid_row.finish();
    VariantBatchBuilder invalid_batch = invalid_builder.finish_batch();
    const VariantRef invalid = invalid_batch.value_at(0);

    VariantBatchBuilder valid_builder;
    auto valid_row = valid_builder.begin_row();
    auto valid_root = valid_row.start_object();
    valid_root.add_key(StringRef("time_us"));
    valid_row.add_int(7);
    valid_root.finish();
    valid_row.finish();
    VariantBatchBuilder valid_batch = valid_builder.finish_batch();
    const VariantRef valid = valid_batch.value_at(0);

    std::string invalid_metadata(invalid.metadata.data, invalid.metadata.size);
    const size_t invalid_key = invalid_metadata.find("zz_unused");
    ASSERT_NE(invalid_key, std::string::npos);
    invalid_metadata[invalid_key] = static_cast<char>(0xff);

    MutableColumns fields;
    fields.push_back(nullable_strings(
            {StringRef(invalid_metadata), StringRef(valid.metadata.data, valid.metadata.size)},
            {0, 0}));
    fields.push_back(nullable_strings({invalid.value, valid.value}, {0, 0}));
    auto physical = root_wrapper(std::move(fields), {0, 0});
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("time_us"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
    ColumnPtr result;
    ASSERT_TRUE(extract_variant_element_v2(
                        root_variants, *path,
                        std::span<const uint8_t>(root_nullable.get_null_map_data().data(),
                                                 root_nullable.get_null_map_data().size()),
                        &result)
                        .ok());

    const auto& result_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*result).get_nested_column());
    ASSERT_TRUE(result_variants.is_shredded());
    EXPECT_THROW((void)result_variants.get_value_ref(0), Exception);
}

TEST(VariantColumnReaderTest, UnshreddedIntegerLeafReportsCorruptPayloadOnFallback) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto root = row.start_object();
    root.add_key(StringRef("time_us"));
    row.add_int(7);
    root.finish();
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantRef encoded = batch.value_at(0);
    VariantRef encoded_integer;
    ASSERT_TRUE(encoded.object_find(StringRef("time_us"), &encoded_integer));

    std::string invalid_value(encoded.value.data, encoded.value.size);
    const size_t integer_offset = encoded_integer.value.data - encoded.value.data;
    ASSERT_LT(integer_offset, invalid_value.size());
    invalid_value[integer_offset] = static_cast<char>(
            static_cast<uint8_t>(VariantPrimitiveId::INT64) << VARIANT_VALUE_HEADER_SHIFT);

    MutableColumns fields;
    fields.push_back(
            nullable_strings({StringRef(encoded.metadata.data, encoded.metadata.size)}, {0}));
    fields.push_back(nullable_strings({StringRef(invalid_value)}, {0}));
    auto physical = root_wrapper(std::move(fields));
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("time_us"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
    ColumnPtr result;
    const Status extract_status = extract_variant_element_v2(
            root_variants, *path,
            std::span<const uint8_t>(root_nullable.get_null_map_data().data(),
                                     root_nullable.get_null_map_data().size()),
            &result);
    EXPECT_FALSE(extract_status.ok());
    EXPECT_EQ(extract_status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(extract_status.to_string().find("Truncated Variant value"), std::string::npos);
    EXPECT_FALSE(result);
}

TEST(VariantColumnReaderTest, UnshreddedMixedScalarLeafFallsBackToSubtree) {
    VariantBatchBuilder builder;
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("value"));
        row.add_int(7);
        root.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto root = row.start_object();
        root.add_key(StringRef("value"));
        row.add_string(StringRef("seven"));
        root.finish();
        row.finish();
    }
    VariantBatchBuilder batch = builder.finish_batch();
    std::vector<StringRef> metadata;
    std::vector<StringRef> values;
    for (size_t row = 0; row < batch.num_rows(); ++row) {
        const VariantRef value = batch.value_at(row);
        metadata.emplace_back(value.metadata.data, value.metadata.size);
        values.push_back(value.value);
    }

    MutableColumns fields;
    fields.push_back(nullable_strings(metadata, {0, 0}));
    fields.push_back(nullable_strings(values, {0, 0}));
    auto physical = root_wrapper(std::move(fields), {0, 0});
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(unshredded_schema(), *physical, output).ok());
    const auto& root_nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& root_variants =
            assert_cast<const ColumnVariantV2&>(root_nullable.get_nested_column());
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("value"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());
    ColumnPtr result;
    ASSERT_TRUE(extract_variant_element_v2(
                        root_variants, *path,
                        std::span<const uint8_t>(root_nullable.get_null_map_data().data(),
                                                 root_nullable.get_null_map_data().size()),
                        &result)
                        .ok());

    const auto& result_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*result).get_nested_column());
    ASSERT_TRUE(result_variants.is_shredded());
    EXPECT_EQ(result_variants.get_value_ref(0).get_int(), 7);
    EXPECT_EQ(result_variants.get_value_ref(1).get_string(), StringRef("seven"));
}

TEST(VariantColumnReaderTest, UnshreddedDirectImportPreservesEncodedBytes) {
    VariantBatchBuilder metadata_builder;
    auto metadata_row = metadata_builder.begin_row();
    auto metadata_object = metadata_row.start_object();
    metadata_object.add_key(StringRef("unused"));
    metadata_row.add_int(1);
    metadata_object.finish();
    metadata_row.finish();
    VariantBatchBuilder metadata_batch = metadata_builder.finish_batch();
    const VariantRef metadata_source = metadata_batch.value_at(0);

    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const StringRef encoded_value(int_seven.data(), int_seven.size());
    MutableColumns fields;
    fields.push_back(nullable_strings(
            {{metadata_source.metadata.data, metadata_source.metadata.size}}, {0}));
    fields.push_back(nullable_strings({encoded_value}, {0}));
    auto physical = root_wrapper(std::move(fields));

    RuntimeProfile runtime_profile("unshredded-direct-import");
    ParquetProfile parquet_profile;
    parquet_profile.init(&runtime_profile);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const Status status = materialize_variant_rows(unshredded_schema(), *physical, output,
                                                   parquet_profile.column_reader_profile());
    ASSERT_TRUE(status.ok()) << status;
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    ASSERT_TRUE(variants.is_shredded());
    auto* reconstructed_rows = runtime_profile.get_counter("VariantReconstructedRows");
    auto* reconstruction_time = runtime_profile.get_counter("VariantReconstructionTime");
    auto* direct_import_time = runtime_profile.get_counter("VariantUnshreddedDirectImportTime");
    auto* direct_import_rows = runtime_profile.get_counter("VariantUnshreddedDirectImportRows");
    auto* direct_import_bytes = runtime_profile.get_counter("VariantUnshreddedDirectImportBytes");
    ASSERT_NE(reconstructed_rows, nullptr);
    ASSERT_NE(reconstruction_time, nullptr);
    ASSERT_NE(direct_import_time, nullptr);
    ASSERT_NE(direct_import_rows, nullptr);
    ASSERT_NE(direct_import_bytes, nullptr);
    EXPECT_EQ(direct_import_rows->value(), 0);
    EXPECT_EQ(direct_import_bytes->value(), 0);

    const VariantRef imported = variants.get_value_ref(0);
    EXPECT_EQ(StringRef(imported.metadata.data, imported.metadata.size),
              StringRef(metadata_source.metadata.data, metadata_source.metadata.size));
    EXPECT_EQ(imported.value, encoded_value);
    EXPECT_EQ(imported.get_int(), 7);
    EXPECT_EQ(reconstructed_rows->value(), 1);
    EXPECT_EQ(direct_import_rows->value(), 1);
    EXPECT_EQ(direct_import_bytes->value(),
              static_cast<int64_t>(metadata_source.metadata.size + encoded_value.size));
    EXPECT_GT(direct_import_time->value(), 0);
    EXPECT_GE(reconstruction_time->value(), direct_import_time->value());
}

TEST(VariantColumnReaderTest, UnshreddedDirectImportCrossesMaterializationBatchBoundary) {
    constexpr size_t ROWS = 4097;
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const StringRef encoded_value(int_seven.data(), int_seven.size());
    std::vector<std::string> metadata_storage;
    std::vector<StringRef> metadata_rows;
    metadata_storage.reserve(ROWS);
    metadata_rows.reserve(ROWS);
    size_t metadata_bytes = 0;
    for (size_t row = 0; row < ROWS; ++row) {
        metadata_storage.push_back(single_key_metadata_bytes("unused-" + std::to_string(row)));
        metadata_bytes += metadata_storage.back().size();
        metadata_rows.emplace_back(metadata_storage.back());
    }
    MutableColumns fields;
    fields.push_back(nullable_strings(metadata_rows, std::vector<uint8_t>(ROWS, 0)));
    fields.push_back(nullable_strings(std::vector<StringRef>(ROWS, encoded_value),
                                      std::vector<uint8_t>(ROWS, 0)));
    auto physical = root_wrapper(std::move(fields), NullMap(ROWS, 0));

    RuntimeProfile runtime_profile("unshredded-direct-import-batches");
    ParquetProfile parquet_profile;
    parquet_profile.init(&runtime_profile);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const Status status = materialize_variant_rows(unshredded_schema(), *physical, output,
                                                   parquet_profile.column_reader_profile());
    ASSERT_TRUE(status.ok()) << status;
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    ASSERT_EQ(variants.size(), ROWS);
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 7);
    EXPECT_EQ(variants.get_value_ref(4095).get_int(), 7);
    EXPECT_EQ(variants.get_value_ref(4096).get_int(), 7);
    EXPECT_EQ(StringRef(variants.get_value_ref(0).metadata.data,
                        variants.get_value_ref(0).metadata.size),
              metadata_rows[0]);
    EXPECT_EQ(StringRef(variants.get_value_ref(4096).metadata.data,
                        variants.get_value_ref(4096).metadata.size),
              metadata_rows[4096]);
    auto* direct_import_rows = runtime_profile.get_counter("VariantUnshreddedDirectImportRows");
    auto* direct_import_bytes = runtime_profile.get_counter("VariantUnshreddedDirectImportBytes");
    ASSERT_NE(direct_import_rows, nullptr);
    ASSERT_NE(direct_import_bytes, nullptr);
    EXPECT_EQ(direct_import_rows->value(), ROWS);
    EXPECT_EQ(direct_import_bytes->value(),
              static_cast<int64_t>(metadata_bytes + ROWS * encoded_value.size));
}

TEST(VariantColumnReaderTest, UnshreddedDirectImportProfilesImmediateFailureTime) {
    const std::array<char, 1> invalid_value {static_cast<char>(0xff)};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    MutableColumns fields;
    fields.push_back(nullable_strings({metadata}, {0}));
    fields.push_back(nullable_strings({{invalid_value.data(), invalid_value.size()}}, {0}));
    auto physical = root_wrapper(std::move(fields));

    RuntimeProfile runtime_profile("unshredded-direct-import-immediate-failure");
    ParquetProfile parquet_profile;
    parquet_profile.init(&runtime_profile);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const Status status = materialize_variant_rows(unshredded_schema(), *physical, output,
                                                   parquet_profile.column_reader_profile());
    ASSERT_TRUE(status.ok()) << status;
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    EXPECT_THROW((void)variants.get_value_ref(0), Exception);

    auto* reconstructed_rows = runtime_profile.get_counter("VariantReconstructedRows");
    auto* direct_import_time = runtime_profile.get_counter("VariantUnshreddedDirectImportTime");
    auto* direct_import_rows = runtime_profile.get_counter("VariantUnshreddedDirectImportRows");
    auto* direct_import_bytes = runtime_profile.get_counter("VariantUnshreddedDirectImportBytes");
    ASSERT_NE(reconstructed_rows, nullptr);
    ASSERT_NE(direct_import_time, nullptr);
    ASSERT_NE(direct_import_rows, nullptr);
    ASSERT_NE(direct_import_bytes, nullptr);
    EXPECT_EQ(reconstructed_rows->value(), 0);
    EXPECT_GT(direct_import_time->value(), 0);
    EXPECT_EQ(direct_import_rows->value(), 0);
    EXPECT_EQ(direct_import_bytes->value(), 0);
}

TEST(VariantColumnReaderTest, UnshreddedDirectImportProfilesCompletedChunksBeforeFailure) {
    constexpr size_t VALID_ROWS = 4096;
    constexpr size_t ROWS = VALID_ROWS + 1;
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const std::array<char, 1> invalid_value {static_cast<char>(0xff)};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    const StringRef encoded_value(int_seven.data(), int_seven.size());
    std::vector<StringRef> values(VALID_ROWS, encoded_value);
    values.emplace_back(invalid_value.data(), invalid_value.size());
    MutableColumns fields;
    fields.push_back(nullable_strings(std::vector<StringRef>(ROWS, metadata),
                                      std::vector<uint8_t>(ROWS, 0)));
    fields.push_back(nullable_strings(values, std::vector<uint8_t>(ROWS, 0)));
    auto physical = root_wrapper(std::move(fields), NullMap(ROWS, 0));

    RuntimeProfile runtime_profile("unshredded-direct-import-late-failure");
    ParquetProfile parquet_profile;
    parquet_profile.init(&runtime_profile);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const Status status = materialize_variant_rows(unshredded_schema(), *physical, output,
                                                   parquet_profile.column_reader_profile());
    ASSERT_TRUE(status.ok()) << status;
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    EXPECT_THROW((void)variants.get_value_ref(0), Exception);

    auto* reconstructed_rows = runtime_profile.get_counter("VariantReconstructedRows");
    auto* direct_import_time = runtime_profile.get_counter("VariantUnshreddedDirectImportTime");
    auto* direct_import_rows = runtime_profile.get_counter("VariantUnshreddedDirectImportRows");
    auto* direct_import_bytes = runtime_profile.get_counter("VariantUnshreddedDirectImportBytes");
    ASSERT_NE(reconstructed_rows, nullptr);
    ASSERT_NE(direct_import_time, nullptr);
    ASSERT_NE(direct_import_rows, nullptr);
    ASSERT_NE(direct_import_bytes, nullptr);
    EXPECT_EQ(reconstructed_rows->value(), 0);
    EXPECT_GT(direct_import_time->value(), 0);
    EXPECT_EQ(direct_import_rows->value(), VALID_ROWS);
    EXPECT_EQ(direct_import_bytes->value(),
              static_cast<int64_t>(VALID_ROWS * (metadata.size + encoded_value.size)));
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

TEST(VariantColumnReaderTest, ReconstructsShreddedPrimitiveTypeMatrix) {
    auto decode = [](ParquetColumnSchema schema, MutableColumnPtr typed,
                     const std::function<void(const ColumnVariantV2&)>& verify) {
        auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
        const Status status = materialize_variant_rows(
                schema, shredded_primitive_physical(std::move(typed)), output);
        ASSERT_TRUE(status.ok()) << status;
        verify(assert_cast<const ColumnVariantV2&>(
                assert_cast<const ColumnNullable&>(*output).get_nested_column()));
    };

    decode(shredded_primitive_schema(std::make_shared<DataTypeBool>()),
           nullable_fixed<ColumnUInt8, UInt8>({0, 1}, {0, 0}), [](const auto& values) {
               EXPECT_EQ(values.get_value_ref(0).primitive_id(), VariantPrimitiveId::FALSE_VALUE);
               EXPECT_EQ(values.get_value_ref(1).primitive_id(), VariantPrimitiveId::TRUE_VALUE);
           });

    auto verify_integer = [&](DataTypePtr type, MutableColumnPtr typed, int width, int64_t first,
                              int64_t second) {
        auto schema = shredded_primitive_schema(std::move(type));
        schema.children.back()->type_descriptor.integer_bit_width = width;
        decode(std::move(schema), std::move(typed), [&](const auto& values) {
            EXPECT_EQ(values.get_value_ref(0).get_int(), first);
            EXPECT_EQ(values.get_value_ref(1).get_int(), second);
        });
    };
    verify_integer(
            std::make_shared<DataTypeInt8>(),
            nullable_fixed<ColumnInt8, Int8>(
                    {std::numeric_limits<Int8>::min(), std::numeric_limits<Int8>::max()}, {0, 0}),
            8, std::numeric_limits<Int8>::min(), std::numeric_limits<Int8>::max());
    verify_integer(
            std::make_shared<DataTypeInt16>(),
            nullable_fixed<ColumnInt16, Int16>(
                    {std::numeric_limits<Int16>::min(), std::numeric_limits<Int16>::max()}, {0, 0}),
            16, std::numeric_limits<Int16>::min(), std::numeric_limits<Int16>::max());
    verify_integer(
            std::make_shared<DataTypeInt32>(),
            nullable_fixed<ColumnInt32, Int32>(
                    {std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max()}, {0, 0}),
            32, std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max());
    verify_integer(
            std::make_shared<DataTypeInt64>(),
            nullable_fixed<ColumnInt64, Int64>(
                    {std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max()}, {0, 0}),
            64, std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max());

    decode(shredded_primitive_schema(std::make_shared<DataTypeFloat32>()),
           nullable_fixed<ColumnFloat32, Float32>({std::numeric_limits<Float32>::quiet_NaN(),
                                                   std::numeric_limits<Float32>::infinity()},
                                                  {0, 0}),
           [](const auto& values) {
               EXPECT_TRUE(std::isnan(values.get_value_ref(0).get_float()));
               EXPECT_TRUE(std::isinf(values.get_value_ref(1).get_float()));
           });
    decode(shredded_primitive_schema(std::make_shared<DataTypeFloat64>()),
           nullable_fixed<ColumnFloat64, Float64>({-std::numeric_limits<Float64>::infinity(), 1.25},
                                                  {0, 0}),
           [](const auto& values) {
               EXPECT_EQ(values.get_value_ref(0).get_double(),
                         -std::numeric_limits<Float64>::infinity());
               EXPECT_EQ(values.get_value_ref(1).get_double(), 1.25);
           });

    {
        auto schema = shredded_primitive_schema(std::make_shared<DataTypeDecimal32>(9, 2));
        schema.children.back()->type_descriptor.decimal_precision = 9;
        schema.children.back()->type_descriptor.decimal_scale = 2;
        decode(std::move(schema),
               nullable_decimal<ColumnDecimal32, Decimal32>(2, {Decimal32 {12345}, Decimal32 {-1}}),
               [](const auto& values) {
                   EXPECT_EQ(values.get_value_ref(0).get_decimal(), (VariantDecimal {12345, 2, 4}));
                   EXPECT_EQ(values.get_value_ref(1).get_decimal(), (VariantDecimal {-1, 2, 4}));
               });
    }
    {
        auto schema = shredded_primitive_schema(std::make_shared<DataTypeDecimal64>(18, 3));
        schema.children.back()->type_descriptor.decimal_precision = 18;
        schema.children.back()->type_descriptor.decimal_scale = 3;
        decode(std::move(schema),
               nullable_decimal<ColumnDecimal64, Decimal64>(
                       3, {Decimal64 {123456789}, Decimal64 {-123456789}}),
               [](const auto& values) {
                   EXPECT_EQ(values.get_value_ref(0).get_decimal(),
                             (VariantDecimal {123456789, 3, 8}));
                   EXPECT_EQ(values.get_value_ref(1).get_decimal(),
                             (VariantDecimal {-123456789, 3, 8}));
               });
    }
    {
        auto schema = shredded_primitive_schema(std::make_shared<DataTypeDecimal128>(38, 4));
        schema.children.back()->type_descriptor.decimal_precision = 38;
        schema.children.back()->type_descriptor.decimal_scale = 4;
        decode(std::move(schema),
               nullable_decimal<ColumnDecimal128V3, Decimal128V3>(
                       4, {Decimal128V3 {static_cast<Int128>(1234567890123456789LL)}}),
               [](const auto& values) {
                   EXPECT_EQ(values.get_value_ref(0).get_decimal(),
                             (VariantDecimal {1234567890123456789LL, 4, 16}));
               });
    }

    const auto date = DateV2Value<DateV2ValueType>::create_from_olap_date(
            (static_cast<uint32_t>(1970) << 9) | (static_cast<uint32_t>(1) << 5) | 2);
    decode(shredded_primitive_schema(std::make_shared<DataTypeDateV2>()),
           nullable_fixed<ColumnDateV2, DateV2Value<DateV2ValueType>>({date}, {0}),
           [](const auto& values) { EXPECT_EQ(values.get_value_ref(0).get_date(), 1); });

    auto datetime = DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000001ULL);
    datetime.set_microsecond(234567);
    {
        auto schema = shredded_primitive_schema(std::make_shared<DataTypeDateTimeV2>(6));
        schema.children.back()->type_descriptor.time_unit = ParquetTimeUnit::MICROS;
        schema.children.back()->type_descriptor.timestamp_is_adjusted_to_utc = false;
        decode(std::move(schema),
               nullable_fixed<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>({datetime}, {0}),
               [](const auto& values) {
                   EXPECT_EQ(values.get_value_ref(0).get_timestamp_ntz_micros(), 1234567);
               });
    }
    TimestampTzValue timestamp;
    timestamp.unchecked_set_time(1970, 1, 1, 0, 0, 2, 345678);
    {
        auto schema = shredded_primitive_schema(std::make_shared<DataTypeTimeStampTz>(6));
        schema.children.back()->type_descriptor.time_unit = ParquetTimeUnit::MICROS;
        schema.children.back()->type_descriptor.timestamp_is_adjusted_to_utc = true;
        decode(std::move(schema),
               nullable_fixed<ColumnTimeStampTz, TimestampTzValue>({timestamp}, {0}),
               [](const auto& values) {
                   EXPECT_EQ(values.get_value_ref(0).get_timestamp_micros(), 2345678);
               });
    }

    auto verify_bytes = [&](bool string_annotation, bool uuid) {
        const std::array<uint8_t, 16> bytes {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        auto schema = shredded_primitive_schema(std::make_shared<DataTypeString>());
        schema.children.back()->type_descriptor.is_string_annotation = string_annotation;
        schema.children.back()->type_descriptor.is_uuid = uuid;
        auto strings = ColumnString::create();
        if (uuid) {
            strings->insert_data(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        } else {
            strings->insert_data("bytes", 5);
        }
        auto typed = ColumnNullable::create(std::move(strings), ColumnUInt8::create(1, 0));
        decode(std::move(schema), std::move(typed), [&](const auto& values) {
            if (uuid) {
                EXPECT_EQ(values.get_value_ref(0).get_uuid(), bytes);
            } else if (string_annotation) {
                EXPECT_EQ(values.get_value_ref(0).get_string(), StringRef("bytes"));
            } else {
                EXPECT_EQ(values.get_value_ref(0).get_binary(), StringRef("bytes"));
            }
        });
    };
    verify_bytes(false, false);
    verify_bytes(true, false);
    verify_bytes(false, true);
}

TEST(VariantColumnReaderTest, RejectsInvalidShreddedUuidWidth) {
    auto schema = shredded_primitive_schema(std::make_shared<DataTypeString>());
    schema.children.back()->type_descriptor.is_uuid = true;
    auto strings = ColumnString::create();
    strings->insert_data("short", 5);
    auto typed = ColumnNullable::create(std::move(strings), ColumnUInt8::create(1, 0));
    const std::string error =
            materialization_error(schema, shredded_primitive_physical(std::move(typed)));
    EXPECT_NE(error.find("UUID has 5 bytes instead of 16"), std::string::npos) << error;
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

TEST(VariantColumnReaderTest, AppendsCompleteShreddedStatesWithDifferentSchemasAndMetadata) {
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    auto first_schema = shredded_named_object_schema("a");
    ASSERT_TRUE(materialize_variant_rows(first_schema,
                                         complete_shredded_object_physical("left", 1, 11), output)
                        .ok());
    auto second_schema = shredded_named_object_schema("b");
    ASSERT_TRUE(materialize_variant_rows(second_schema,
                                         complete_shredded_object_physical("right", 2, 22), output)
                        .ok());

    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    ASSERT_EQ(variants.size(), 2);
    VariantRef field;
    ASSERT_TRUE(variants.get_value_ref(0).object_find(StringRef("left"), &field));
    EXPECT_EQ(field.get_int(), 1);
    ASSERT_TRUE(variants.get_value_ref(0).object_find(StringRef("a"), &field));
    EXPECT_EQ(field.get_int(), 11);
    ASSERT_TRUE(variants.get_value_ref(1).object_find(StringRef("right"), &field));
    EXPECT_EQ(field.get_int(), 2);
    ASSERT_TRUE(variants.get_value_ref(1).object_find(StringRef("b"), &field));
    EXPECT_EQ(field.get_int(), 22);
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

TEST(VariantColumnReaderTest, GathersConsecutiveProjectedShreddedBatches) {
    auto schema = shredded_object_schema();
    schema.local_id = 0;
    schema.children[2]->local_id = 2;
    schema.children[2]->children[0]->local_id = 0;
    schema.children[2]->children[0]->children[0]->local_id = 0;
    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);

    auto first = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    auto second = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(
            materialize_variant_columns(plan, projected_shredded_object_physical({10, 20}), first)
                    .ok());
    ASSERT_TRUE(materialize_variant_columns(plan, projected_shredded_object_physical({30}), second)
                        .ok());

    auto gathered = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const std::array<uint32_t, 2> first_indices {1, 0};
    const std::array<uint32_t, 1> second_indices {0};
    gathered->insert_indices_from(*first, first_indices.begin(), first_indices.end());
    gathered->insert_indices_from(*second, second_indices.begin(), second_indices.end());

    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*gathered).get_nested_column());
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    const auto match = variants.find_shredded_typed_value(path);
    ASSERT_TRUE(match.has_value());
    const auto& values = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(*match->column).get_nested_column());
    EXPECT_EQ(values.get_data(), ColumnInt64::Container({20, 10, 30}));

    auto restored = binary_round_trip(variants);
    const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> resolved_path;
    ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &resolved_path).ok());
    ColumnPtr extracted;
    ASSERT_TRUE(extract_variant_element_v2(assert_cast<const ColumnVariantV2&>(*restored),
                                           *resolved_path, {}, &extracted)
                        .ok());
    const auto& restored_values = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*extracted).get_nested_column());
    EXPECT_EQ(restored_values.get_value_ref(0).get_int(), 20);
    EXPECT_EQ(restored_values.get_value_ref(1).get_int(), 10);
    EXPECT_EQ(restored_values.get_value_ref(2).get_int(), 30);
}

TEST(VariantColumnReaderTest, SelectsProjectedShreddedRowsWithoutMaterializing) {
    auto schema = shredded_object_schema();
    schema.local_id = 0;
    schema.children[2]->local_id = 2;
    schema.children[2]->children[0]->local_id = 0;
    schema.children[2]->children[0]->children[0]->local_id = 0;
    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(plan, projected_shredded_object_physical({10, 20, 30}),
                                            output)
                        .ok());
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    const IColumn::Permutation permutation {2, 0, 1};
    MutableColumnPtr permuted = variants.permute(permutation, 2);
    MutableColumnPtr truncated = variants.clone_resized(2);
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    auto verify = [&](const IColumn& column, const ColumnInt64::Container& expected) {
        const auto& selected = assert_cast<const ColumnVariantV2&>(column);
        ASSERT_TRUE(selected.is_shredded());
        const auto match = selected.find_shredded_typed_value(path);
        ASSERT_TRUE(match.has_value());
        EXPECT_EQ(assert_cast<const ColumnInt64&>(
                          assert_cast<const ColumnNullable&>(*match->column).get_nested_column())
                          .get_data(),
                  expected);
    };
    verify(*permuted, ColumnInt64::Container({30, 10}));
    verify(*truncated, ColumnInt64::Container({10, 20}));
}

TEST(VariantColumnReaderTest, GathersLocalProjectedAndRemoteSerializedRowsInEitherOrder) {
    auto schema = shredded_object_schema();
    schema.local_id = 0;
    schema.children[2]->local_id = 2;
    schema.children[2]->children[0]->local_id = 0;
    schema.children[2]->children[0]->children[0]->local_id = 0;
    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(
            materialize_variant_columns(plan, projected_shredded_object_physical({10, 20}), output)
                    .ok());
    const auto& local = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    ASSERT_TRUE(local.is_shredded());
    MutableColumnPtr remote = binary_round_trip(local);
    ASSERT_FALSE(assert_cast<const ColumnVariantV2&>(*remote).is_shredded());

    const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &path).ok());
    auto verify_result = [&](const ColumnVariantV2& gathered,
                             const std::array<int64_t, 2>& expected) {
        ASSERT_EQ(gathered.size(), expected.size());

        ColumnPtr extracted;
        ASSERT_TRUE(extract_variant_element_v2(gathered, *path, {}, &extracted).ok());
        const auto& values = assert_cast<const ColumnVariantV2&>(
                assert_cast<const ColumnNullable&>(*extracted).get_nested_column());
        for (size_t row = 0; row < expected.size(); ++row) {
            EXPECT_EQ(values.get_value_ref(row).get_int(), expected[row]);
        }
    };
    auto verify = [&](const std::vector<const IColumn*>& sources,
                      const std::vector<size_t>& positions,
                      const std::array<int64_t, 2>& expected) {
        auto gathered = ColumnVariantV2::create();
        gathered->insert_from_multi_column(sources, positions);
        verify_result(*gathered, expected);
    };

    verify({&local, remote.get()}, {0, 1}, {10, 20});
    verify({remote.get(), &local}, {1, 0}, {20, 10});

    const std::array<uint32_t, 1> first_row {0};
    const std::array<uint32_t, 1> second_row {1};
    auto indexed = ColumnVariantV2::create();
    indexed->insert_indices_from(local, first_row.begin(), first_row.end());
    indexed->insert_indices_from(*remote, second_row.begin(), second_row.end());
    verify_result(*indexed, {10, 20});

    indexed = ColumnVariantV2::create();
    indexed->insert_indices_from(*remote, second_row.begin(), second_row.end());
    indexed->insert_indices_from(local, first_row.begin(), first_row.end());
    verify_result(*indexed, {20, 10});
}

TEST(VariantColumnReaderTest, ShrinksProjectedShreddedStateWithoutMaterializing) {
    auto schema = shredded_object_schema();
    schema.local_id = 0;
    schema.children[2]->local_id = 2;
    schema.children[2]->children[0]->local_id = 0;
    schema.children[2]->children[0]->children[0]->local_id = 0;
    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(plan, projected_shredded_object_physical({10, 20, 30}),
                                            output)
                        .ok());
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    ASSERT_TRUE(variants.is_shredded());

    ColumnPtr shrink_source = variants.clone_resized(variants.size());
    ColumnPtr shrunk = shrink_source->shrink(2);
    const auto& shrunk_variants = assert_cast<const ColumnVariantV2&>(*shrunk);
    ASSERT_TRUE(shrunk_variants.is_shredded());
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    const auto match = shrunk_variants.find_shredded_typed_value(path);
    ASSERT_TRUE(match.has_value());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(
                      assert_cast<const ColumnNullable&>(*match->column).get_nested_column())
                      .get_data(),
              ColumnInt64::Container({10, 20}));

    ColumnPtr empty_source = variants.clone_resized(variants.size());
    ColumnPtr empty = empty_source->shrink(0);
    EXPECT_EQ(empty->size(), 0);
    EXPECT_FALSE(assert_cast<const ColumnVariantV2&>(*empty).is_shredded());
}

TEST(VariantColumnReaderTest, GathersCompleteAndProjectedShreddedBatches) {
    auto projected_schema = shredded_object_schema();
    projected_schema.local_id = 0;
    projected_schema.children[0]->local_id = 0;
    projected_schema.children[1]->local_id = 1;
    projected_schema.children[2]->local_id = 2;
    projected_schema.children[2]->children[0]->local_id = 0;
    projected_schema.children[2]->children[0]->children[0]->local_id = 0;

    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    VariantMaterializationNode projected_plan;
    projected_plan.schema = &projected_schema;
    projected_plan.contains_variant = true;
    projected_plan.variant_projection = std::move(projection);
    projected_plan.variant_state_schema =
            create_variant_state_schema(projected_schema, &*projected_plan.variant_projection);

    auto complete_schema = shredded_named_object_schema("b");
    VariantMaterializationNode complete_plan;
    complete_plan.schema = &complete_schema;
    complete_plan.contains_variant = true;
    complete_plan.variant_state_schema = create_variant_state_schema(complete_schema, nullptr);

    auto projected = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    auto complete = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(projected_plan, projected_shredded_object_physical({7}),
                                            projected)
                        .ok());
    ASSERT_TRUE(materialize_variant_columns(
                        complete_plan, complete_shredded_object_physical("other", 9, 8), complete)
                        .ok());

    const std::array<uint32_t, 1> selected {0};
    const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &path).ok());
    auto verify_order = [&](const IColumn& first, const IColumn& second,
                            const NullMap& expected_nulls, size_t value_row) {
        auto gathered = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
        gathered->insert_indices_from(first, selected.begin(), selected.end());
        gathered->insert_indices_from(second, selected.begin(), selected.end());

        const auto& nullable = assert_cast<const ColumnNullable&>(*gathered);
        const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        ColumnPtr extracted;
        ASSERT_TRUE(extract_variant_element_v2(variants, *path, nullable.get_null_map_data(),
                                               &extracted)
                            .ok());
        const auto& extracted_nullable = assert_cast<const ColumnNullable&>(*extracted);
        EXPECT_EQ(extracted_nullable.get_null_map_data(), expected_nulls);
        const auto& extracted_values =
                assert_cast<const ColumnVariantV2&>(extracted_nullable.get_nested_column());
        EXPECT_EQ(extracted_values.get_value_ref(value_row).get_int(), 7);
    };

    // Complete and projected files can alternate in either order; a field absent from the
    // complete file must contribute NULL without forcing the projected file to materialize.
    verify_order(*projected, *complete, NullMap({0, 1}), 0);
    verify_order(*complete, *projected, NullMap({1, 0}), 1);
}

TEST(VariantColumnReaderTest, PreservesPrimitiveWidthsAcrossProjectedFiles) {
    auto int64_schema = shredded_object_schema();
    auto int32_schema = shredded_object_schema();
    int32_schema.children[2]->children[0]->children[0]->type =
            make_nullable(std::make_shared<DataTypeInt32>());
    int32_schema.children[2]->children[0]->children[0]->type_descriptor.integer_bit_width = 32;
    for (auto* schema : {&int64_schema, &int32_schema}) {
        schema->local_id = 0;
        schema->children[2]->local_id = 2;
        schema->children[2]->children[0]->local_id = 0;
        schema->children[2]->children[0]->children[0]->local_id = 0;
    }
    auto make_plan = [](const ParquetColumnSchema& schema) {
        auto projection = format::LocalColumnIndex::partial_local(0);
        projection.children.push_back(format::LocalColumnIndex::partial_local(2));
        projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
        projection.children.back().children.back().children.push_back(
                format::LocalColumnIndex::local(0));
        VariantMaterializationNode plan;
        plan.schema = &schema;
        plan.contains_variant = true;
        plan.variant_projection = std::move(projection);
        plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);
        return plan;
    };
    auto int64_plan = make_plan(int64_schema);
    auto int32_plan = make_plan(int32_schema);
    auto int64_rows = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    auto int32_rows = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(int64_plan, projected_shredded_object_physical({7}),
                                            int64_rows)
                        .ok());
    ASSERT_TRUE(materialize_variant_columns(
                        int32_plan, projected_shredded_int32_object_physical({8}), int32_rows)
                        .ok());

    auto gathered = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const std::array<uint32_t, 1> selected {0};
    gathered->insert_indices_from(*int64_rows, selected.begin(), selected.end());
    gathered->insert_indices_from(*int32_rows, selected.begin(), selected.end());

    const auto& nullable = assert_cast<const ColumnNullable&>(*gathered);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &path).ok());
    ColumnPtr extracted;
    ASSERT_TRUE(
            extract_variant_element_v2(variants, *path, nullable.get_null_map_data(), &extracted)
                    .ok());
    const auto& values = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*extracted).get_nested_column());
    EXPECT_EQ(values.get_value_ref(0).primitive_id(), VariantPrimitiveId::INT64);
    EXPECT_EQ(values.get_value_ref(1).primitive_id(), VariantPrimitiveId::INT32);
}

TEST(VariantColumnReaderTest, PreservesWidthsAcrossMaterializedPathFallback) {
    auto projected_int_schema = shredded_object_schema();
    auto projected_decimal_schema = shredded_object_schema();
    auto* decimal_leaf = projected_decimal_schema.children[2]->children[0]->children[0].get();
    decimal_leaf->type = make_nullable(std::make_shared<DataTypeDecimal128>(38, 2));
    decimal_leaf->type_descriptor.decimal_precision = 38;
    decimal_leaf->type_descriptor.decimal_scale = 2;
    auto prepare_projected = [](ParquetColumnSchema& schema) {
        schema.local_id = 0;
        schema.children[2]->local_id = 2;
        schema.children[2]->children[0]->local_id = 0;
        schema.children[2]->children[0]->children[0]->local_id = 0;
        auto projection = format::LocalColumnIndex::partial_local(0);
        projection.children.push_back(format::LocalColumnIndex::partial_local(2));
        projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
        projection.children.back().children.back().children.push_back(
                format::LocalColumnIndex::local(0));
        VariantMaterializationNode plan;
        plan.schema = &schema;
        plan.contains_variant = true;
        plan.variant_projection = std::move(projection);
        plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);
        return plan;
    };
    auto projected_int_plan = prepare_projected(projected_int_schema);
    auto projected_decimal_plan = prepare_projected(projected_decimal_schema);
    auto complete_schema = shredded_named_object_schema("b");
    VariantMaterializationNode complete_plan;
    complete_plan.schema = &complete_schema;
    complete_plan.contains_variant = true;
    complete_plan.variant_state_schema = create_variant_state_schema(complete_schema, nullptr);

    auto verify = [&](VariantMaterializationNode& projected_plan,
                      MutableColumnPtr projected_physical, MutableColumnPtr complete_physical,
                      VariantPrimitiveId expected_id) {
        auto projected = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
        auto complete = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
        ASSERT_TRUE(materialize_variant_columns(projected_plan, std::move(projected_physical),
                                                projected)
                            .ok());
        ASSERT_TRUE(
                materialize_variant_columns(complete_plan, std::move(complete_physical), complete)
                        .ok());
        const std::array<uint32_t, 1> selected {0};
        const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
        std::unique_ptr<ResolvedVariantElementV2Path> path;
        ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &path).ok());
        for (const auto order : {std::array<const IColumn*, 2> {projected.get(), complete.get()},
                                 std::array<const IColumn*, 2> {complete.get(), projected.get()}}) {
            auto gathered = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
            for (const IColumn* source : order) {
                gathered->insert_indices_from(*source, selected.begin(), selected.end());
            }
            const auto& nullable = assert_cast<const ColumnNullable&>(*gathered);
            const auto& variants =
                    assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
            ColumnPtr extracted;
            ASSERT_TRUE(extract_variant_element_v2(variants, *path, nullable.get_null_map_data(),
                                                   &extracted)
                                .ok());
            const auto& values = assert_cast<const ColumnVariantV2&>(
                    assert_cast<const ColumnNullable&>(*extracted).get_nested_column());
            EXPECT_EQ(values.get_value_ref(0).primitive_id(), expected_id);
            EXPECT_EQ(values.get_value_ref(1).primitive_id(), expected_id);
        }
    };

    verify(projected_int_plan, projected_shredded_object_physical({7}),
           complete_shredded_object_physical("a", 8, 9, 8), VariantPrimitiveId::INT64);
    verify(projected_decimal_plan, projected_shredded_decimal_object_physical(7, 2),
           complete_shredded_decimal_object_physical("a", 8, 2, 16, 9),
           VariantPrimitiveId::DECIMAL16);
}

TEST(VariantColumnReaderTest, GathersProjectedShreddedBatchesWithDifferentLeafTypes) {
    auto integer_schema = shredded_object_schema();
    auto string_schema = shredded_binary_object_schema();
    for (auto* schema : {&integer_schema, &string_schema}) {
        schema->local_id = 0;
        schema->children[2]->local_id = 2;
        schema->children[2]->children[0]->local_id = 0;
        schema->children[2]->children[0]->children[0]->local_id = 0;
    }
    auto make_plan = [](const ParquetColumnSchema& schema) {
        auto projection = format::LocalColumnIndex::partial_local(0);
        projection.children.push_back(format::LocalColumnIndex::partial_local(2));
        projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
        projection.children.back().children.back().children.push_back(
                format::LocalColumnIndex::local(0));
        VariantMaterializationNode plan;
        plan.schema = &schema;
        plan.contains_variant = true;
        plan.variant_projection = std::move(projection);
        plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);
        return plan;
    };
    auto integer_plan = make_plan(integer_schema);
    auto string_plan = make_plan(string_schema);
    auto integers = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    auto strings = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(integer_plan,
                                            projected_shredded_object_physical({7, 8}), integers)
                        .ok());
    ASSERT_TRUE(materialize_variant_columns(
                        string_plan, projected_shredded_binary_object_physical({"seven"}), strings)
                        .ok());

    auto gathered = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const std::array<uint32_t, 2> integer_rows {0, 1};
    const std::array<uint32_t, 1> string_rows {0};
    gathered->insert_indices_from(*integers, integer_rows.begin(), integer_rows.end());
    gathered->insert_indices_from(*strings, string_rows.begin(), string_rows.end());

    const auto& nullable = assert_cast<const ColumnNullable&>(*gathered);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const std::array path_segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(path_segments, &path).ok());
    ColumnPtr extracted;
    ASSERT_TRUE(
            extract_variant_element_v2(variants, *path, nullable.get_null_map_data(), &extracted)
                    .ok());
    const auto& extracted_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*extracted).get_nested_column());
    EXPECT_EQ(extracted_variants.get_value_ref(0).get_int(), 7);
    EXPECT_EQ(extracted_variants.get_value_ref(1).get_int(), 8);
    EXPECT_EQ(extracted_variants.get_value_ref(2).get_binary(), StringRef("seven"));

    variants.sanity_check();
    EXPECT_GT(variants.byte_size(), 0);
    EXPECT_GE(variants.allocated_bytes(), variants.byte_size());

    auto ranged = ColumnVariantV2::create();
    ranged->insert_range_from(variants, 1, 2);
    const auto ranged_match =
            ranged->find_shredded_typed_value(std::array {VariantShreddedPathSegment {
                    .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}});
    ASSERT_TRUE(ranged_match.has_value());
    ASSERT_TRUE(ranged_match->normalized);
    const auto& ranged_values = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*ranged_match->normalized).get_nested_column());
    EXPECT_EQ(ranged_values.get_value_ref(0).get_int(), 8);
    EXPECT_EQ(ranged_values.get_value_ref(1).get_binary(), StringRef("seven"));

    const std::array shredded_path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("a")}};
    auto reordered = ColumnVariantV2::create();
    const std::array<uint32_t, 3> reversed {2, 1, 0};
    reordered->insert_indices_from(variants, reversed.begin(), reversed.end());
    const auto reordered_match = reordered->find_shredded_typed_value(shredded_path);
    ASSERT_TRUE(reordered_match.has_value());
    ASSERT_TRUE(reordered_match->normalized);
    const auto& reordered_values = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*reordered_match->normalized).get_nested_column());
    EXPECT_EQ(reordered_values.get_value_ref(0).get_binary(), StringRef("seven"));
    EXPECT_EQ(reordered_values.get_value_ref(1).get_int(), 8);
    EXPECT_EQ(reordered_values.get_value_ref(2).get_int(), 7);

    IColumn::Filter keep_integer {1, 0, 0};
    const auto filtered = variants.filter(keep_integer, 1);
    const auto filtered_match =
            assert_cast<const ColumnVariantV2&>(*filtered).find_shredded_typed_value(shredded_path);
    ASSERT_TRUE(filtered_match.has_value());
    ASSERT_TRUE(filtered_match->column);
    EXPECT_EQ(
            assert_cast<const ColumnInt64&>(
                    assert_cast<const ColumnNullable&>(*filtered_match->column).get_nested_column())
                    .get_data()[0],
            7);
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

TEST(VariantColumnReaderTest, ShreddedStateOutlivesScannerProfile) {
    auto runtime_profile = std::make_unique<RuntimeProfile>("variant-reader-test");
    ParquetProfile parquet_profile;
    parquet_profile.init(runtime_profile.get());
    ParquetProfile reused_profile;
    reused_profile.init(runtime_profile.get());
    EXPECT_EQ(parquet_profile.variant_reconstructed_rows,
              reused_profile.variant_reconstructed_rows);
    EXPECT_EQ(parquet_profile.variant_unshredded_direct_import_rows,
              reused_profile.variant_unshredded_direct_import_rows);

    auto visible_output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_int64_schema(), shredded_int64_physical({7}),
                                         visible_output, parquet_profile.column_reader_profile())
                        .ok());
    const auto& visible_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*visible_output).get_nested_column());
    EXPECT_EQ(visible_variants.get_value_ref(0).get_int(), 7);
    EXPECT_EQ(runtime_profile->get_counter("VariantReconstructedRows")->value(), 1);
    EXPECT_EQ(runtime_profile->get_counter("VariantUnshreddedDirectImportRows")->value(), 0);

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_int64_schema(), shredded_int64_physical({42}),
                                         output, parquet_profile.column_reader_profile())
                        .ok());
    runtime_profile.reset();

    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 42);
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

TEST(VariantColumnReaderTest, MaterializesFallbackOnlyArrayFromRequiredValueLeaf) {
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    const std::array<char, 2> first_value {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            3};
    const std::array<char, 2> second_value {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            4};

    auto values = ColumnString::create();
    values->insert_data(first_value.data(), first_value.size());
    values->insert_data(second_value.data(), second_value.size());
    MutableColumns wrapper_fields;
    wrapper_fields.push_back(std::move(values));
    auto wrappers = ColumnStruct::create(std::move(wrapper_fields));
    auto elements = ColumnNullable::create(std::move(wrappers), ColumnUInt8::create(2, 0));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto array = ColumnArray::create(std::move(elements), std::move(offsets));

    const std::array<char, 1> ignored {0};
    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata}, {0}));
    root_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
    root_fields.push_back(ColumnNullable::create(std::move(array), ColumnUInt8::create(1, 0)));
    auto physical = root_wrapper(std::move(root_fields));

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    const auto status =
            materialize_variant_rows(shredded_fallback_only_array_schema(), *physical, output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const VariantRef value = variants.get_value_ref(0);
    ASSERT_EQ(value.num_elements(), 2);
    EXPECT_EQ(value.array_at(0).get_int(), 3);
    EXPECT_EQ(value.array_at(1).get_int(), 4);
}

TEST(VariantColumnReaderTest, RejectsCorruptShreddedWrappersWithoutCrashing) {
    const std::array<char, 2> int_seven {
            static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                              << VARIANT_VALUE_HEADER_SHIFT),
            7};
    const std::array<char, 1> invalid_value {static_cast<char>(0xff)};
    const std::array<char, 1> ignored {0};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    const StringRef residual_int(int_seven.data(), int_seven.size());
    auto expect_error = [](const std::string& error, std::string_view expected) {
        EXPECT_NE(error.find(expected), std::string::npos) << error;
    };
    std::string_view current_case;

    try {
        {
            current_case = "null metadata";
            SCOPED_TRACE("null metadata");
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {1}));
            fields.push_back(nullable_strings({residual_int}, {0}));
            expect_error(
                    materialization_error(unshredded_schema(), root_wrapper(std::move(fields))),
                    "null metadata");
        }
        {
            current_case = "wrapper without carriers";
            SCOPED_TRACE("wrapper without carriers");
            auto schema = unshredded_schema();
            schema.children.pop_back();
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            expect_error(materialization_error(schema, root_wrapper(std::move(fields))),
                         "neither value nor typed_value");
        }
        {
            current_case = "scalar with residual";
            SCOPED_TRACE("scalar with residual");
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({residual_int}, {0}));
            fields.push_back(nullable_int64({8}, {0}));
            expect_error(
                    materialization_error(shredded_int64_schema(), root_wrapper(std::move(fields))),
                    "scalar typed_value cannot have residual");
        }
        {
            current_case = "object with scalar residual";
            SCOPED_TRACE("object with scalar residual");
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({residual_int}, {0}));
            MutableColumns wrapper_fields;
            wrapper_fields.push_back(nullable_int64({9}, {0}));
            MutableColumns object_fields;
            object_fields.push_back(ColumnNullable::create(
                    ColumnStruct::create(std::move(wrapper_fields)), ColumnUInt8::create(1, 0)));
            fields.push_back(ColumnNullable::create(ColumnStruct::create(std::move(object_fields)),
                                                    ColumnUInt8::create(1, 0)));
            expect_error(materialization_error(shredded_object_schema(),
                                               root_wrapper(std::move(fields))),
                         "non-object residual");
        }
        {
            current_case = "object field count mismatch";
            SCOPED_TRACE("object field count mismatch");
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
            MutableColumns unexpected_object_fields;
            unexpected_object_fields.push_back(nullable_int64({1}, {0}));
            unexpected_object_fields.push_back(nullable_int64({2}, {0}));
            fields.push_back(ColumnNullable::create(
                    ColumnStruct::create(std::move(unexpected_object_fields)),
                    ColumnUInt8::create(1, 0)));
            expect_error(materialization_error(shredded_object_schema(),
                                               root_wrapper(std::move(fields))),
                         "physical field count mismatch");
        }
        {
            current_case = "array with residual";
            SCOPED_TRACE("array with residual");
            MutableColumns empty_wrapper_fields;
            empty_wrapper_fields.push_back(nullable_int64({}, {}));
            auto empty_elements = ColumnNullable::create(
                    ColumnStruct::create(std::move(empty_wrapper_fields)), ColumnUInt8::create());
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->insert_value(0);
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({residual_int}, {0}));
            fields.push_back(ColumnNullable::create(
                    ColumnArray::create(std::move(empty_elements), std::move(offsets)),
                    ColumnUInt8::create(1, 0)));
            expect_error(
                    materialization_error(shredded_array_schema(), root_wrapper(std::move(fields))),
                    "array typed_value cannot have residual");
        }
        {
            current_case = "null array element wrapper";
            SCOPED_TRACE("null array element wrapper");
            MutableColumns wrapper_fields;
            wrapper_fields.push_back(nullable_int64({0}, {1}));
            auto wrappers = ColumnStruct::create(std::move(wrapper_fields));
            auto elements = ColumnNullable::create(std::move(wrappers), ColumnUInt8::create(1, 1));
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->insert_value(1);
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
            fields.push_back(ColumnNullable::create(
                    ColumnArray::create(std::move(elements), std::move(offsets)),
                    ColumnUInt8::create(1, 0)));
            expect_error(
                    materialization_error(shredded_array_schema(), root_wrapper(std::move(fields))),
                    "array element wrapper is null");
        }
        {
            current_case = "missing array element";
            SCOPED_TRACE("missing array element");
            MutableColumns element_fields;
            element_fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
            element_fields.push_back(nullable_int64({0}, {1}));
            auto elements = ColumnNullable::create(ColumnStruct::create(std::move(element_fields)),
                                                   ColumnUInt8::create(1, 0));
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->insert_value(1);
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({{ignored.data(), 0}}, {1}));
            fields.push_back(ColumnNullable::create(
                    ColumnArray::create(std::move(elements), std::move(offsets)),
                    ColumnUInt8::create(1, 0)));
            expect_error(materialization_error(shredded_mixed_array_schema(),
                                               root_wrapper(std::move(fields))),
                         "array element is missing");
        }
        {
            current_case = "root field count mismatch";
            SCOPED_TRACE("root field count mismatch");
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({residual_int}, {0}));
            fields.push_back(nullable_int64({8}, {0}));
            fields.push_back(nullable_int64({9}, {0}));
            expect_error(
                    materialization_error(shredded_int64_schema(), root_wrapper(std::move(fields))),
                    "physical field count mismatch");
        }
        {
            current_case = "invalid metadata";
            SCOPED_TRACE("invalid metadata");
            MutableColumns fields;
            fields.push_back(nullable_strings({StringRef("bad")}, {0}));
            fields.push_back(nullable_strings({residual_int}, {0}));
            expect_error(
                    materialization_error(unshredded_schema(), root_wrapper(std::move(fields))),
                    "metadata");
        }
        {
            current_case = "invalid residual value";
            SCOPED_TRACE("invalid residual value");
            MutableColumns fields;
            fields.push_back(nullable_strings({metadata}, {0}));
            fields.push_back(nullable_strings({{invalid_value.data(), invalid_value.size()}}, {0}));
            expect_error(
                    materialization_error(unshredded_schema(), root_wrapper(std::move(fields))),
                    "Variant");
        }
    } catch (const std::exception& error) {
        FAIL() << "Unexpected exception in " << current_case << ": " << error.what();
    }
}

TEST(VariantColumnReaderTest, ImmediateCorruptionLeavesDestinationUnchanged) {
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(
            materialize_variant_rows(shredded_int64_schema(), shredded_int64_physical({7}), output)
                    .ok());
    MutableColumns invalid_fields;
    invalid_fields.push_back(nullable_strings(
            {{VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size()}}, {0}));
    const Status status = materialize_variant_rows(shredded_int64_schema(),
                                                   root_wrapper(std::move(invalid_fields)), output);
    EXPECT_FALSE(status.ok());
    ASSERT_EQ(output->size(), 1);
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).get_int(), 7);
}

TEST(VariantColumnReaderTest, LazyNestedCorruptionLeavesDestinationUnchanged) {
    const std::array<char, 1> invalid_value {static_cast<char>(0xff)};
    const StringRef metadata(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    auto corrupt_variant = [&]() {
        MutableColumns fields;
        fields.push_back(nullable_strings({metadata}, {0}));
        fields.push_back(nullable_strings({{invalid_value.data(), invalid_value.size()}}, {0}));
        return root_wrapper(std::move(fields));
    };
    auto label_schema = []() {
        auto schema = std::make_unique<ParquetColumnSchema>();
        schema->name = "label";
        schema->kind = ParquetColumnSchemaKind::PRIMITIVE;
        schema->type = make_nullable(std::make_shared<DataTypeString>());
        return schema;
    };
    auto make_plan = [](const ParquetColumnSchema& root) {
        auto build = [&](auto&& self, const ParquetColumnSchema* schema)
                -> std::unique_ptr<VariantMaterializationNode> {
            auto node = std::make_unique<VariantMaterializationNode>();
            node->schema = schema;
            node->contains_variant = schema->kind == ParquetColumnSchemaKind::VARIANT;
            for (const auto& child_schema : schema->children) {
                auto child = self(self, child_schema.get());
                node->contains_variant = node->contains_variant || child->contains_variant;
                node->children.push_back(std::move(child));
            }
            return node;
        };
        return build(build, &root);
    };
    auto make_struct_schema = [&](ParquetColumnSchema variant_schema) {
        ParquetColumnSchema root;
        root.name = "row";
        root.kind = ParquetColumnSchemaKind::STRUCT;
        root.children.push_back(label_schema());
        root.children.push_back(std::make_unique<ParquetColumnSchema>(std::move(variant_schema)));
        return root;
    };
    auto make_struct_physical = [&](std::string_view label, MutableColumnPtr variant) {
        MutableColumns fields;
        fields.push_back(nullable_strings({StringRef(label.data(), label.size())}, {0}));
        fields.push_back(std::move(variant));
        return ColumnStruct::create(std::move(fields));
    };
    const auto element_type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeString>()),
                       make_nullable(std::make_shared<DataTypeVariantV2>())},
            Strings {"label", "payload"});

    {
        auto output = element_type->create_column();
        auto valid_schema = make_struct_schema(shredded_int64_schema());
        auto valid_plan = make_plan(valid_schema);
        ASSERT_TRUE(materialize_variant_columns(
                            *valid_plan,
                            *make_struct_physical("before", shredded_int64_physical({7})), output)
                            .ok());

        auto corrupt_schema = make_struct_schema(unshredded_schema());
        auto corrupt_plan = make_plan(corrupt_schema);
        const Status status = materialize_variant_columns(
                *corrupt_plan, *make_struct_physical("after", corrupt_variant()), output);
        EXPECT_FALSE(status.ok());

        const auto& structure = assert_cast<const ColumnStruct&>(*output);
        const auto& label = assert_cast<const ColumnNullable&>(structure.get_column(0));
        EXPECT_EQ(label.size(), 1);
        EXPECT_EQ(label.get_null_map_data(), (NullMap {0}));
        EXPECT_EQ(label.get_nested_column().get_data_at(0).to_string(), "before");
        const auto& payload = assert_cast<const ColumnNullable&>(structure.get_column(1));
        EXPECT_EQ(payload.size(), 1);
        EXPECT_EQ(payload.get_null_map_data(), (NullMap {0}));
        EXPECT_EQ(assert_cast<const ColumnVariantV2&>(payload.get_nested_column())
                          .get_value_ref(0)
                          .get_int(),
                  7);
    }

    {
        auto output = std::make_shared<DataTypeArray>(element_type)->create_column();
        auto valid_element_schema = make_struct_schema(shredded_int64_schema());
        ParquetColumnSchema valid_schema;
        valid_schema.name = "rows";
        valid_schema.kind = ParquetColumnSchemaKind::LIST;
        valid_schema.children.push_back(
                std::make_unique<ParquetColumnSchema>(std::move(valid_element_schema)));
        auto valid_plan = make_plan(valid_schema);
        auto valid_offsets = ColumnArray::ColumnOffsets::create();
        valid_offsets->insert_value(1);
        auto valid_physical =
                ColumnArray::create(make_struct_physical("before", shredded_int64_physical({7})),
                                    std::move(valid_offsets));
        ASSERT_TRUE(materialize_variant_columns(*valid_plan, *valid_physical, output).ok());

        auto corrupt_element_schema = make_struct_schema(unshredded_schema());
        ParquetColumnSchema corrupt_schema;
        corrupt_schema.name = "rows";
        corrupt_schema.kind = ParquetColumnSchemaKind::LIST;
        corrupt_schema.children.push_back(
                std::make_unique<ParquetColumnSchema>(std::move(corrupt_element_schema)));
        auto corrupt_plan = make_plan(corrupt_schema);
        auto corrupt_offsets = ColumnArray::ColumnOffsets::create();
        corrupt_offsets->insert_value(1);
        auto corrupt_physical = ColumnArray::create(
                make_struct_physical("after", corrupt_variant()), std::move(corrupt_offsets));
        const Status status = materialize_variant_columns(*corrupt_plan, *corrupt_physical, output);
        EXPECT_FALSE(status.ok());

        const auto& array = assert_cast<const ColumnArray&>(*output);
        EXPECT_EQ(array.get_offsets(), (ColumnArray::Offsets64 {1}));
        const auto& element = assert_cast<const ColumnNullable&>(array.get_data());
        EXPECT_EQ(element.get_null_map_data(), (NullMap {0}));
        const auto& structure = assert_cast<const ColumnStruct&>(element.get_nested_column());
        const auto& label = assert_cast<const ColumnNullable&>(structure.get_column(0));
        EXPECT_EQ(label.size(), 1);
        EXPECT_EQ(label.get_null_map_data(), (NullMap {0}));
        EXPECT_EQ(label.get_nested_column().get_data_at(0).to_string(), "before");
        const auto& payload = assert_cast<const ColumnNullable&>(structure.get_column(1));
        EXPECT_EQ(payload.size(), 1);
        EXPECT_EQ(payload.get_null_map_data(), (NullMap {0}));
        EXPECT_EQ(assert_cast<const ColumnVariantV2&>(payload.get_nested_column())
                          .get_value_ref(0)
                          .get_int(),
                  7);
    }

    {
        auto output =
                std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                              make_nullable(std::make_shared<DataTypeVariantV2>()))
                        ->create_column();
        auto make_map_schema = [&](ParquetColumnSchema variant_schema) {
            ParquetColumnSchema root;
            root.name = "entries";
            root.kind = ParquetColumnSchemaKind::MAP;
            root.children.push_back(label_schema());
            root.children.push_back(
                    std::make_unique<ParquetColumnSchema>(std::move(variant_schema)));
            return root;
        };
        auto make_map_physical = [&](std::string_view key, MutableColumnPtr variant) {
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->insert_value(1);
            return ColumnMap::create(nullable_strings({StringRef(key.data(), key.size())}, {0}),
                                     std::move(variant), std::move(offsets));
        };

        auto valid_schema = make_map_schema(shredded_int64_schema());
        auto valid_plan = make_plan(valid_schema);
        ASSERT_TRUE(materialize_variant_columns(
                            *valid_plan, *make_map_physical("before", shredded_int64_physical({7})),
                            output)
                            .ok());
        auto corrupt_schema = make_map_schema(unshredded_schema());
        auto corrupt_plan = make_plan(corrupt_schema);
        const Status status = materialize_variant_columns(
                *corrupt_plan, *make_map_physical("after", corrupt_variant()), output);
        EXPECT_FALSE(status.ok());

        const auto& map = assert_cast<const ColumnMap&>(*output);
        EXPECT_EQ(map.get_offsets(), (ColumnArray::Offsets64 {1}));
        const auto& keys = assert_cast<const ColumnNullable&>(map.get_keys());
        EXPECT_EQ(keys.size(), 1);
        EXPECT_EQ(keys.get_null_map_data(), (NullMap {0}));
        EXPECT_EQ(keys.get_nested_column().get_data_at(0).to_string(), "before");
        const auto& values = assert_cast<const ColumnNullable&>(map.get_values());
        EXPECT_EQ(values.size(), 1);
        EXPECT_EQ(values.get_null_map_data(), (NullMap {0}));
        EXPECT_EQ(assert_cast<const ColumnVariantV2&>(values.get_nested_column())
                          .get_value_ref(0)
                          .get_int(),
                  7);
    }
}

TEST(VariantColumnReaderTest, MaterializesMixedRootArraysAndNullKinds) {
    VariantBatchBuilder residual_builder;
    {
        auto row = residual_builder.begin_row();
        row.add_null();
        row.finish();
    }
    {
        auto row = residual_builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("x"));
        row.add_int(2);
        object.finish();
        row.finish();
    }
    {
        auto row = residual_builder.begin_row();
        auto array = row.start_array();
        row.add_int(3);
        row.add_int(4);
        array.finish();
        row.finish();
    }
    {
        auto row = residual_builder.begin_row();
        row.add_string(StringRef("tail"));
        row.finish();
    }
    VariantBatchBuilder residuals = residual_builder.finish_batch();
    const VariantRef first = residuals.value_at(0);
    std::vector<StringRef> residual_values;
    for (size_t row = 0; row < residuals.num_rows(); ++row) {
        residual_values.push_back(residuals.value_at(row).value);
    }
    residual_values.insert(residual_values.begin() + 1, StringRef {});

    MutableColumns element_fields;
    element_fields.push_back(nullable_strings(residual_values, {0, 1, 0, 0, 0}));
    element_fields.push_back(nullable_int64({0, 1, 0, 0, 0}, {1, 0, 1, 1, 1}));
    auto elements = ColumnNullable::create(ColumnStruct::create(std::move(element_fields)),
                                           ColumnUInt8::create(5, 0));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({0, 5, 5, 5});
    auto arrays = ColumnArray::create(std::move(elements), std::move(offsets));

    const StringRef metadata(first.metadata.data, first.metadata.size);
    const std::array<char, 1> ignored {0};
    MutableColumns root_fields;
    root_fields.push_back(nullable_strings({metadata, metadata, metadata, metadata}, {0, 0, 0, 0}));
    root_fields.push_back(nullable_strings(
            {{ignored.data(), 0}, {ignored.data(), 0}, {ignored.data(), 0}, {ignored.data(), 0}},
            {1, 1, 1, 1}));
    auto typed_nulls = ColumnUInt8::create(4, 0);
    typed_nulls->get_data()[2] = 1;
    typed_nulls->get_data()[3] = 1;
    root_fields.push_back(ColumnNullable::create(std::move(arrays), std::move(typed_nulls)));
    auto physical = root_wrapper(std::move(root_fields), {0, 0, 0, 1});

    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_rows(shredded_mixed_array_schema(), *physical, output).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 0, 1}));
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variants.get_value_ref(0).num_elements(), 0);
    const VariantRef mixed = variants.get_value_ref(1);
    ASSERT_EQ(mixed.num_elements(), 5);
    EXPECT_TRUE(mixed.array_at(0).is_null());
    EXPECT_EQ(mixed.array_at(1).get_int(), 1);
    VariantRef object_field;
    ASSERT_TRUE(mixed.array_at(2).object_find(StringRef("x"), &object_field));
    EXPECT_EQ(object_field.get_int(), 2);
    EXPECT_EQ(mixed.array_at(3).array_at(1).get_int(), 4);
    EXPECT_EQ(mixed.array_at(4).get_string(), StringRef("tail"));
    EXPECT_TRUE(variants.get_value_ref(2).is_null());
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

TEST(VariantColumnReaderTest, MaterializesPhysicallyShreddedVariantInStructArrayAndMap) {
    auto make_plan_child = [](const ParquetColumnSchema* schema) {
        auto child = std::make_unique<VariantMaterializationNode>();
        child->schema = schema;
        child->contains_variant = schema->kind == ParquetColumnSchemaKind::VARIANT;
        return child;
    };

    {
        ParquetColumnSchema root_schema;
        root_schema.name = "root";
        root_schema.kind = ParquetColumnSchemaKind::STRUCT;
        root_schema.children.push_back(
                std::make_unique<ParquetColumnSchema>(shredded_int64_schema()));
        VariantMaterializationNode plan;
        plan.schema = &root_schema;
        plan.contains_variant = true;
        plan.children.push_back(make_plan_child(root_schema.children[0].get()));
        MutableColumns physical_fields;
        physical_fields.push_back(shredded_int64_physical({11}));
        auto physical = ColumnStruct::create(std::move(physical_fields));
        auto output = std::make_shared<DataTypeStruct>(
                              DataTypes {make_nullable(std::make_shared<DataTypeVariantV2>())},
                              Strings {"v"})
                              ->create_column();
        ASSERT_TRUE(materialize_variant_columns(plan, *physical, output).ok());
        const auto& variants = assert_cast<const ColumnVariantV2&>(
                assert_cast<const ColumnNullable&>(
                        assert_cast<const ColumnStruct&>(*output).get_column(0))
                        .get_nested_column());
        EXPECT_EQ(variants.get_value_ref(0).get_int(), 11);
    }

    {
        ParquetColumnSchema root_schema;
        root_schema.name = "items";
        root_schema.kind = ParquetColumnSchemaKind::LIST;
        root_schema.children.push_back(
                std::make_unique<ParquetColumnSchema>(shredded_int64_schema()));
        VariantMaterializationNode plan;
        plan.schema = &root_schema;
        plan.contains_variant = true;
        plan.children.push_back(make_plan_child(root_schema.children[0].get()));
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(2);
        auto physical = ColumnArray::create(shredded_int64_physical({12, 13}), std::move(offsets));
        auto output = std::make_shared<DataTypeArray>(
                              make_nullable(std::make_shared<DataTypeVariantV2>()))
                              ->create_column();
        ASSERT_TRUE(materialize_variant_columns(plan, *physical, output).ok());
        const auto& variants = assert_cast<const ColumnVariantV2&>(
                assert_cast<const ColumnNullable&>(
                        assert_cast<const ColumnArray&>(*output).get_data())
                        .get_nested_column());
        EXPECT_EQ(variants.get_value_ref(0).get_int(), 12);
        EXPECT_EQ(variants.get_value_ref(1).get_int(), 13);
    }

    {
        ParquetColumnSchema root_schema;
        root_schema.name = "entries";
        root_schema.kind = ParquetColumnSchemaKind::MAP;
        auto key_schema = std::make_unique<ParquetColumnSchema>();
        key_schema->name = "key";
        key_schema->kind = ParquetColumnSchemaKind::PRIMITIVE;
        key_schema->type = std::make_shared<DataTypeString>();
        root_schema.children.push_back(std::move(key_schema));
        root_schema.children.push_back(
                std::make_unique<ParquetColumnSchema>(shredded_int64_schema()));
        VariantMaterializationNode plan;
        plan.schema = &root_schema;
        plan.contains_variant = true;
        plan.children.push_back(make_plan_child(root_schema.children[0].get()));
        plan.children.push_back(make_plan_child(root_schema.children[1].get()));
        auto keys = ColumnString::create();
        keys->insert_data("a", 1);
        keys->insert_data("b", 1);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(2);
        auto physical = ColumnMap::create(std::move(keys), shredded_int64_physical({14, 15}),
                                          std::move(offsets));
        auto output =
                std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                              make_nullable(std::make_shared<DataTypeVariantV2>()))
                        ->create_column();
        ASSERT_TRUE(materialize_variant_columns(plan, *physical, output).ok());
        const auto& variants = assert_cast<const ColumnVariantV2&>(
                assert_cast<const ColumnNullable&>(
                        assert_cast<const ColumnMap&>(*output).get_values())
                        .get_nested_column());
        EXPECT_EQ(variants.get_value_ref(0).get_int(), 14);
        EXPECT_EQ(variants.get_value_ref(1).get_int(), 15);
    }
}

TEST(VariantColumnReaderTest, ProjectedShreddedStateRejectsRootMaterialization) {
    auto schema = shredded_object_schema();
    schema.local_id = 0;
    schema.children[2]->local_id = 2;
    schema.children[2]->children[0]->local_id = 0;
    schema.children[2]->children[0]->children[0]->local_id = 0;
    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(materialize_variant_columns(plan, projected_shredded_object_physical({17}), output)
                        .ok());
    const auto& variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    EXPECT_THROW((void)variants.get_value_ref(0), Exception);
}

TEST(VariantColumnReaderTest, ProjectedShreddedStateServesBinaryDeepPathChain) {
    auto schema = shredded_deep_object_schema();
    schema.local_id = 0;
    schema.children[2]->local_id = 2;
    auto* profile = schema.children[2]->children[0].get();
    profile->local_id = 0;
    profile->children[0]->local_id = 0;
    auto* address = profile->children[0]->children[0].get();
    address->local_id = 0;
    address->children[0]->local_id = 0;

    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.back().children.push_back(
            format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));

    VariantMaterializationNode plan;
    plan.schema = &schema;
    plan.contains_variant = true;
    plan.variant_projection = std::move(projection);
    plan.variant_state_schema = create_variant_state_schema(schema, &*plan.variant_projection);
    auto output = make_nullable(std::make_shared<DataTypeVariantV2>())->create_column();
    ASSERT_TRUE(
            materialize_variant_columns(plan, projected_shredded_deep_object_physical({17}), output)
                    .ok());
    const auto& root = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*output).get_nested_column());
    EXPECT_THROW((void)root.get_value_ref(0), Exception);

    const std::array profile_segment {
            VariantElementV2PathSegment::object_key(StringRef("profile"))};
    std::unique_ptr<ResolvedVariantElementV2Path> profile_path;
    ASSERT_TRUE(resolve_variant_element_v2_path(profile_segment, &profile_path).ok());
    ColumnPtr profile_value;
    const Status profile_status =
            extract_variant_element_v2(root, *profile_path, {}, &profile_value);
    ASSERT_TRUE(profile_status.ok()) << profile_status;
    const auto& profile_value_variant = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*profile_value).get_nested_column());

    const std::array address_segment {
            VariantElementV2PathSegment::object_key(StringRef("address"))};
    std::unique_ptr<ResolvedVariantElementV2Path> address_path;
    ASSERT_TRUE(resolve_variant_element_v2_path(address_segment, &address_path).ok());
    ColumnPtr address_value;
    const Status address_status =
            extract_variant_element_v2(profile_value_variant, *address_path, {}, &address_value);
    ASSERT_TRUE(address_status.ok()) << address_status;
    const auto& address_value_variant = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*address_value).get_nested_column());
    EXPECT_EQ(address_value_variant.get_value_ref(0).get_int(), 17);
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
