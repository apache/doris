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

#include "format_v2/parquet/parquet_column_schema.h"

#include <parquet/api/schema.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/parquet/parquet_type.h"

namespace doris::parquet {
namespace {

struct SchemaBuildContext {
    // Reader-local id inside the parent schema node.
    int32_t local_id = -1;
    int16_t definition_level = 0;
    int16_t repetition_level = 0;
    int16_t nullable_definition_level = 0;
    int16_t repeated_repetition_level = 0;
};

bool is_list_node(const ::parquet::schema::Node& node) {
    const auto& logical_type = node.logical_type();
    return node.converted_type() == ::parquet::ConvertedType::LIST ||
           (logical_type != nullptr && logical_type->is_valid() && logical_type->is_list());
}

bool is_map_node(const ::parquet::schema::Node& node) {
    const auto& logical_type = node.logical_type();
    return node.converted_type() == ::parquet::ConvertedType::MAP ||
           node.converted_type() == ::parquet::ConvertedType::MAP_KEY_VALUE ||
           (logical_type != nullptr && logical_type->is_valid() && logical_type->is_map());
}

DataTypePtr nullable_if_needed(DataTypePtr type, const ::parquet::schema::Node& node) {
    return node.is_optional() ? make_nullable(type) : type;
}

void inherit_common_schema_state(const ::parquet::schema::Node& node,
                                 const SchemaBuildContext& context,
                                 ParquetColumnSchema* column_schema) {
    DORIS_CHECK(column_schema != nullptr);
    column_schema->local_id = context.local_id;
    column_schema->parquet_field_id = node.field_id();
    column_schema->name = node.name();
    column_schema->max_definition_level = context.definition_level;
    column_schema->max_repetition_level = context.repetition_level;
    column_schema->nullable_definition_level = context.nullable_definition_level;
    column_schema->repeated_repetition_level = context.repeated_repetition_level;
}

SchemaBuildContext child_context(const SchemaBuildContext& parent,
                                 const ::parquet::schema::Node& child_node, int32_t child_idx) {
    SchemaBuildContext result = parent;
    result.local_id = child_idx;
    if (child_node.repetition() != ::parquet::Repetition::REQUIRED) {
        result.definition_level++;
        result.nullable_definition_level = result.definition_level;
    }
    if (child_node.is_repeated()) {
        result.repetition_level++;
        result.repeated_repetition_level = result.repetition_level;
    }
    return result;
}

void propagate_child_levels(ParquetColumnSchema* column_schema) {
    DORIS_CHECK(column_schema != nullptr);
    for (const auto& child : column_schema->children) {
        column_schema->max_definition_level =
                std::max(column_schema->max_definition_level, child->max_definition_level);
        column_schema->max_repetition_level =
                std::max(column_schema->max_repetition_level, child->max_repetition_level);
    }
}

// Recursively builds ParquetColumnSchema for the given schema node and its children in Parquet file's metadata.
Status build_node_schema(const ::parquet::SchemaDescriptor& schema,
                         const ::parquet::schema::Node& node, const SchemaBuildContext& context,
                         std::unique_ptr<ParquetColumnSchema>* result) {
    if (result == nullptr) {
        return Status::InvalidArgument("result is null");
    }
    auto column_schema = std::make_unique<ParquetColumnSchema>();
    inherit_common_schema_state(node, context, column_schema.get());

    if (node.is_primitive()) {
        const int leaf_column_id = schema.ColumnIndex(node);
        if (leaf_column_id < 0) {
            return Status::InvalidArgument("Cannot find leaf column id for parquet column {}",
                                           node.name());
        }
        column_schema->kind = ParquetColumnSchemaKind::PRIMITIVE;
        column_schema->leaf_column_id = leaf_column_id;
        column_schema->descriptor = schema.Column(leaf_column_id);
        if (column_schema->descriptor != nullptr) {
            column_schema->max_definition_level = column_schema->descriptor->max_definition_level();
            column_schema->max_repetition_level = column_schema->descriptor->max_repetition_level();
        }
        column_schema->type_descriptor = resolve_parquet_type(column_schema->descriptor);
        column_schema->type = column_schema->type_descriptor.doris_type;
        if (column_schema->type == nullptr) {
            return Status::NotSupported("Unsupported parquet column type for column {}",
                                        node.name());
        }
        column_schema->type = node.is_optional()
                                      ? make_nullable(remove_nullable(column_schema->type))
                                      : remove_nullable(column_schema->type);
        *result = std::move(column_schema);
        return Status::OK();
    }

    const auto& group = static_cast<const ::parquet::schema::GroupNode&>(node);
    if (is_list_node(node)) {
        column_schema->kind = ParquetColumnSchemaKind::LIST;
        if (group.field_count() != 1) {
            return Status::NotSupported("Unsupported parquet LIST encoding for column {}",
                                        node.name());
        }
        const auto& repeated_node = *group.field(0);
        if (!repeated_node.is_repeated() || repeated_node.is_primitive()) {
            return Status::NotSupported("Unsupported parquet LIST encoding for column {}",
                                        node.name());
        }
        const auto& repeated_group =
                static_cast<const ::parquet::schema::GroupNode&>(repeated_node);
        if (repeated_group.field_count() != 1) {
            return Status::NotSupported("Unsupported parquet LIST element layout for column {}",
                                        node.name());
        }
        auto repeated_context = child_context(context, repeated_node, 0);
        column_schema->repeated_repetition_level = repeated_context.repeated_repetition_level;
        std::unique_ptr<ParquetColumnSchema> child;
        RETURN_IF_ERROR(build_node_schema(
                schema, *repeated_group.field(0),
                child_context(repeated_context, *repeated_group.field(0), 0), &child));
        column_schema->type =
                nullable_if_needed(std::make_shared<DataTypeArray>(child->type), node);
        column_schema->children.push_back(std::move(child));
        propagate_child_levels(column_schema.get());
        *result = std::move(column_schema);
        return Status::OK();
    }

    if (is_map_node(node)) {
        column_schema->kind = ParquetColumnSchemaKind::MAP;
        if (group.field_count() != 1) {
            return Status::NotSupported("Unsupported parquet MAP encoding for column {}",
                                        node.name());
        }
        const auto& key_value_node = *group.field(0);
        if (!key_value_node.is_repeated()) {
            return Status::NotSupported("Unsupported parquet MAP encoding for column {}",
                                        node.name());
        }
        auto key_value_context = child_context(context, key_value_node, 0);
        column_schema->repeated_repetition_level = key_value_context.repeated_repetition_level;
        if (key_value_node.is_primitive()) {
            return Status::NotSupported("Unsupported parquet MAP key_value layout for column {}",
                                        node.name());
        }
        const auto& key_value_group =
                static_cast<const ::parquet::schema::GroupNode&>(key_value_node);
        if (key_value_group.field_count() != 2) {
            return Status::NotSupported("Unsupported parquet MAP key_value layout for column {}",
                                        node.name());
        }
        auto key_value = std::make_unique<ParquetColumnSchema>();
        inherit_common_schema_state(key_value_node, key_value_context, key_value.get());
        key_value->kind = ParquetColumnSchemaKind::STRUCT;
        DataTypes child_types;
        Strings child_names;
        child_types.reserve(key_value_group.field_count());
        child_names.reserve(key_value_group.field_count());
        for (int child_idx = 0; child_idx < key_value_group.field_count(); ++child_idx) {
            std::unique_ptr<ParquetColumnSchema> child;
            RETURN_IF_ERROR(build_node_schema(
                    schema, *key_value_group.field(child_idx),
                    child_context(key_value_context, *key_value_group.field(child_idx), child_idx),
                    &child));
            child_types.push_back(child->type);
            child_names.push_back(child->name);
            key_value->children.push_back(std::move(child));
        }
        key_value->type = std::make_shared<DataTypeStruct>(child_types, child_names);
        propagate_child_levels(key_value.get());
        if (key_value->children.size() != 2) {
            return Status::NotSupported("Unsupported parquet MAP key_value layout for column {}",
                                        node.name());
        }
        if (key_value_group.field(0)->repetition() != ::parquet::Repetition::REQUIRED) {
            return Status::NotSupported("Unsupported nullable parquet MAP key for column {}",
                                        node.name());
        }
        auto key_type = key_value->children[0]->type;
        auto value_type = key_value->children[1]->type;
        column_schema->type =
                nullable_if_needed(std::make_shared<DataTypeMap>(key_type, value_type), node);
        column_schema->children.push_back(std::move(key_value));
        propagate_child_levels(column_schema.get());
        *result = std::move(column_schema);
        return Status::OK();
    }

    column_schema->kind = ParquetColumnSchemaKind::STRUCT;
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(group.field_count());
    child_names.reserve(group.field_count());
    for (int child_idx = 0; child_idx < group.field_count(); ++child_idx) {
        std::unique_ptr<ParquetColumnSchema> child;
        RETURN_IF_ERROR(build_node_schema(
                schema, *group.field(child_idx),
                child_context(context, *group.field(child_idx), child_idx), &child));
        child_types.push_back(child->type);
        child_names.push_back(child->name);
        column_schema->children.push_back(std::move(child));
    }
    column_schema->type =
            nullable_if_needed(std::make_shared<DataTypeStruct>(child_types, child_names), node);
    propagate_child_levels(column_schema.get());
    *result = std::move(column_schema);
    return Status::OK();
}

} // namespace

Status build_parquet_column_schema(const ::parquet::SchemaDescriptor& schema,
                                   std::vector<std::unique_ptr<ParquetColumnSchema>>* fields) {
    if (fields == nullptr) {
        return Status::InvalidArgument("fields is null");
    }
    fields->clear();
    const auto* root = schema.group_node();
    if (root == nullptr) {
        return Status::InvalidArgument("Parquet schema root is null");
    }
    fields->reserve(root->field_count());
    for (int field_idx = 0; field_idx < root->field_count(); ++field_idx) {
        std::unique_ptr<ParquetColumnSchema> field;
        SchemaBuildContext context;
        RETURN_IF_ERROR(build_node_schema(
                schema, *root->field(field_idx),
                child_context(context, *root->field(field_idx), field_idx), &field));
        fields->push_back(std::move(field));
    }
    return Status::OK();
}

} // namespace doris::parquet
