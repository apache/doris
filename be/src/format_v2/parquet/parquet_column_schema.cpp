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
    int16_t repeated_ancestor_definition_level = 0;
};

enum class SchemaBuildMode {
    // Normal recursive schema build. A repeated LIST/MAP annotated group is rejected in this mode
    // because Parquet LIST/MAP outer groups are not allowed to be repeated at a top-level or struct
    // field boundary.
    NORMAL,
    // Build the current repeated node as the already-selected element of an enclosing LIST. This
    // is the compatibility path for Arrow/parquet-format legacy two-level LIST encodings where the
    // repeated node itself is the array element instead of a wrapper that should be stripped.
    REPEATED_NODE_AS_LIST_ELEMENT,
};

// Result of applying Parquet LIST backward compatibility rules to the single repeated child of a
// LIST-annotated group. The repeated child can either be a physical wrapper whose only child is the
// element, or the element node itself.
struct ListElementResolution {
    // Parquet node that should be exposed as Doris ARRAY element.
    const ::parquet::schema::Node* element_node = nullptr;
    // Level state after consuming the LIST repeated child. The parent ARRAY schema keeps this state
    // to materialize offsets, empty arrays and null arrays.
    SchemaBuildContext repeated_context;
    // Level state used to build element_node. This equals repeated_context when the repeated child
    // itself is the element, and includes the wrapper's only child when standard 3-level LIST
    // encoding is stripped.
    SchemaBuildContext element_context;
    // True when element_node is the repeated child itself. The builder then uses
    // REPEATED_NODE_AS_LIST_ELEMENT so the repeated level is not interpreted as a second unrelated
    // array at the same boundary.
    bool element_is_repeated_node = false;
};

// Resolved repeated entry group of a MAP-annotated group. The entry wrapper is a physical Parquet
// encoding detail; Doris folds it into the parent MAP schema and exposes only direct [key, value]
// children.
struct MapEntryResolution {
    const ::parquet::schema::GroupNode* entry_group = nullptr;
    // Level state after consuming the repeated entry group. The parent MAP schema keeps this state
    // to materialize offsets, empty maps and null maps.
    SchemaBuildContext entry_context;
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

bool has_logical_annotation(const ::parquet::schema::Node& node) {
    const auto& logical_type = node.logical_type();
    return (node.converted_type() != ::parquet::ConvertedType::NONE &&
            node.converted_type() != ::parquet::ConvertedType::UNDEFINED) ||
           (logical_type != nullptr && logical_type->is_valid() && !logical_type->is_none());
}

bool has_structural_list_name(const std::string& list_name, const std::string& repeated_name) {
    return repeated_name == "array" || repeated_name == list_name + "_tuple";
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
    column_schema->definition_level = context.definition_level;
    column_schema->repetition_level = context.repetition_level;
    column_schema->repeated_ancestor_definition_level = context.repeated_ancestor_definition_level;
    column_schema->repeated_repetition_level = context.repeated_repetition_level;
}

SchemaBuildContext child_context(const SchemaBuildContext& parent,
                                 const ::parquet::schema::Node& child_node, int32_t child_idx) {
    SchemaBuildContext result = parent;
    result.local_id = child_idx;
    if (child_node.repetition() == ::parquet::Repetition::OPTIONAL) {
        result.definition_level++;
        result.nullable_definition_level = result.definition_level;
    }
    if (child_node.is_repeated()) {
        result.repetition_level++;
        result.definition_level++;
        result.repeated_repetition_level = result.repetition_level;
        result.repeated_ancestor_definition_level = result.definition_level;
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

// Mirrors Arrow's ResolveList() compatibility rules, but only decides which Parquet node is the
// logical LIST element. The caller still builds Doris' semantic LIST->[element] schema tree.
//
// Important cases:
// - repeated primitive: the primitive itself is the element (legacy two-level LIST).
// - repeated group with multiple children: the group itself is a STRUCT element.
// - repeated group named "array" or "<list_name>_tuple": the group itself is a STRUCT element per
//   Parquet backward compatibility rules, even when it has one child.
// - repeated group with a logical annotation, or whose only child is repeated: the group itself is
//   the element. This preserves nested LIST/MAP and repeated fields inside struct elements.
// - otherwise, strip the one-child repeated wrapper as standard three-level LIST encoding.
Status resolve_list_element_node(const ::parquet::schema::GroupNode& list_group,
                                 const SchemaBuildContext& list_context,
                                 ListElementResolution* result) {
    if (result == nullptr) {
        return Status::InvalidArgument("result is null");
    }
    if (list_group.field_count() != 1) {
        return Status::NotSupported("Unsupported parquet LIST encoding for column {}",
                                    list_group.name());
    }
    const auto& repeated_node = *list_group.field(0);
    if (!repeated_node.is_repeated()) {
        return Status::NotSupported("Unsupported parquet LIST encoding for column {}",
                                    list_group.name());
    }
    result->repeated_context = child_context(list_context, repeated_node, 0);
    if (repeated_node.is_primitive()) {
        result->element_node = &repeated_node;
        result->element_context = result->repeated_context;
        result->element_is_repeated_node = true;
        return Status::OK();
    }

    const auto& repeated_group = static_cast<const ::parquet::schema::GroupNode&>(repeated_node);
    if (repeated_group.field_count() == 0) {
        return Status::NotSupported("Unsupported parquet LIST element layout for column {}",
                                    list_group.name());
    }
    if (repeated_group.field_count() > 1 ||
        has_structural_list_name(list_group.name(), repeated_group.name()) ||
        has_logical_annotation(repeated_group)) {
        result->element_node = &repeated_node;
        result->element_context = result->repeated_context;
        result->element_is_repeated_node = true;
        return Status::OK();
    }

    const auto& only_child = *repeated_group.field(0);
    if (only_child.is_repeated()) {
        result->element_node = &repeated_node;
        result->element_context = result->repeated_context;
        result->element_is_repeated_node = true;
        return Status::OK();
    }

    result->element_node = &only_child;
    result->element_context = child_context(result->repeated_context, only_child, 0);
    result->element_is_repeated_node = false;
    return Status::OK();
}

// Resolves the repeated entry group of a MAP/MAP_KEY_VALUE node. Unlike LIST, MAP has no supported
// two-level form in this reader: Doris requires a repeated group with exactly required key and
// value children, then folds that physical entry group out of ParquetColumnSchema.
Status resolve_map_entry_group(const ::parquet::schema::GroupNode& map_group,
                               const SchemaBuildContext& map_context, MapEntryResolution* result) {
    if (result == nullptr) {
        return Status::InvalidArgument("result is null");
    }
    if (map_group.field_count() != 1) {
        return Status::NotSupported("Unsupported parquet MAP encoding for column {}",
                                    map_group.name());
    }
    const auto& entry_node = *map_group.field(0);
    if (!entry_node.is_repeated()) {
        return Status::NotSupported("Unsupported parquet MAP encoding for column {}",
                                    map_group.name());
    }
    if (entry_node.is_primitive()) {
        return Status::NotSupported("Unsupported parquet MAP key_value layout for column {}",
                                    map_group.name());
    }
    const auto& entry_group = static_cast<const ::parquet::schema::GroupNode&>(entry_node);
    if (entry_group.field_count() != 2) {
        return Status::NotSupported("Unsupported parquet MAP key_value layout for column {}",
                                    map_group.name());
    }
    if (entry_group.field(0)->repetition() != ::parquet::Repetition::REQUIRED) {
        return Status::NotSupported("Unsupported nullable parquet MAP key for column {}",
                                    map_group.name());
    }
    result->entry_group = &entry_group;
    result->entry_context = child_context(map_context, entry_node, 0);
    return Status::OK();
}

Status build_node_schema_with_mode(const ::parquet::SchemaDescriptor& schema,
                                   const ::parquet::schema::Node& node,
                                   const SchemaBuildContext& context,
                                   std::unique_ptr<ParquetColumnSchema>* result,
                                   SchemaBuildMode mode);

// Builds a semantic ARRAY schema for a simple repeated field. Arrow handles this in
// NodeToSchemaField()/GroupToSchemaField(); Doris only enables it while materializing the element
// of a LIST-annotated parent, so existing top-level repeated primitive rejection is unchanged.
//
// Example:
//   optional group a (LIST) {
//     repeated group element {
//       repeated int32 items;
//     }
//   }
// The outer LIST element is the repeated "element" group, and its repeated "items" child should be
// represented as a field of type ARRAY<INT> inside the struct element.
Status build_repeated_field_as_list_schema(const ::parquet::SchemaDescriptor& schema,
                                           const ::parquet::schema::Node& repeated_node,
                                           const SchemaBuildContext& repeated_context,
                                           std::unique_ptr<ParquetColumnSchema>* result) {
    if (result == nullptr) {
        return Status::InvalidArgument("result is null");
    }
    auto list_schema = std::make_unique<ParquetColumnSchema>();
    inherit_common_schema_state(repeated_node, repeated_context, list_schema.get());
    list_schema->kind = ParquetColumnSchemaKind::LIST;
    list_schema->definition_level = repeated_context.definition_level;
    list_schema->repetition_level = repeated_context.repetition_level;
    list_schema->repeated_repetition_level = repeated_context.repeated_repetition_level;

    std::unique_ptr<ParquetColumnSchema> element_child;
    RETURN_IF_ERROR(build_node_schema_with_mode(schema, repeated_node, repeated_context,
                                                &element_child,
                                                SchemaBuildMode::REPEATED_NODE_AS_LIST_ELEMENT));
    list_schema->type = std::make_shared<DataTypeArray>(element_child->type);
    list_schema->children.push_back(std::move(element_child));
    propagate_child_levels(list_schema.get());
    *result = std::move(list_schema);
    return Status::OK();
}

// Recursively builds ParquetColumnSchema for the given schema node and its children in Parquet
// file's metadata. The mode only affects repeated nodes that have already been selected as LIST
// elements by resolve_list_element_node(); normal file schema recursion remains strict.
Status build_node_schema_with_mode(const ::parquet::SchemaDescriptor& schema,
                                   const ::parquet::schema::Node& node,
                                   const SchemaBuildContext& context,
                                   std::unique_ptr<ParquetColumnSchema>* result,
                                   SchemaBuildMode mode) {
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
        if (mode == SchemaBuildMode::NORMAL && node.is_repeated()) {
            return Status::NotSupported("Unsupported repeated parquet LIST column {}", node.name());
        }
        column_schema->kind = ParquetColumnSchemaKind::LIST;
        ListElementResolution list_element;
        RETURN_IF_ERROR(resolve_list_element_node(group, context, &list_element));
        column_schema->definition_level = list_element.repeated_context.definition_level;
        column_schema->repetition_level = list_element.repeated_context.repetition_level;
        column_schema->repeated_repetition_level =
                list_element.repeated_context.repeated_repetition_level;
        std::unique_ptr<ParquetColumnSchema> child;
        RETURN_IF_ERROR(build_node_schema_with_mode(
                schema, *list_element.element_node, list_element.element_context, &child,
                list_element.element_is_repeated_node
                        ? SchemaBuildMode::REPEATED_NODE_AS_LIST_ELEMENT
                        : SchemaBuildMode::NORMAL));
        column_schema->type =
                nullable_if_needed(std::make_shared<DataTypeArray>(child->type), node);
        column_schema->children.push_back(std::move(child));
        propagate_child_levels(column_schema.get());
        *result = std::move(column_schema);
        return Status::OK();
    }

    if (is_map_node(node)) {
        if (mode == SchemaBuildMode::NORMAL && node.is_repeated()) {
            return Status::NotSupported("Unsupported repeated parquet MAP column {}", node.name());
        }
        column_schema->kind = ParquetColumnSchemaKind::MAP;
        MapEntryResolution map_entry;
        RETURN_IF_ERROR(resolve_map_entry_group(group, context, &map_entry));
        column_schema->definition_level = map_entry.entry_context.definition_level;
        column_schema->repetition_level = map_entry.entry_context.repetition_level;
        column_schema->repeated_repetition_level =
                map_entry.entry_context.repeated_repetition_level;
        for (int child_idx = 0; child_idx < map_entry.entry_group->field_count(); ++child_idx) {
            std::unique_ptr<ParquetColumnSchema> child;
            RETURN_IF_ERROR(build_node_schema_with_mode(
                    schema, *map_entry.entry_group->field(child_idx),
                    child_context(map_entry.entry_context, *map_entry.entry_group->field(child_idx),
                                  child_idx),
                    &child, SchemaBuildMode::NORMAL));
            column_schema->children.push_back(std::move(child));
        }
        if (column_schema->children.size() != 2) {
            return Status::NotSupported("Unsupported parquet MAP key_value layout for column {}",
                                        node.name());
        }
        auto key_type = make_nullable(column_schema->children[0]->type);
        auto value_type = make_nullable(column_schema->children[1]->type);
        column_schema->type =
                nullable_if_needed(std::make_shared<DataTypeMap>(key_type, value_type), node);
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
        const auto& child_node = *group.field(child_idx);
        std::unique_ptr<ParquetColumnSchema> child;
        const auto child_ctx = child_context(context, child_node, child_idx);
        if (mode == SchemaBuildMode::REPEATED_NODE_AS_LIST_ELEMENT && child_node.is_repeated() &&
            !is_list_node(child_node) && !is_map_node(child_node)) {
            RETURN_IF_ERROR(
                    build_repeated_field_as_list_schema(schema, child_node, child_ctx, &child));
        } else {
            RETURN_IF_ERROR(build_node_schema_with_mode(schema, child_node, child_ctx, &child,
                                                        SchemaBuildMode::NORMAL));
        }
        child_types.push_back(make_nullable(child->type));
        child_names.push_back(child->name);
        column_schema->children.push_back(std::move(child));
    }
    column_schema->type =
            nullable_if_needed(std::make_shared<DataTypeStruct>(child_types, child_names), node);
    propagate_child_levels(column_schema.get());
    *result = std::move(column_schema);
    return Status::OK();
}

Status build_node_schema(const ::parquet::SchemaDescriptor& schema,
                         const ::parquet::schema::Node& node, const SchemaBuildContext& context,
                         std::unique_ptr<ParquetColumnSchema>* result) {
    return build_node_schema_with_mode(schema, node, context, result, SchemaBuildMode::NORMAL);
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
