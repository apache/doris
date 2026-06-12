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

#include "format_v2/schema_projection.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"

namespace doris::format {
namespace {

// Rebuild the complex DataType for one already-pruned semantic ColumnDefinition node.
//
// The caller has already matched the projection against ColumnDefinition::children and preserved
// the file-local child order. This helper only mirrors those projected semantic children into the
// node type. It intentionally does not understand physical format wrappers. In particular, a MAP
// node is expected to have semantic children [key, value], even if the underlying format stores a
// wrapper such as Parquet key_value/entry.
Status rebuild_semantic_projected_type(const DataTypePtr& original_type,
                                       const std::vector<ColumnDefinition>& projected_children,
                                       DataTypePtr* projected_type) {
    DORIS_CHECK(original_type != nullptr);
    DORIS_CHECK(projected_type != nullptr);

    DataTypePtr nested_projected_type;
    const auto primitive_type = remove_nullable(original_type)->get_primitive_type();
    switch (primitive_type) {
    case TYPE_STRUCT: {
        DataTypes child_types;
        Strings child_names;
        child_types.reserve(projected_children.size());
        child_names.reserve(projected_children.size());
        for (const auto& child : projected_children) {
            child_types.push_back(child.type);
            child_names.push_back(child.name);
        }
        nested_projected_type = std::make_shared<DataTypeStruct>(child_types, child_names);
        break;
    }
    case TYPE_ARRAY:
        DORIS_CHECK(projected_children.size() == 1);
        nested_projected_type = std::make_shared<DataTypeArray>(projected_children[0].type);
        break;
    case TYPE_MAP: {
        DORIS_CHECK(remove_nullable(original_type)->get_primitive_type() == TYPE_MAP);
        const auto* original_map_type =
                assert_cast<const DataTypeMap*>(remove_nullable(original_type).get());
        DataTypePtr key_type = original_map_type->get_key_type();
        DataTypePtr value_type;
        for (const auto& child : projected_children) {
            // Partial MAP projection only prunes the value subtree. The key stream must remain
            // complete because it defines entry existence and offsets when materializing ColumnMap;
            // the projected DataTypeMap also preserves the original key type instead of rebuilding
            // it from children.
            if (child.file_local_id() == 0 || child.name == "key") {
                return Status::NotSupported(
                        "MAP projection for type {} does not support key child projection",
                        original_type->get_name());
            }
            if (child.file_local_id() == 1 || child.name == "value") {
                value_type = child.type;
            }
        }
        if (value_type == nullptr) {
            return Status::NotSupported("MAP projection for type {} contains no value child",
                                        original_type->get_name());
        }
        nested_projected_type = std::make_shared<DataTypeMap>(key_type, value_type);
        break;
    }
    default:
        return Status::InvalidArgument("Cannot project children from non-complex type {}",
                                       original_type->get_name());
    }

    *projected_type = original_type->is_nullable() ? make_nullable(nested_projected_type)
                                                   : nested_projected_type;
    return Status::OK();
}

} // namespace

Status project_column_definition(const ColumnDefinition& field, const LocalColumnIndex& projection,
                                 ColumnDefinition* projected_field) {
    if (projected_field == nullptr) {
        return Status::InvalidArgument("projected_field is null");
    }
    *projected_field = field;
    if (projection.project_all_children || projection.children.empty()) {
        return Status::OK();
    }

    projected_field->children.clear();
    for (const auto& child_projection : projection.children) {
        if (child_projection.local_id() == -1) {
            return Status::InvalidArgument("Empty projection path for field {}", field.name);
        }
        const auto child_it =
                std::ranges::find_if(field.children, [&](const ColumnDefinition& child) {
                    return child.file_local_id() == child_projection.local_id();
                });
        if (child_it == field.children.end()) {
            return Status::InvalidArgument("Invalid projection child id {} for field {}",
                                           child_projection.local_id(), field.name);
        }
    }
    for (const auto& child : field.children) {
        const auto child_projection_it =
                std::ranges::find_if(projection.children, [&](const LocalColumnIndex& child_proj) {
                    return child_proj.local_id() == child.file_local_id();
                });
        if (child_projection_it == projection.children.end()) {
            continue;
        }
        ColumnDefinition projected_child;
        RETURN_IF_ERROR(project_column_definition(child, *child_projection_it, &projected_child));
        projected_field->children.push_back(std::move(projected_child));
    }
    if (projected_field->children.empty()) {
        return Status::NotSupported("Projection for field {} contains no children", field.name);
    }

    return rebuild_semantic_projected_type(field.type, projected_field->children,
                                           &projected_field->type);
}

} // namespace doris::format
