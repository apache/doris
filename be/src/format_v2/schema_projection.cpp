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

Status rebuild_projected_type(const DataTypePtr& original_type,
                              const std::vector<DataTypePtr>& child_types,
                              const std::vector<std::string>& child_names,
                              DataTypePtr* projected_type) {
    DORIS_CHECK(original_type != nullptr);
    DORIS_CHECK(projected_type != nullptr);
    DORIS_CHECK(child_types.size() == child_names.size());

    DataTypePtr nested_projected_type;
    const auto primitive_type = remove_nullable(original_type)->get_primitive_type();
    switch (primitive_type) {
    case TYPE_STRUCT:
        nested_projected_type = std::make_shared<DataTypeStruct>(child_types, child_names);
        break;
    case TYPE_ARRAY:
        DORIS_CHECK(child_types.size() == 1);
        nested_projected_type = std::make_shared<DataTypeArray>(child_types[0]);
        break;
    case TYPE_MAP: {
        // TODO: ?
        DORIS_CHECK(child_types.size() == 1);
        DORIS_CHECK(remove_nullable(child_types[0])->get_primitive_type() == TYPE_STRUCT);
        DORIS_CHECK(remove_nullable(original_type)->get_primitive_type() == TYPE_MAP);
        const auto* entry_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(child_types[0]).get());
        DORIS_CHECK(entry_type->get_elements().size() == 1 ||
                    entry_type->get_elements().size() == 2);
        const auto value_idx = entry_type->get_elements().size() == 1 ? 0 : 1;
        nested_projected_type = std::make_shared<DataTypeMap>(
                assert_cast<const DataTypeMap*>(remove_nullable(original_type).get())
                        ->get_key_type(),
                entry_type->get_element(value_idx));
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
        if (child_projection.field_id() == -1) {
            return Status::InvalidArgument("Empty projection path for field {}", field.name);
        }
        const auto child_it =
                std::ranges::find_if(field.children, [&](const ColumnDefinition& child) {
                    return child.file_local_id() == child_projection.field_id();
                });
        if (child_it == field.children.end()) {
            return Status::InvalidArgument("Invalid projection child id {} for field {}",
                                           child_projection.field_id(), field.name);
        }
    }
    for (const auto& child : field.children) {
        const auto child_projection_it =
                std::ranges::find_if(projection.children, [&](const LocalColumnIndex& child_proj) {
                    return child_proj.field_id() == child.file_local_id();
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

    DataTypes child_types;
    Strings child_names;
    child_types.reserve(projected_field->children.size());
    child_names.reserve(projected_field->children.size());
    for (const auto& child : projected_field->children) {
        child_types.push_back(child.type);
        child_names.push_back(child.name);
    }
    return rebuild_projected_type(field.type, child_types, child_names, &projected_field->type);
}

} // namespace doris::format
