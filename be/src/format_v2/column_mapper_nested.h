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

#pragma once

#include <cstddef>
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "exprs/vexpr_fwd.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/column_mapper.h"
#include "format_v2/file_reader.h"

namespace doris::format {

struct StructChildSelector {
    bool by_name = true;
    std::string name;
    size_t ordinal = 0;
};

struct NestedStructPath {
    GlobalIndex root_global_index;
    std::vector<StructChildSelector> selectors;
};

struct ResolvedNestedStructPath {
    LocalColumnIndex file_projection;
    std::vector<std::string> file_child_names;
    std::vector<DataTypePtr> file_child_types;
};

// A split-local literal produced by slot-literal predicate localization. This wrapper keeps the
// original table literal so a cloned conjunct can be localized again for another split.
class SplitLocalFileLiteral final : public VLiteral {
public:
    SplitLocalFileLiteral(const DataTypePtr& file_type, const Field& file_field,
                          DataTypePtr original_type, Field original_field);

    const DataTypePtr& original_type() const { return _original_type; }
    const Field& original_field() const { return _original_field; }
    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        Field file_field;
        get_column_ptr()->get(0, file_field);
        *cloned_expr = std::make_shared<SplitLocalFileLiteral>(_data_type, file_field,
                                                               _original_type, _original_field);
        return Status::OK();
    }

private:
    DataTypePtr _original_type;
    Field _original_field;
};

GlobalIndex slot_ref_global_index(const VSlotRef& slot_ref);
bool is_struct_element_expr(const VExprSPtr& expr);
Field literal_field(const VExprSPtr& literal_expr);

bool resolve_nested_struct_path_for_file(const NestedStructPath& path,
                                         const std::vector<ColumnMapping>& mappings,
                                         ResolvedNestedStructPath* resolved,
                                         bool require_scan_projection = false);

bool resolve_nested_struct_expr_for_file(const VExprSPtr& expr,
                                         const std::vector<ColumnMapping>& mappings,
                                         ResolvedNestedStructPath* resolved);

void collect_nested_struct_paths(const VExprSPtr& expr, std::vector<NestedStructPath>* paths);

std::vector<const ColumnMapping*> present_child_mappings_in_file_order(
        const std::vector<ColumnMapping>& child_mappings);

Status build_file_child_projection_from_schema(const std::vector<ColumnDefinition>& children,
                                               std::span<const StructChildSelector> selectors,
                                               LocalColumnIndex* projection);

} // namespace doris::format
