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

#include "format/generic_reader.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

namespace doris {

Status GenericReader::init_column_descriptors(
        const TFileScanRangeParams& params, const TFileRangeDesc& range,
        const std::vector<ColumnDescriptor>& column_descs, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor, RuntimeState* state, bool fill_partition_from_path,
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_col_descs,
        std::unordered_map<std::string, VExprContextSPtr>* out_missing_col_descs,
        std::unordered_set<std::string>* out_missing_cols,
        std::unordered_map<std::string, DataTypePtr>* out_name_to_col_type) {
    // 1. Get columns from reader (which columns exist in the file, which are missing)
    RETURN_IF_ERROR(get_columns(out_name_to_col_type, out_missing_cols));

    // 2. Build default value expressions for missing columns
    std::unordered_map<std::string, VExprContextSPtr> col_default_value_ctx;
    for (auto* slot_desc : tuple_descriptor->slots()) {
        auto it = params.default_value_of_src_slot.find(slot_desc->id());
        if (it != params.default_value_of_src_slot.end()) {
            VExprContextSPtr ctx;
            if (!it->second.nodes.empty()) {
                RETURN_IF_ERROR(VExpr::create_expr_tree(it->second, ctx));
                RETURN_IF_ERROR(ctx->prepare(state, *row_descriptor));
                RETURN_IF_ERROR(ctx->open(state));
            }
            col_default_value_ctx.emplace(slot_desc->col_name(), ctx);
        }
    }

    // 3. Build missing_col_descs from missing columns
    out_missing_col_descs->clear();
    for (const auto& col_desc : column_descs) {
        if (!col_desc.is_file_slot) {
            continue;
        }
        if (out_missing_cols->contains(col_desc.name) ||
            out_missing_cols->contains(to_lower(col_desc.name))) {
            auto it = col_default_value_ctx.find(col_desc.name);
            if (it != col_default_value_ctx.end()) {
                out_missing_col_descs->emplace(col_desc.name, it->second);
            } else {
                // No default value: fill with null
                out_missing_col_descs->emplace(col_desc.name, nullptr);
            }
        }
    }

    // 4. Call set_fill_columns on the reader
    if (fill_partition_from_path) {
        RETURN_IF_ERROR(set_fill_columns(partition_col_descs, *out_missing_col_descs));
    } else {
        RETURN_IF_ERROR(set_fill_columns({}, *out_missing_col_descs));
    }
    return Status::OK();
}

} // namespace doris
