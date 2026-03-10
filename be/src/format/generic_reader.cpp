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

Status GenericReader::init_column_descriptors(const TFileScanRangeParams& params,
                                              const TFileRangeDesc& range,
                                              const std::vector<ColumnDescriptor>& column_descs,
                                              const TupleDescriptor* tuple_descriptor,
                                              const RowDescriptor* row_descriptor,
                                              RuntimeState* state) {
    std::unordered_map<std::string, VExprContextSPtr> missing_col_descs;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_col_descs;
    std::unordered_set<std::string> missing_cols;

    std::unordered_map<std::string, DataTypePtr> name_to_col_type;
    RETURN_IF_ERROR(get_columns(&name_to_col_type, &missing_cols));

    // Determine partition and missing columns exactly like FileScanner used to do
    std::unordered_map<std::string, int> partition_name_to_key_index;
    if (range.__isset.columns_from_path_keys) {
        int index = 0;
        for (const auto& key : range.columns_from_path_keys) {
            partition_name_to_key_index.emplace(key, index++);
        }
    }

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

    for (const auto& col_desc : column_descs) {
        if (partition_name_to_key_index.contains(col_desc.name)) {
            if (col_desc.is_file_slot) {
                // Ignore as partition if it's also a file slot
                continue;
            }
            int index = partition_name_to_key_index[col_desc.name];
            if (range.__isset.columns_from_path && index < range.columns_from_path.size()) {
                partition_col_descs.emplace(
                        col_desc.name,
                        std::make_tuple(range.columns_from_path[index], col_desc.slot_desc));
            }
        }

        if (col_desc.is_file_slot) {
            if (missing_cols.contains(col_desc.name) ||
                missing_cols.contains(to_lower(col_desc.name))) {
                auto it = col_default_value_ctx.find(col_desc.name);
                if (it != col_default_value_ctx.end()) {
                    missing_col_descs.emplace(col_desc.name, it->second);
                } else {
                    return Status::InternalError(
                            "failed to find default value expr for missing slot: {}",
                            col_desc.name);
                }
            }
        }
    }

    RETURN_IF_ERROR(set_fill_columns(partition_col_descs, missing_col_descs));
    return Status::OK();
}

} // namespace doris
