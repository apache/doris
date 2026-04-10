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

#include "jdbc_jni_reader.h"

#include <sstream>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/simple_function_factory.h"
#include "format/jni/jni_data_bridge.h"
#include "runtime/descriptors.h"
#include "util/jdbc_utils.h"

namespace doris {

JdbcJniReader::JdbcJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile,
                             const std::map<std::string, std::string>& jdbc_params)
        : JniReader(
                  file_slot_descs, state, profile, "org/apache/doris/jdbc/JdbcJniScanner",
                  [&]() {
                      std::ostringstream required_fields;
                      std::ostringstream columns_types;
                      std::ostringstream replace_string;
                      int index = 0;
                      for (const auto& desc : file_slot_descs) {
                          std::string field = desc->col_name();
                          std::string type =
                                  JniDataBridge::get_jni_type_with_different_string(desc->type());

                          // Determine replace_string for special types
                          // (bitmap, hll, quantile_state, jsonb)
                          std::string replace_type = "not_replace";
                          auto ptype = desc->type()->get_primitive_type();
                          if (ptype == PrimitiveType::TYPE_BITMAP) {
                              replace_type = "bitmap";
                          } else if (ptype == PrimitiveType::TYPE_HLL) {
                              replace_type = "hll";
                          } else if (ptype == PrimitiveType::TYPE_JSONB) {
                              replace_type = "jsonb";
                          } else if (ptype == PrimitiveType::TYPE_QUANTILE_STATE) {
                              replace_type = "quantile_state";
                          }

                          if (index == 0) {
                              required_fields << field;
                              columns_types << type;
                              replace_string << replace_type;
                          } else {
                              required_fields << "," << field;
                              columns_types << "#" << type;
                              replace_string << "," << replace_type;
                          }
                          index++;
                      }
                      // Merge JDBC-specific params with schema params
                      std::map<std::string, std::string> params = jdbc_params;
                      params["required_fields"] = required_fields.str();
                      params["columns_types"] = columns_types.str();
                      params["replace_string"] = replace_string.str();
                      // Resolve jdbc_driver_url to absolute file:// URL
                      if (params.count("jdbc_driver_url")) {
                          std::string resolved;
                          if (JdbcUtils::resolve_driver_url(params["jdbc_driver_url"], &resolved)
                                      .ok()) {
                              params["jdbc_driver_url"] = resolved;
                          }
                      }
                      return params;
                  }(),
                  [&]() {
                      std::vector<std::string> names;
                      for (const auto& desc : file_slot_descs) {
                          names.emplace_back(desc->col_name());
                      }
                      return names;
                  }()),
          _jdbc_params(jdbc_params) {}

Status JdbcJniReader::init_reader() {
    return open(_state, _profile);
}

bool JdbcJniReader::_is_special_type(PrimitiveType type) {
    return type == PrimitiveType::TYPE_BITMAP || type == PrimitiveType::TYPE_HLL ||
           type == PrimitiveType::TYPE_QUANTILE_STATE || type == PrimitiveType::TYPE_JSONB;
}

Status JdbcJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    // Identify columns with special types (bitmap, HLL, quantile_state, JSONB)
    // and temporarily replace them with string columns for JNI data transfer.
    // This follows the same pattern as the old vjdbc_connector.cpp _get_reader_params.
    struct SpecialColumnInfo {
        int block_idx;
        DataTypePtr original_type;
        ColumnPtr original_column;
    };
    std::vector<SpecialColumnInfo> special_columns;

    auto name_to_pos_map = block->get_name_to_pos_map();
    const auto& slots = _file_slot_descs;
    for (size_t i = 0; i < slots.size(); ++i) {
        auto* slot = slots[i];
        auto ptype = slot->type()->get_primitive_type();
        if (_is_special_type(ptype)) {
            // Find the block index for this column
            int block_idx = name_to_pos_map[slot->col_name()];
            auto& col_with_type = block->get_by_position(block_idx);

            SpecialColumnInfo info;
            info.block_idx = block_idx;
            info.original_type = col_with_type.type;
            info.original_column = col_with_type.column;
            special_columns.push_back(info);

            // Replace block column with string type
            DataTypePtr string_type = std::make_shared<DataTypeString>();
            if (slot->is_nullable()) {
                string_type = make_nullable(string_type);
            }
            block->get_by_position(block_idx).column =
                    string_type->create_column()->convert_to_full_column_if_const();
            block->get_by_position(block_idx).type = string_type;
        }
    }

    // Call parent to do the actual JNI read with string columns
    RETURN_IF_ERROR(JniReader::get_next_block(block, read_rows, eof));

    // Cast string columns back to their target types
    if (*read_rows > 0 && !special_columns.empty()) {
        for (size_t i = 0; i < slots.size(); ++i) {
            auto* slot = slots[i];
            auto ptype = slot->type()->get_primitive_type();
            if (_is_special_type(ptype)) {
                int block_idx = name_to_pos_map[slot->col_name()];
                RETURN_IF_ERROR(_cast_string_to_special_type(slot, block, block_idx,
                                                             static_cast<int>(*read_rows)));
            }
        }
    } else if (special_columns.empty()) {
        // No special columns, nothing to do
    } else {
        // No rows read but we replaced columns, restore original types for next call
        for (auto& info : special_columns) {
            block->get_by_position(info.block_idx).type = info.original_type;
            block->get_by_position(info.block_idx).column = info.original_column;
        }
    }

    return Status::OK();
}

Status JdbcJniReader::_cast_string_to_special_type(const SlotDescriptor* slot_desc, Block* block,
                                                   int column_index, int num_rows) {
    DataTypePtr target_data_type = slot_desc->get_data_type_ptr();
    std::string target_data_type_name = target_data_type->get_name();

    // Build input string type (nullable if slot is nullable)
    DataTypePtr input_string_type;
    if (slot_desc->is_nullable()) {
        input_string_type = make_nullable(std::make_shared<DataTypeString>());
    } else {
        input_string_type = std::make_shared<DataTypeString>();
    }

    auto& input_col = block->get_by_position(column_index).column;

    // Build CAST function arguments
    DataTypePtr cast_param_data_type = target_data_type;
    ColumnPtr cast_param = cast_param_data_type->create_column_const_with_default_value(1);

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(std::move(input_col), input_string_type, "java.sql.String");
    argument_template.emplace_back(cast_param, cast_param_data_type, target_data_type_name);

    FunctionBasePtr func_cast = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(target_data_type));

    if (func_cast == nullptr) {
        return Status::InternalError("Failed to find CAST function for type {}",
                                     target_data_type_name);
    }

    Block cast_block(argument_template);
    int result_idx = cast_block.columns();
    cast_block.insert({nullptr, make_nullable(target_data_type), "cast_result"});
    RETURN_IF_ERROR(func_cast->execute(nullptr, cast_block, {0}, result_idx, num_rows));

    auto res_col = cast_block.get_by_position(result_idx).column;
    block->get_by_position(column_index).type = target_data_type;
    if (target_data_type->is_nullable()) {
        block->replace_by_position(column_index, res_col);
    } else {
        auto nested_ptr =
                reinterpret_cast<const ColumnNullable*>(res_col.get())->get_nested_column_ptr();
        block->replace_by_position(column_index, nested_ptr);
    }

    return Status::OK();
}

} // namespace doris
