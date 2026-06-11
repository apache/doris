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

#include "format_v2/jni/jdbc_reader.h"

#include <memory>
#include <utility>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vexpr_context.h"
#include "format_v2/table_reader.h"
#include "util/jdbc_utils.h"

namespace doris::jdbc {

std::string JdbcJniReader::connector_class() const {
    return "org/apache/doris/jdbc/JdbcJniScanner";
}

Status JdbcJniReader::prepare_split(const format::SplitReadOptions& options) {
    _jdbc_params.clear();
    _current_range = options.current_range;
    if (options.current_range.__isset.table_format_params &&
        options.current_range.table_format_params.table_format_type == "jdbc") {
        _jdbc_params = std::map<std::string, std::string>(
                options.current_range.table_format_params.jdbc_params.begin(),
                options.current_range.table_format_params.jdbc_params.end());
    }
    return format::JniTableReader::prepare_split(options);
}

// need pass to the java side, so the java scanner can parse the params and construct the JDBC connection
Status JdbcJniReader::build_scanner_params(std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    *params = _jdbc_params;
    if (params->contains("jdbc_driver_url")) {
        std::string resolved;
        if (JdbcUtils::resolve_driver_url((*params)["jdbc_driver_url"], &resolved).ok()) {
            (*params)["jdbc_driver_url"] = resolved;
        }
    }
    return Status::OK();
}

Status JdbcJniReader::build_jni_columns(
        std::vector<format::JniTableReader::JniColumn>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    columns->reserve(_projected_columns.size());
    for (size_t i = 0; i < _projected_columns.size(); ++i) {
        const auto& table_column = _projected_columns[i];
        const auto primitive_type = remove_nullable(table_column.type)->get_primitive_type();
        columns->push_back({
                .java_name = table_column.name,
                .output_index = i,
                .output_type = table_column.type,
                .transfer_type = _transfer_type_for(table_column.type),
                .replace_type = _replace_type_for(primitive_type),
        });
    }
    return Status::OK();
}

Status JdbcJniReader::finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows) {
    DORIS_CHECK(jni_block != nullptr);
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    const auto original_rows = *rows;
    const auto& columns = jni_columns();
    DORIS_CHECK(columns.size() == jni_block->columns());

    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& column = columns[i];
        DORIS_CHECK(column.output_type != nullptr);
        DORIS_CHECK(column.output_index < output_block->columns());
        if (_is_special_type(remove_nullable(column.output_type)->get_primitive_type())) {
            RETURN_IF_ERROR(_cast_string_to_special_type(column, jni_block, i, output_block,
                                                         original_rows));
            continue;
        }
        output_block->get_by_position(column.output_index).type = column.output_type;
        output_block->replace_by_position(column.output_index,
                                          jni_block->get_by_position(i).column);
    }
    DORIS_CHECK(output_block->rows() == original_rows);
    if (!_conjuncts.empty()) {
        RETURN_IF_ERROR(
                VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    }
    *rows = output_block->rows();
    return Status::OK();
}

int64_t JdbcJniReader::self_split_weight() const {
    return _current_range.__isset.self_split_weight ? _current_range.self_split_weight : -1;
}

std::string JdbcJniReader::_replace_type_for(PrimitiveType type) const {
    switch (type) {
    case PrimitiveType::TYPE_BITMAP:
        return "bitmap";
    case PrimitiveType::TYPE_HLL:
        return "hll";
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return "quantile_state";
    case PrimitiveType::TYPE_JSONB:
        return "jsonb";
    default:
        return "not_replace";
    }
}

bool JdbcJniReader::_is_special_type(PrimitiveType type) const {
    return type == PrimitiveType::TYPE_BITMAP || type == PrimitiveType::TYPE_HLL ||
           type == PrimitiveType::TYPE_QUANTILE_STATE || type == PrimitiveType::TYPE_JSONB;
}

DataTypePtr JdbcJniReader::_transfer_type_for(const DataTypePtr& output_type) const {
    DORIS_CHECK(output_type != nullptr);
    if (!_is_special_type(remove_nullable(output_type)->get_primitive_type())) {
        return output_type;
    }
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    if (output_type->is_nullable()) {
        string_type = make_nullable(string_type);
    }
    return string_type;
}

Status JdbcJniReader::_cast_string_to_special_type(const format::JniTableReader::JniColumn& column,
                                                   Block* jni_block, size_t jni_column_index,
                                                   Block* output_block, size_t rows) {
    DORIS_CHECK(column.output_type != nullptr);
    DORIS_CHECK(column.transfer_type != nullptr);
    const auto target_type = column.output_type;
    const auto target_type_name = target_type->get_name();

    ColumnPtr input_column = jni_block->get_by_position(jni_column_index).column;
    ColumnPtr cast_param = target_type->create_column_const_with_default_value(1);

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(std::move(input_column), column.transfer_type,
                                   "java.sql.String");
    argument_template.emplace_back(std::move(cast_param), target_type, target_type_name);

    FunctionBasePtr cast_function = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, make_nullable(target_type));
    if (cast_function == nullptr) {
        return Status::InternalError("Failed to find CAST function for type {}", target_type_name);
    }

    Block cast_block(argument_template);
    const auto result_idx = cast_set<uint32_t>(cast_block.columns());
    cast_block.insert({nullptr, make_nullable(target_type), "cast_result"});
    RETURN_IF_ERROR(
            cast_function->execute(nullptr, cast_block, {0}, result_idx, cast_set<int>(rows)));

    auto result_column = cast_block.get_by_position(result_idx).column;
    output_block->get_by_position(column.output_index).type = target_type;
    if (target_type->is_nullable()) {
        output_block->replace_by_position(column.output_index, result_column);
    } else {
        const auto* nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
        output_block->replace_by_position(column.output_index,
                                          nullable_column->get_nested_column_ptr());
    }
    return Status::OK();
}

} // namespace doris::jdbc
