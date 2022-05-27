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

#include "vec/exprs/vliteral.h"

#include <fmt/format.h>

#include "util/string_parser.hpp"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"
namespace doris::vectorized {

void VLiteral::init(const TExprNode& node) {
    Field field;
    if (node.node_type != TExprNodeType::NULL_LITERAL) {
        switch (_type.type) {
        case TYPE_BOOLEAN: {
            DCHECK_EQ(node.node_type, TExprNodeType::BOOL_LITERAL);
            DCHECK(node.__isset.bool_literal);
            field = Int8(node.bool_literal.value);
            break;
        }
        case TYPE_TINYINT: {
            DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
            DCHECK(node.__isset.int_literal);
            field = Int8(node.int_literal.value);
            break;
        }
        case TYPE_SMALLINT: {
            DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
            DCHECK(node.__isset.int_literal);
            field = Int16(node.int_literal.value);
            break;
        }
        case TYPE_INT: {
            DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
            DCHECK(node.__isset.int_literal);
            field = Int32(node.int_literal.value);
            break;
        }
        case TYPE_BIGINT: {
            DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
            DCHECK(node.__isset.int_literal);
            field = Int64(node.int_literal.value);
            break;
        }
        case TYPE_LARGEINT: {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            DCHECK_EQ(node.node_type, TExprNodeType::LARGE_INT_LITERAL);
            __int128_t value = StringParser::string_to_int<__int128>(
                    node.large_int_literal.value.c_str(), node.large_int_literal.value.size(),
                    &parse_result);
            if (parse_result != StringParser::PARSE_SUCCESS) {
                value = MAX_INT128;
            }
            field = Int128(value);
            break;
        }
        case TYPE_FLOAT: {
            DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
            DCHECK(node.__isset.float_literal);
            field = Float32(node.float_literal.value);
            break;
        }
        case TYPE_TIME:
        case TYPE_DOUBLE: {
            DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
            DCHECK(node.__isset.float_literal);
            field = Float64(node.float_literal.value);
            break;
        }
        case TYPE_DATE: {
            VecDateTimeValue value;
            value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size());
            value.cast_to_date();
            field = Int64(*reinterpret_cast<__int64_t*>(&value));
            break;
        }
        case TYPE_DATETIME: {
            VecDateTimeValue value;
            value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size());
            value.to_datetime();
            field = Int64(*reinterpret_cast<__int64_t*>(&value));
            break;
        }
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
            DCHECK(node.__isset.string_literal);
            field = node.string_literal.value;
            break;
        }
        case TYPE_DECIMALV2: {
            DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
            DCHECK(node.__isset.decimal_literal);
            DecimalV2Value value(node.decimal_literal.value);
            field = DecimalField<Decimal128>(value.value(), value.scale());
            break;
        }
        default: {
            DCHECK(false) << "Invalid type: " << _type.type;
            break;
        }
        }
    }
    _column_ptr = _data_type->create_column_const(1, field);
}

Status VLiteral::execute(VExprContext* context, vectorized::Block* block, int* result_column_id) {
    int rows = block->rows();
    if (rows < 1) {
        rows = 1;
    }
    size_t res = block->columns();
    block->insert({_column_ptr->clone_resized(rows), _data_type, _expr_name});
    *result_column_id = res;
    return Status::OK();
}
} // namespace doris::vectorized
