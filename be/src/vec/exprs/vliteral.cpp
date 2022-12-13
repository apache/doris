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

#include "runtime/jsonb_value.h"
#include "runtime/large_int_value.h"
#include "util/string_parser.hpp"
#include "vec/core/field.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

namespace vectorized {

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
        case TYPE_TIMEV2:
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
        case TYPE_DATEV2: {
            DateV2Value<DateV2ValueType> value;
            value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size());
            field = value.to_date_int_val();
            break;
        }
        case TYPE_DATETIMEV2: {
            DateV2Value<DateTimeV2ValueType> value;
            value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size());
            field = value.to_date_int_val();
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
        case TYPE_JSONB: {
            DCHECK_EQ(node.node_type, TExprNodeType::JSON_LITERAL);
            DCHECK(node.__isset.json_literal);
            JsonBinaryValue value(node.json_literal.value);
            field = String(value.value(), value.size());
            break;
        }
        case TYPE_DECIMALV2: {
            DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
            DCHECK(node.__isset.decimal_literal);
            DecimalV2Value value(node.decimal_literal.value);
            field = DecimalField<Decimal128>(value.value(), value.scale());
            break;
        }
        case TYPE_DECIMAL32: {
            DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
            DCHECK(node.__isset.decimal_literal);
            DataTypePtr type_ptr = create_decimal(node.type.types[0].scalar_type.precision,
                                                  node.type.types[0].scalar_type.scale, false);
            auto val = typeid_cast<const DataTypeDecimal<Decimal32>*>(type_ptr.get())
                               ->parse_from_string(node.decimal_literal.value);
            auto scale =
                    typeid_cast<const DataTypeDecimal<Decimal32>*>(type_ptr.get())->get_scale();
            field = DecimalField<Decimal32>(val, scale);
            break;
        }
        case TYPE_DECIMAL64: {
            DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
            DCHECK(node.__isset.decimal_literal);
            DataTypePtr type_ptr = create_decimal(node.type.types[0].scalar_type.precision,
                                                  node.type.types[0].scalar_type.scale, false);
            auto val = typeid_cast<const DataTypeDecimal<Decimal64>*>(type_ptr.get())
                               ->parse_from_string(node.decimal_literal.value);
            auto scale =
                    typeid_cast<const DataTypeDecimal<Decimal64>*>(type_ptr.get())->get_scale();
            field = DecimalField<Decimal64>(val, scale);
            break;
        }
        case TYPE_DECIMAL128I: {
            DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
            DCHECK(node.__isset.decimal_literal);
            DataTypePtr type_ptr = create_decimal(node.type.types[0].scalar_type.precision,
                                                  node.type.types[0].scalar_type.scale, false);
            auto val = typeid_cast<const DataTypeDecimal<Decimal128I>*>(type_ptr.get())
                               ->parse_from_string(node.decimal_literal.value);
            auto scale =
                    typeid_cast<const DataTypeDecimal<Decimal128I>*>(type_ptr.get())->get_scale();
            field = DecimalField<Decimal128I>(val, scale);
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
    // Literal expr should return least one row.
    size_t row_size = std::max(block->rows(), _column_ptr->size());
    *result_column_id = VExpr::insert_param(block, {_column_ptr, _data_type, _expr_name}, row_size);
    return Status::OK();
}

std::string VLiteral::debug_string() const {
    std::stringstream out;
    out << "VLiteral (name = " << _expr_name;
    out << ", type = " << _data_type->get_name();
    out << ", value = (";
    for (size_t i = 0; i < _column_ptr->size(); i++) {
        if (i != 0) {
            out << ", ";
        }
        StringRef ref = _column_ptr->get_data_at(i);
        if (ref.data == nullptr) {
            out << "null";
        } else {
            switch (_type.type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                out << *(reinterpret_cast<const int8_t*>(ref.data));
            case TYPE_SMALLINT:
                out << *(reinterpret_cast<const int16_t*>(ref.data));
            case TYPE_INT: {
                out << *(reinterpret_cast<const int32_t*>(ref.data));
                break;
            }
            case TYPE_BIGINT: {
                out << *(reinterpret_cast<const int64_t*>(ref.data));
                break;
            }
            case TYPE_LARGEINT: {
                out << fmt::format("{}", *(reinterpret_cast<const __int128_t*>(ref.data)));
                break;
            }
            case TYPE_FLOAT: {
                out << *(reinterpret_cast<const float*>(ref.data));
                break;
            }
            case TYPE_TIME:
            case TYPE_TIMEV2:
            case TYPE_DOUBLE: {
                out << *(reinterpret_cast<const double_t*>(ref.data));
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                auto value = *(reinterpret_cast<const int64_t*>(ref.data));
                auto date_value = (VecDateTimeValue*)&value;
                out << *date_value;
                break;
            }
            case TYPE_DATEV2: {
                auto* value = (DateV2Value<DateV2ValueType>*)ref.data;
                out << *value;
                break;
            }
            case TYPE_DATETIMEV2: {
                auto* value = (DateV2Value<DateTimeV2ValueType>*)ref.data;
                out << *value;
                break;
            }
            case TYPE_STRING:
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_JSONB: {
                out << ref;
                break;
            }
            case TYPE_DECIMALV2: {
                DecimalV2Value value(*(reinterpret_cast<const int128_t*>(ref.data)));
                out << value;
                break;
            }
            case TYPE_DECIMAL32: {
                write_text<int32_t>(*(reinterpret_cast<const int32_t*>(ref.data)), _type.scale,
                                    out);
                break;
            }
            case TYPE_DECIMAL64: {
                write_text<int64_t>(*(reinterpret_cast<const int64_t*>(ref.data)), _type.scale,
                                    out);
                break;
            }
            case TYPE_DECIMAL128I: {
                write_text<int128_t>(*(reinterpret_cast<const int128_t*>(ref.data)), _type.scale,
                                     out);
                break;
            }
            default: {
                out << "UNKNOWN TYPE: " << int(_type.type);
                break;
            }
            }
        }
    }
    out << "))";
    return out.str();
}
} // namespace vectorized
} // namespace doris
