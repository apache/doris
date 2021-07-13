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

#include "exprs/literal.h"

#include <string>

#include "gen_cpp/Exprs_types.h"
#include "runtime/collection_value.h"
#include "runtime/runtime_state.h"
#include "util/string_parser.hpp"

namespace doris {

Literal::Literal(const TExprNode& node) : Expr(node) {
    switch (_type.type) {
    case TYPE_BOOLEAN:
        DCHECK_EQ(node.node_type, TExprNodeType::BOOL_LITERAL);
        DCHECK(node.__isset.bool_literal);
        _value.bool_val = node.bool_literal.value;
        break;
    case TYPE_TINYINT:
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        _value.tinyint_val = node.int_literal.value;
        break;
    case TYPE_SMALLINT:
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        _value.smallint_val = node.int_literal.value;
        break;
    case TYPE_INT:
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        _value.int_val = node.int_literal.value;
        break;
    case TYPE_BIGINT:
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        _value.bigint_val = node.int_literal.value;
        break;
    case TYPE_LARGEINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        DCHECK_EQ(node.node_type, TExprNodeType::LARGE_INT_LITERAL);
        _value.large_int_val = StringParser::string_to_int<__int128>(
                node.large_int_literal.value.c_str(), node.large_int_literal.value.size(),
                &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            _value.large_int_val = MAX_INT128;
        }
        break;
    }
    case TYPE_FLOAT:
        DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        DCHECK(node.__isset.float_literal);
        _value.float_val = node.float_literal.value;
        break;
    case TYPE_DOUBLE:
    case TYPE_TIME:
        DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        DCHECK(node.__isset.float_literal);
        _value.double_val = node.float_literal.value;
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        _value.datetime_val.from_date_str(node.date_literal.value.c_str(),
                                          node.date_literal.value.size());
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        DCHECK(node.__isset.string_literal);
        _value.set_string_val(node.string_literal.value);
        break;

    case TYPE_DECIMALV2: {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        _value.decimalv2_val = DecimalV2Value(node.decimal_literal.value);
        break;
    }
    case TYPE_ARRAY: {
        DCHECK_EQ(node.node_type, TExprNodeType::ARRAY_LITERAL);
        // init in prepare
        break;
    }
    default:
        break;
        // DCHECK(false) << "Invalid type: " << TypeToString(_type.type);
    }
}

Literal::~Literal() {}

BooleanVal Literal::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BOOLEAN) << _type;
    return BooleanVal(_value.bool_val);
}

TinyIntVal Literal::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_TINYINT) << _type;
    return TinyIntVal(_value.tinyint_val);
}

SmallIntVal Literal::get_small_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_SMALLINT) << _type;
    return SmallIntVal(_value.smallint_val);
}

IntVal Literal::get_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_INT) << _type;
    return IntVal(_value.int_val);
}

BigIntVal Literal::get_big_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BIGINT) << _type;
    return BigIntVal(_value.bigint_val);
}

LargeIntVal Literal::get_large_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_LARGEINT) << _type;
    return LargeIntVal(_value.large_int_val);
}

FloatVal Literal::get_float_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_FLOAT) << _type;
    return FloatVal(_value.float_val);
}

DoubleVal Literal::get_double_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.type == TYPE_DOUBLE || _type.type == TYPE_TIME) << _type;
    return DoubleVal(_value.double_val);
}

DecimalV2Val Literal::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMALV2) << _type;
    DecimalV2Val dec_val;
    _value.decimalv2_val.to_decimal_val(&dec_val);
    return dec_val;
}

DateTimeVal Literal::get_datetime_val(ExprContext* context, TupleRow* row) {
    DateTimeVal dt_val;
    _value.datetime_val.to_datetime_val(&dt_val);
    return dt_val;
}

StringVal Literal::get_string_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_string_type()) << _type;
    StringVal str_val;
    _value.string_val.to_string_val(&str_val);
    return str_val;
}

CollectionVal Literal::get_array_val(ExprContext* context, TupleRow*) {
    DCHECK(_type.is_collection_type());
    CollectionVal val;
    _value.array_val.to_collection_val(&val);
    return val;
}

Status Literal::prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, row_desc, context));

    if (type().type == TYPE_ARRAY) {
        DCHECK_EQ(type().children.size(), 1) << "array children type not 1";
        // init array value
        auto td = type().children.at(0).type;
        RETURN_IF_ERROR(CollectionValue::init_collection(state->obj_pool(), get_num_children(), td,
                                                         &_value.array_val));
        // init every item
        for (int i = 0; i < get_num_children(); ++i) {
            Expr* children = get_child(i);
            RETURN_IF_ERROR(_value.array_val.set(i, td, children->get_const_val(context)));
        }
    }

    return Status::OK();
}
} // namespace doris
