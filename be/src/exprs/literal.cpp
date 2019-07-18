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

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "gen_cpp/Exprs_types.h"
#include "util/string_parser.hpp"
#include "runtime/runtime_state.h"

using llvm::BasicBlock;
using llvm::Function;
using llvm::Type;
using llvm::Value;

namespace doris {

Literal::Literal(const TExprNode& node) : 
        Expr(node) {
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
        _value.large_int_val = 
            StringParser::string_to_int<__int128>(node.large_int_literal.value.c_str(),
                                                  node.large_int_literal.value.size(),
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
        _value.datetime_val.from_date_str(
            node.date_literal.value.c_str(), node.date_literal.value.size());
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        DCHECK(node.__isset.string_literal);
        _value.set_string_val(node.string_literal.value);
        break;
    case TYPE_DECIMAL: {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        _value.decimal_val = DecimalValue(node.decimal_literal.value);
        break;
    }
    case TYPE_DECIMALV2: {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        _value.decimalv2_val = DecimalV2Value(node.decimal_literal.value);
        break;
    }
    default: 
        break;
        // DCHECK(false) << "Invalid type: " << TypeToString(_type.type);
    }
}

Literal::~Literal() {
}

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
    DCHECK_EQ(_type.type, TYPE_DOUBLE) << _type;
    return DoubleVal(_value.double_val);
}

DecimalVal Literal::get_decimal_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMAL) << _type;
    DecimalVal dec_val;
    _value.decimal_val.to_decimal_val(&dec_val);
    return dec_val;
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

// IR produced for bigint literal 10:
//
// define { i8, i64 } @Literal(i8* %context, %"class.doris::TupleRow"* %row) {
// entry:
//   ret { i8, i64 } { i8 0, i64 10 }
// }
Status Literal::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }

    DCHECK_EQ(get_num_children(), 0);
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "literal", &args);
    BasicBlock* entry_block = BasicBlock::Create(codegen->context(), "entry", *fn);
    LlvmCodeGen::LlvmBuilder builder(entry_block);

    CodegenAnyVal v = CodegenAnyVal::get_non_null_val(codegen, &builder, _type);
    switch (_type.type) {
    case TYPE_BOOLEAN:
        v.set_val(_value.bool_val);
        break;
    case TYPE_TINYINT:
        v.set_val(_value.tinyint_val);
        break;
    case TYPE_SMALLINT:
        v.set_val(_value.smallint_val);
        break;
    case TYPE_INT:
        v.set_val(_value.int_val);
        break;
    case TYPE_BIGINT:
        v.set_val(_value.bigint_val);
        break;
    case TYPE_LARGEINT:
        v.set_val(_value.large_int_val);
        break;
    case TYPE_FLOAT:
        v.set_val(_value.float_val);
        break;
    case TYPE_DOUBLE:
        v.set_val(_value.double_val);
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        v.set_len(builder.getInt32(_value.string_val.len));
        v.set_ptr(codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), _value.string_val.ptr));
        break;
    case TYPE_DECIMAL: {
        Type* raw_ptr_type = codegen->decimal_val_type()->getPointerTo();
        Value* raw_ptr = codegen->cast_ptr_to_llvm_ptr(raw_ptr_type, &_value.decimal_val);
        v.set_from_raw_ptr(raw_ptr);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        Type* raw_ptr_type = codegen->decimal_val_type()->getPointerTo();
        Value* raw_ptr = codegen->cast_ptr_to_llvm_ptr(raw_ptr_type, &_value.datetime_val);
        v.set_from_raw_ptr(raw_ptr);
        break;
    }
    default:
        std::stringstream ss;
        ss << "Invalid type: " << _type;
        DCHECK(false) << ss.str();
        return Status::InternalError(ss.str());
    }

    builder.CreateRet(v.value());
    *fn = codegen->finalize_function(*fn);
    _ir_compute_fn = *fn;
    return Status::OK();
}

}
