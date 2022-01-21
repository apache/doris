#!/usr/bin/env python
# encoding: utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
# This script will generate the implementation of the simple functions for the BE.
# These include:
#   - Arithmetic functions
#   - Binary functions
#   - Cast functions
#
# The script outputs (run: 'src/common/function/gen_functions.py')
#   - header and implemention for above functions:
#     - src/gen_cpp/opcode/functions.[h/cc]
#   - python file that contains the metadata for those functions:
#     - src/gen_cpp/generated_functions.py
"""

import string
import os
import errno

unary_op = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.${result_field} = ${native_op} *val;\n\
  return &e->_result.${result_field};\n\
}\n\n")


binary_op_divid = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->get_value(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->get_value(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  double value= *val2;\n\
  if (value == 0) return NULL;\n\
  e->_result.${result_field} = (*val1 ${native_op} *val2);\n\
  return &e->_result.${result_field};\n\
}\n\n")

binary_op = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->get_value(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->get_value(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  e->_result.${result_field} = (*val1 ${native_op} *val2);\n\
  return &e->_result.${result_field};\n\
}\n\n")

double_mod = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->get_value(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->get_value(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  double value= *val2;\n\
  if (value == 0) return NULL;\n\
  e->_result.${result_field} = fmod(*val1, *val2);\n\
  return &e->_result.${result_field};\n\
}\n\n")

binary_func = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->get_value(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->get_value(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  e->_result.${result_field} = val1->${native_func}(*val2);\n\
  return &e->_result.${result_field};\n\
}\n\n")

float_to_decimal = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.${result_field}.assign_from_float(*val);;\n\
  return &e->_result.${result_field};\n\
}\n\n")

double_to_decimal = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.${result_field}.assign_from_double(*val);;\n\
  return &e->_result.${result_field};\n\
}\n\n")

cast = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.${result_field} = *val;\n\
  return &e->_result.${result_field};\n\
}\n\n")

string_to_int = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  StringParser::ParseResult result;\n\
  e->_result.${result_field} = \
      StringParser::string_to_int<${native_type2}>(val->ptr, val->len, &result);\n\
  if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) return NULL;\n\
  return &e->_result.${result_field};\n\
}\n\n")

string_to_float = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  StringParser::ParseResult result;\n\
  e->_result.${result_field} = \
      StringParser::string_to_float<${native_type2}>(val->ptr, val->len, &result);\n\
  if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) return NULL;\n\
  return &e->_result.${result_field};\n\
}\n\n")

numeric_to_date = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  DateTimeValue *date_val = &e->_result.${result_field};\n\
  if (!date_val->from_date_int64(*val)) {\n\
    return NULL;\n\
  }\n\
  date_val->cast_to_date();\n\
  return date_val;\n\
}\n\n")

string_to_date = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  DateTimeValue *date_val = &e->_result.${result_field};\n\
  if (!date_val->from_date_str(val->ptr, val->len)) {\n\
    return NULL;\n\
  }\n\
  date_val->cast_to_date();\n\
  return date_val;\n\
}\n\n")

datetime_to_date = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  DateTimeValue *date_val = &e->_result.${result_field};\n\
  *date_val = *val;\n\
  date_val->cast_to_date();\n\
  return date_val;\n\
}\n\n")

numeric_to_datetime = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  DateTimeValue *date_val = &e->_result.${result_field};\n\
  if (!date_val->from_date_int64(*val)) {\n\
    return NULL;\n\
  }\n\
  date_val->to_datetime();\n\
  return date_val;\n\
}\n\n")

string_to_datetime = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  DateTimeValue *date_val = &e->_result.${result_field};\n\
  if (!date_val->from_date_str(val->ptr, val->len)) {\n\
    return NULL;\n\
  }\n\
  date_val->to_datetime();\n\
  return date_val;\n\
}\n\n")

date_to_datetime = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  DateTimeValue *date_val = &e->_result.${result_field};\n\
  *date_val = *val;\n\
  date_val->to_datetime();\n\
  return date_val;\n\
}\n\n")

datetime_to_numeric = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.${result_field} = val->to_int64();\n\
  return &e->_result.${result_field};\n\
}\n\n")

decimal_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.set_string_val(val->to_string());\n\
  return &e->_result.${result_field};\n\
}\n\n")

datetime_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  char buf[64];\n\
  val->to_string(buf);\n\
  e->_result.set_string_val(buf);\n\
  return &e->_result.${result_field};\n\
}\n\n")

numeric_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  e->_result.set_string_val(std::to_string(*val));\n\
  return &e->_result.${result_field};\n\
}\n\n")

largeint_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  char buf[64];\n\
  int len = 64;\n\
  char *str = LargeIntValue::to_string(*val, buf, &len);\n\
  e->_result.set_string_val(std::string(str, len));\n\
  return &e->_result.${result_field};\n\
}\n\n")

float_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  char buf[64];\n\
  my_gcvt(*val, MY_GCVT_ARG_FLOAT, 64, buf, NULL);\n\
  e->_result.set_string_val(buf);\n\
  return &e->_result.${result_field};\n\
}\n\n")

double_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  char buf[64];\n\
  my_gcvt(*val, MY_GCVT_ARG_DOUBLE, 64, buf, NULL);\n\
  e->_result.set_string_val(buf);\n\
  return &e->_result.${result_field};\n\
}\n\n")

# Need to special case tinyint.  boost thinks it is a char and handles it differently.
# e.g. '0' is written as an empty string.
string_to_tinyint = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  string tmp(val->ptr, val->len);\n\
  try {\n\
    e->_result.${result_field} = static_cast<int8_t>(lexical_cast<int16_t>(tmp));\n\
  } catch (bad_lexical_cast &) {\n\
    return NULL;\n\
  }\n\
  return &e->_result.${result_field};\n\
}\n\n")

tinyint_to_string = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->get_value(row));\n\
  if (val == NULL) return NULL;\n\
  int64_t tmp_val = *val;\n\
  e->_result.set_string_val(lexical_cast<string>(tmp_val));\n\
  return &e->_result.${result_field};\n\
}\n\n")

case = string.Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  CaseExpr* expr = static_cast<CaseExpr*>(e);\n\
  int num_children = e->get_num_children();\n\
  int loop_end = (expr->has_else_expr()) ? num_children - 1 : num_children;\n\
  // Make sure we set the right compute function.\n\
  DCHECK_EQ(expr->has_case_expr(), true);\n\
  // Need at least case, when and then expr, and optionally an else.\n\
  DCHECK_GE(num_children, (expr->has_else_expr()) ? 4 : 3);\n\
  // All case and when exprs return the same type (we guaranteed that during analysis).\n\
  void* case_val = e->children()[0]->get_value(row);\n\
  if (case_val == NULL) {\n\
    if (expr->has_else_expr()) {\n\
      // Return else value.\n\
      return e->children()[num_children - 1]->get_value(row);\n\
    } else {\n\
      return NULL;\n\
    }\n\
  }\n\
  for (int i = 1; i < loop_end; i += 2) {\n\
    ${native_type1}* when_val =\n\
        reinterpret_cast<${native_type1}*>(e->children()[i]->get_value(row));\n\
    if (when_val == NULL) continue;\n\
    if (*reinterpret_cast<${native_type1}*>(case_val) == *when_val) {\n\
      // Return then value.\n\
      return e->children()[i + 1]->get_value(row);\n\
    }\n\
  }\n\
  if (expr->has_else_expr()) {\n\
    // Return else value.\n\
    return e->children()[num_children - 1]->get_value(row);\n\
  }\n\
  return NULL;\n\
}\n\n")

python_template = string.Template("\
  ['${fn_name}', '${return_type}', [${args}], 'ComputeFunctions::${fn_signature}', []], \n")

# Mapping of function to template
templates = {
  'Add': binary_op,
  'Subtract': binary_op,
  'Multiply': binary_op,
  'Divide': binary_op_divid,
  'Int_Divide': binary_op_divid,
  'Mod': binary_op_divid,
  'BitAnd': binary_op,
  'BitXor': binary_op,
  'BitOr': binary_op,
  'BitNot': unary_op,
  'Eq': binary_op,
  'Ne': binary_op,
  'Ge': binary_op,
  'Gt': binary_op,
  'Lt': binary_op,
  'Le': binary_op,
  'Cast': cast,
}

# Some aggregate types that are useful for defining functions
types = {
  'BOOLEAN': ['BOOLEAN'],
  'TINYINT': ['TINYINT'],
  'SMALLINT': ['SMALLINT'],
  'INT': ['INT'],
  'BIGINT': ['BIGINT'],
  'LARGEINT': ['LARGEINT'],
  'FLOAT': ['FLOAT'],
  'DOUBLE': ['DOUBLE'],
  'STRING': ['VARCHAR'],
  'DATE': ['DATE'],
  'DATETIME': ['DATETIME'],
  'DECIMALV2': ['DECIMALV2'],
  'NATIVE_INT_TYPES': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT'],
  'INT_TYPES': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'LARGEINT'],
  'FLOAT_TYPES': ['FLOAT', 'DOUBLE'],
  'NUMERIC_TYPES': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', \
          'LARGEINT', 'DECIMALV2'],
  'STRING_TYPES': ['VARCHAR'],
  'DATETIME_TYPES': ['DATE', 'DATETIME'],
  'FIXED_TYPES': ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'LARGEINT'],
  'NATIVE_TYPES': ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'STRCAST_FIXED_TYPES': ['BOOLEAN', 'SMALLINT', 'INT', 'BIGINT'],
  'ALL_TYPES': ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'LARGEINT', 'FLOAT',\
                     'DOUBLE', 'VARCHAR', 'DATETIME', 'DECIMALV2'],
  'MAX_TYPES': ['BIGINT', 'LARGEINT', 'DOUBLE', 'DECIMALV2'],
}

# Operation, [ReturnType], [[Args1], [Args2], ... [ArgsN]]
functions = [
  # Arithmetic Expr
  ['Add', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']]],
  ['Subtract', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']]],
  ['Multiply', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']]],
  ['Divide', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']]],
  ['Int_Divide', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']]],
  ['Mod', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']]],
  ['Mod', ['DECIMALV2'], [['DECIMALV2'], ['DECIMALV2']]],
  ['Mod', ['DOUBLE'], [['DOUBLE'], ['DOUBLE']], double_mod],
  ['BitAnd', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']]],
  ['BitXor', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']]],
  ['BitOr', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']]],
  ['BitNot', ['INT_TYPES'], [['INT_TYPES']]],

  # BinaryPredicates
  ['Eq', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']]],
  ['Ne', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']]],
  ['Gt', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']]],
  ['Lt', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']]],
  ['Ge', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']]],
  ['Le', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']]],
  ['Eq', ['BOOLEAN'], [['LARGEINT'], ['LARGEINT']],],
  ['Ne', ['BOOLEAN'], [['LARGEINT'], ['LARGEINT']],],
  ['Gt', ['BOOLEAN'], [['LARGEINT'], ['LARGEINT']],],
  ['Lt', ['BOOLEAN'], [['LARGEINT'], ['LARGEINT']],],
  ['Ge', ['BOOLEAN'], [['LARGEINT'], ['LARGEINT']],],
  ['Le', ['BOOLEAN'], [['LARGEINT'], ['LARGEINT']],],
  ['Eq', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func],
  ['Ne', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func],
  ['Gt', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func],
  ['Lt', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func],
  ['Ge', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func],
  ['Le', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func],
  ['Eq', ['BOOLEAN'], [['DATETIME'], ['DATETIME']],],
  ['Ne', ['BOOLEAN'], [['DATETIME'], ['DATETIME']],],
  ['Gt', ['BOOLEAN'], [['DATETIME'], ['DATETIME']],],
  ['Lt', ['BOOLEAN'], [['DATETIME'], ['DATETIME']],],
  ['Ge', ['BOOLEAN'], [['DATETIME'], ['DATETIME']],],
  ['Le', ['BOOLEAN'], [['DATETIME'], ['DATETIME']],],
  ['Eq', ['BOOLEAN'], [['DECIMALV2'], ['DECIMALV2']],],
  ['Ne', ['BOOLEAN'], [['DECIMALV2'], ['DECIMALV2']],],
  ['Gt', ['BOOLEAN'], [['DECIMALV2'], ['DECIMALV2']],],
  ['Lt', ['BOOLEAN'], [['DECIMALV2'], ['DECIMALV2']],],
  ['Ge', ['BOOLEAN'], [['DECIMALV2'], ['DECIMALV2']],],
  ['Le', ['BOOLEAN'], [['DECIMALV2'], ['DECIMALV2']],],

  # Casts
  ['Cast', ['BOOLEAN'], [['NATIVE_TYPES'], ['BOOLEAN']]],
  ['Cast', ['TINYINT'], [['NATIVE_TYPES'], ['TINYINT']]],
  ['Cast', ['SMALLINT'], [['NATIVE_TYPES'], ['SMALLINT']]],
  ['Cast', ['INT'], [['NATIVE_TYPES'], ['INT']]],
  ['Cast', ['BIGINT'], [['NATIVE_TYPES'], ['BIGINT']]],
  ['Cast', ['LARGEINT'], [['NATIVE_TYPES'], ['LARGEINT']]],
  ['Cast', ['LARGEINT'], [['DECIMALV2'], ['LARGEINT']]],
  ['Cast', ['NATIVE_TYPES'], [['LARGEINT'], ['NATIVE_TYPES']]],
  ['Cast', ['FLOAT'], [['NATIVE_TYPES'], ['FLOAT']]],
  ['Cast', ['DOUBLE'], [['NATIVE_TYPES'], ['DOUBLE']]],
  ['Cast', ['DECIMALV2'], [['FIXED_TYPES'], ['DECIMALV2']]],
  ['Cast', ['DECIMALV2'], [['FLOAT'], ['DECIMALV2']], float_to_decimal],
  ['Cast', ['DECIMALV2'], [['DOUBLE'], ['DECIMALV2']], double_to_decimal],
  ['Cast', ['NATIVE_TYPES'], [['DECIMALV2'], ['NATIVE_TYPES']]],
  ['Cast', ['NATIVE_INT_TYPES'], [['STRING'], ['NATIVE_INT_TYPES']], string_to_int],
  ['Cast', ['LARGEINT'], [['STRING'], ['LARGEINT']], string_to_int],
  ['Cast', ['FLOAT_TYPES'], [['STRING'], ['FLOAT_TYPES']], string_to_float],
  ['Cast', ['STRING'], [['STRCAST_FIXED_TYPES'], ['STRING']], numeric_to_string],
  ['Cast', ['STRING'], [['LARGEINT'], ['STRING']], largeint_to_string],
  ['Cast', ['STRING'], [['FLOAT'], ['STRING']], float_to_string],
  ['Cast', ['STRING'], [['DOUBLE'], ['STRING']], double_to_string],
  ['Cast', ['STRING'], [['TINYINT'], ['STRING']], tinyint_to_string],
  ['Cast', ['STRING'], [['DECIMALV2'], ['STRING']], decimal_to_string],
  # Datetime cast
  ['Cast', ['DATE'], [['NUMERIC_TYPES'], ['DATE']], numeric_to_date],
  ['Cast', ['DATETIME'], [['NUMERIC_TYPES'], ['DATETIME']], numeric_to_datetime],
  ['Cast', ['DATE'], [['STRING_TYPES'], ['DATE']], string_to_date],
  ['Cast', ['DATETIME'], [['STRING_TYPES'], ['DATETIME']], string_to_datetime],
  ['Cast', ['DATE'], [['DATETIME'], ['DATE']], datetime_to_date],
  ['Cast', ['DATETIME'], [['DATE'], ['DATETIME']], date_to_datetime],
  ['Cast', ['NUMERIC_TYPES'], [['DATETIME'], ['NUMERIC_TYPES']], datetime_to_numeric],
  ['Cast', ['NUMERIC_TYPES'], [['DATE'], ['NUMERIC_TYPES']], datetime_to_numeric],
  ['Cast', ['STRING_TYPES'], [['DATE'], ['STRING_TYPES']], datetime_to_string],
  ['Cast', ['STRING_TYPES'], [['DATETIME'], ['STRING_TYPES']], datetime_to_string],

  # Case
  # The case expr is special because it has a variable number of function args,
  # but we guarantee that all of them are of the same type during query analysis,
  # so we just list exactly one here.
  # In addition, the return type given here is a dummy, because it is
  # not necessarily the same as the function args type.
  ['Case', ['ALL_TYPES'], [['ALL_TYPES']], case],
]

native_types = {
  'BOOLEAN': 'bool',
  'TINYINT': 'char',
  'SMALLINT': 'short',
  'INT': 'int',
  'BIGINT': 'long',
  'LARGEINT': '__int128',
  'FLOAT': 'float',
  'DOUBLE': 'double',
  'VARCHAR': 'StringValue',
  'DATE': 'Date',
  'DATETIME': 'DateTime',
  'TIME': 'double',
  'DECIMALV2': 'DecimalV2Value',
}

# Portable type used in the function implementation
implemented_types = {
  'BOOLEAN': 'bool',
  'TINYINT': 'int8_t',
  'SMALLINT': 'int16_t',
  'INT': 'int32_t',
  'BIGINT': 'int64_t',
  'LARGEINT': '__int128',
  'FLOAT': 'float',
  'DOUBLE': 'double',
  'VARCHAR': 'StringValue',
  'DATE': 'DateTimeValue',
  'DATETIME': 'DateTimeValue',
  'TIME': 'double',
  'DECIMALV2': 'DecimalV2Value',
}
result_fields = {
  'BOOLEAN': 'bool_val',
  'TINYINT': 'tinyint_val',
  'SMALLINT': 'smallint_val',
  'INT': 'int_val',
  'BIGINT': 'bigint_val',
  'LARGEINT': 'large_int_val',
  'FLOAT': 'float_val',
  'DOUBLE': 'double_val',
  'VARCHAR': 'string_val',
  'DATE': 'datetime_val',
  'DATETIME': 'datetime_val',
  'TIME': 'double_val',
  'DECIMALV2': 'decimalv2_val',
}

native_ops = {
  'BITAND': '&',
  'BITNOT': '~',
  'BITOR': '|',
  'BITXOR': '^',
  'DIVIDE': '/',
  'EQ': '==',
  'GT': '>',
  'GE': '>=',
  'INT_DIVIDE': '/',
  'SUBTRACT': '-',
  'MOD': '%',
  'MULTIPLY': '*',
  'LT': '<',
  'LE': '<=',
  'NE': '!=',
  'ADD': '+',
}

native_funcs = {
  'EQ': 'eq',
  'LE': 'le',
  'LT': 'lt',
  'NE': 'ne',
  'GE': 'ge',
  'GT': 'gt',
}

cc_preamble = '\
\n\
// This is a generated file, DO NOT EDIT.\n\
// To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
#include "gen_cpp/opcode/functions.h"\n\
#include "exprs/expr.h"\n\
#include "exprs/case_expr.h"\n\
#include "runtime/string_value.hpp"\n\
#include "runtime/tuple_row.h"\n\
#include "util/mysql_dtoa.h"\n\
#include "util/string_parser.hpp"\n\
#include <boost/lexical_cast.hpp>\n\
\n\
using namespace boost;\n\
using namespace std;\n\
\n\
namespace doris { \n\
\n'

cc_epilogue = '\
}\n'

h_preamble = '\
\n\
#ifndef DORIS_OPCODE_FUNCTIONS_H\n\
#define DORIS_OPCODE_FUNCTIONS_H\n\
\n\
namespace doris {\n\
class Expr;\n\
class OpcodeRegistry;\n\
class TupleRow;\n\
\n\
class ComputeFunctions {\n\
 public:\n'

h_epilogue = '\
};\n\
\n\
}\n\
\n\
#endif\n'

python_preamble = '\
#!/usr/bin/env python\n\
\n\
# This is a generated file, DO NOT EDIT IT.\n\
# To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
functions = [\n'

python_epilogue = ']'

header_template = string.Template("\
  static void* ${fn_signature}(Expr* e, TupleRow* row);\n")

BE_PATH = "../gen_cpp/opcode/"

def initialize_sub(op, return_type, arg_types):
    """
    Expand the signature data for template substitution.  Returns
    a dictionary with all the entries for all the templates used in this script
    """
    sub = {}
    sub["fn_name"] = op
    sub["fn_signature"] = op
    sub["return_type"] = return_type
    sub["result_field"] = result_fields[return_type]
    sub["args"] = ""
    if op.upper() in native_ops:
        sub["native_op"] = native_ops[op.upper()]
    for idx in range(0, len(arg_types)):
        arg = arg_types[idx]
        sub["fn_signature"] += "_" + native_types[arg]
        sub["native_type" + repr(idx + 1)] = implemented_types[arg]
        sub["args"] += "'" + arg + "', "
    return sub

if __name__ == "__main__":

    try:
        os.makedirs(BE_PATH)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    h_file = open(BE_PATH + 'functions.h', 'w')
    cc_file = open(BE_PATH + 'functions.cc', 'w')
    python_file = open('generated_functions.py', 'w')
    h_file.write(h_preamble)
    cc_file.write(cc_preamble)
    python_file.write(python_preamble)

    # Generate functions and headers
    for func_data in functions:
        op = func_data[0]
        # If a specific template has been specified, use that one.
        if len(func_data) >= 4:
            template = func_data[3]
        else:
            # Skip functions with no template (shouldn't be auto-generated)
            if not op in templates:
                continue
            template = templates[op]

        # Expand all arguments
        return_types = []
        for ret in func_data[1]:
            for t in types[ret]:
                return_types.append(t)
        signatures = []
        for args in func_data[2]:
            expanded_arg = []
            for arg in args:
                for t in types[arg]:
                    expanded_arg.append(t)
            signatures.append(expanded_arg)

        # Put arguments into substitution structure
        num_functions = 0
        for args in signatures:
            num_functions = max(num_functions, len(args))
        num_functions = max(num_functions, len(return_types))
        num_args = len(signatures)

        # Validate the input is correct
        if len(return_types) != 1 and len(return_types) != num_functions:
            print("Invalid Declaration: " + func_data)
            sys.exit(1)

        for args in signatures:
            if len(args) != 1 and len(args) != num_functions:
                print("Invalid Declaration: " + func_data)
                sys.exit(1)

        # Iterate over every function signature to generate
        for i in range(0, num_functions):
            if len(return_types) == 1:
                return_type = return_types[0]
            else:
                return_type = return_types[i]

            arg_types = []
            for j in range(0, num_args):
                if len(signatures[j]) == 1:
                    arg_types.append(signatures[j][0])
                else:
                    arg_types.append(signatures[j][i])

            # At this point, 'return_type' is a single type and 'arg_types'
            # is a list of single types
            sub = initialize_sub(op, return_type, arg_types)
            if template == binary_func:
                sub["native_func"] = native_funcs[op.upper()]

            h_file.write(header_template.substitute(sub))
            cc_file.write(template.substitute(sub))
            python_file.write(python_template.substitute(sub))

    h_file.write(h_epilogue)
    cc_file.write(cc_epilogue)
    python_file.write(python_epilogue)
    h_file.close()
    cc_file.close()
    python_file.close()
