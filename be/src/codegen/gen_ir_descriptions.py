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
This script will generate two headers that describe all of the clang cross compiled
functions.
The script outputs (run: 'doris/common/function-registry/gen_functions.py')
  - be/src/generated-sources/doris-ir/doris-ir-functions.h
    This file contains enums for all of the cross compiled functions
  - be/src/generated-sources/doris-ir/doris-ir-function-names.h
    This file contains a mapping of <string, enum>

Mapping of enum to compiled function name.  The compiled function name only has to
be a substring of the actual, mangled compiler generated name.
TODO: should we work out the mangling rules?
"""

import string
import os

ir_functions = [

    ["AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING", "process_row_batch_with_grouping"],
    ["AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING", "process_row_batch_no_grouping"],
#    ["EXPR_GET_VALUE", "IrExprGetValue"],
#    ["HASH_CRC", "IrCrcHash"],
#    ["HASH_FVN", "IrFvnHash"],
    ["HASH_JOIN_PROCESS_BUILD_BATCH", "12HashJoinNode19process_build_batch"],
    ["HASH_JOIN_PROCESS_PROBE_BATCH", "12HashJoinNode19process_probe_batch"],
    ["EXPR_GET_BOOLEAN_VAL", "4Expr15get_boolean_val"],
    ["EXPR_GET_TINYINT_VAL", "4Expr16get_tiny_int_val"],
    ["EXPR_GET_SMALLINT_VAL", "4Expr17get_small_int_val"],
    ["EXPR_GET_INT_VAL", "4Expr11get_int_val"],
    ["EXPR_GET_BIGINT_VAL", "4Expr15get_big_int_val"],
    ["EXPR_GET_LARGEINT_VAL", "4Expr17get_large_int_val"],
    ["EXPR_GET_FLOAT_VAL", "4Expr13get_float_val"],
    ["EXPR_GET_DOUBLE_VAL", "4Expr14get_double_val"],
    ["EXPR_GET_STRING_VAL", "4Expr14get_string_val"],
    ["EXPR_GET_DATETIME_VAL", "4Expr16get_datetime_val"],
    ["EXPR_GET_DECIMAL_VAL", "4Expr15get_decimal_val"],
    ["HASH_CRC", "ir_crc_hash"],
    ["HASH_FNV", "ir_fnv_hash"],
    ["FROM_DECIMAL_VAL", "16from_decimal_val"],
    ["TO_DECIMAL_VAL", "14to_decimal_val"],
    ["FROM_DATETIME_VAL", "17from_datetime_val"],
    ["TO_DATETIME_VAL", "15to_datetime_val"],
    ["IR_STRING_COMPARE", "ir_string_compare"],
#    ["STRING_VALUE_EQ", "StringValueEQ"],
#    ["STRING_VALUE_NE", "StringValueNE"],
#    ["STRING_VALUE_GE", "StringValueGE"],
#    ["STRING_VALUE_GT", "StringValueGT"],
#    ["STRING_VALUE_LT", "StringValueLT"],
#    ["STRING_VALUE_LE", "StringValueLE"],
#    ["STRING_TO_BOOL", "IrStringToBool"],
#    ["STRING_TO_INT8", "IrStringToInt8"],
#    ["STRING_TO_INT16", "IrStringToInt16"],
#    ["STRING_TO_INT32", "IrStringToInt32"],
#    ["STRING_TO_INT64", "IrStringToInt64"],
#    ["STRING_TO_FLOAT", "IrStringToFloat"],
#    ["STRING_TO_DOUBLE", "IrStringToDouble"],
#    ["STRING_IS_NULL", "IrIsNullString"],
    ["HLL_UPDATE_BOOLEAN", "hll_updateIN8doris_udf10BooleanVal"],
    ["HLL_UPDATE_TINYINT", "hll_updateIN8doris_udf10TinyIntVal"],
    ["HLL_UPDATE_SMALLINT", "hll_updateIN8doris_udf11SmallIntVal"],
    ["HLL_UPDATE_INT", "hll_updateIN8doris_udf6IntVal"],
    ["HLL_UPDATE_BIGINT", "hll_updateIN8doris_udf9BigIntVal"],
    ["HLL_UPDATE_FLOAT", "hll_updateIN8doris_udf8FloatVal"],
    ["HLL_UPDATE_DOUBLE", "hll_updateIN8doris_udf9DoubleVal"],
    ["HLL_UPDATE_STRING", "hll_updateIN8doris_udf9StringVal"],
    ["HLL_UPDATE_TIMESTAMP", "hll_updateIN8doris_udf11DateTimeVal"],
    ["HLL_UPDATE_DECIMAL", "hll_updateIN8doris_udf10DecimalVal"],
    ["HLL_MERGE", "hll_merge"],
    ["CODEGEN_ANYVAL_DATETIME_VAL_EQ", "datetime_val_eq"],
    ["CODEGEN_ANYVAL_STRING_VAL_EQ", "string_val_eq"],
    ["CODEGEN_ANYVAL_DECIMAL_VAL_EQ", "decimal_val_eq"],
    ["CODEGEN_ANYVAL_DATETIME_VALUE_EQ", "datetime_value_eq"],
    ["CODEGEN_ANYVAL_STRING_VALUE_EQ", "string_value_eq"],
    ["CODEGEN_ANYVAL_DECIMAL_VALUE_EQ", "decimal_value_eq"],
    ["RAW_VALUE_COMPARE", "8RawValue7compare"],
]

enums_preamble = '\
// Licensed to the Apache Software Foundation (ASF) under one \n\
// or more contributor license agreements.  See the NOTICE file \n\
// distributed with this work for additional information \n\
// regarding copyright ownership.  The ASF licenses this file \n\
// to you under the Apache License, Version 2.0 (the \n\
// "License"); you may not use this file except in compliance \n\
// with the License.  You may obtain a copy of the License at \n\
// \n\
//   http://www.apache.org/licenses/LICENSE-2.0 \n\
// \n\
// Unless required by applicable law or agreed to in writing, \n\
// software distributed under the License is distributed on an \n\
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY \n\
// KIND, either express or implied.  See the License for the \n\
// specific language governing permissions and limitations \n\
// under the License. \n\
\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see be/src/codegen/gen_ir_descriptions.py.\n\
\n\
#ifndef DORIS_IR_FUNCTIONS_H\n\
#define DORIS_IR_FUNCTIONS_H\n\
\n\
namespace doris {\n\
\n\
class IRFunction {\n\
 public:\n\
  enum Type {\n'

enums_epilogue = '\
  };\n\
};\n\
\n\
}\n\
\n\
#endif\n'

names_preamble = '\
// Licensed to the Apache Software Foundation (ASF) under one \n\
// or more contributor license agreements.  See the NOTICE file \n\
// distributed with this work for additional information \n\
// regarding copyright ownership.  The ASF licenses this file \n\
// to you under the Apache License, Version 2.0 (the \n\
// "License"); you may not use this file except in compliance \n\
// with the License.  You may obtain a copy of the License at \n\
// \n\
//   http://www.apache.org/licenses/LICENSE-2.0 \n\
// \n\
// Unless required by applicable law or agreed to in writing, \n\
// software distributed under the License is distributed on an \n\
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY \n\
// KIND, either express or implied.  See the License for the \n\
// specific language governing permissions and limitations \n\
// under the License. \n\
\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see be/src/codegen/gen_ir_descriptions.py.\n\
\n\
#ifndef DORIS_IR_FUNCTION_NAMES_H\n\
#define DORIS_IR_FUNCTION_NAMES_H\n\
\n\
#include "doris_ir/doris_ir_functions.h"\n\
\n\
namespace doris {\n\
\n\
static struct {\n\
  std::string fn_name; \n\
  IRFunction::Type fn; \n\
} FN_MAPPINGS[] = {\n'

names_epilogue = '\
};\n\
\n\
}\n\
\n\
#endif\n'

BE_PATH = os.environ['DORIS_HOME'] + "/gensrc/build/doris_ir/"
if not os.path.exists(BE_PATH):
    os.makedirs(BE_PATH)

if __name__ == "__main__":
    print("Generating IR description files")
    enums_file = open(BE_PATH + 'doris_ir_functions.h', 'w')
    enums_file.write(enums_preamble)

    names_file = open(BE_PATH + 'doris_ir_names.h', 'w')
    names_file.write(names_preamble)

    idx = 0
    enums_file.write("    FN_START = " + str(idx) + ",\n")
    for fn in ir_functions:
        enum = fn[0]
        fn_name = fn[1]
        enums_file.write("    " + enum + " = " + str(idx) + ",\n")
        names_file.write("  { \"" + fn_name + "\", IRFunction::" + enum + " },\n")
        idx = idx + 1
    enums_file.write("    FN_END = " + str(idx) + "\n")

    enums_file.write(enums_epilogue)
    enums_file.close()

    names_file.write(names_epilogue)
    names_file.close()
