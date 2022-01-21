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
# This script will generate the implementation of the simple vector functions for the BE.
# These include:
#   - Arithmetic functions
#   - Binary functions
#   - Cast functions
#
# The script outputs (run: 'src/common/function/gen_vector_functions.py')
#   - header and implemention for above functions:
#     - src/gen_cpp/opcode/vector_functions.[h/cc]
#   - python file that contains the metadata for those functions:
#     - src/gen_cpp/generated_vector_functions.py
"""

import string
import os
import errno

filter_binary_op = string.Template("\
bool VectorComputeFunctions::${fn_signature}(\n\
        Expr* expr, VectorizedRowBatch* batch) {\n\
    int n = batch->size();\n\
    if (0 == n) {\n\
        return false;\n\
    }\n\
    int* sel = batch->selected();\n\
    Expr* op1 = expr->children()[0];\n\
    Expr* op2 = expr->children()[1];\n\
    batch->add_column(expr->output_column(), expr->type());\n\
    if (expr->is_constant()) {\n\
        ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->get_value(NULL));\n\
        ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->get_value(NULL));\n\
        if (val1 == NULL || val2 == NULL) return false;\n\
        if (!(*val1 ${native_op} *val2)) batch->set_size(0);\n\
    } else if (op1->is_constant()) {\n\
        ${native_type1}* value = reinterpret_cast<${native_type1}*>(op1->get_value(NULL));\n\
        if (NULL == value || !op2->evaluate(batch)) return false;\n\
        ${native_type1}* vector1\n\
            = reinterpret_cast<${native_type1}*>(batch->column(op2->output_column())->col_data());\n\
    \n\
        int new_size = 0;\n\
        if (batch->selected_in_use()) {\n\
            for (int j = 0; j != n; ++j) {\n\
                int i = sel[j];\n\
                if (*value ${native_op} vector1[i]) {\n\
                    sel[new_size++] = i;\n\
                }\n\
            }\n\
            batch->set_size(new_size);\n\
        } else {\n\
            for (int i = 0; i != n; ++i) {\n\
                if (*value ${native_op} vector1[i]) {\n\
                    sel[new_size++] = i;\n\
                }\n\
            }\n\
    \n\
            if (new_size < n) {\n\
                batch->set_size(new_size);\n\
                batch->set_selected_in_use(true);\n\
            }\n\
        }\n\
    } else if (op2->is_constant()) {\n\
        ${native_type2}* value = reinterpret_cast<${native_type2}*>(op2->get_value(NULL));\n\
        if (NULL == value || !op1->evaluate(batch)) return false;\n\
        ${native_type1}* vector1\n\
            = reinterpret_cast<${native_type1}*>(batch->column(op1->output_column())->col_data());\n\
    \n\
        int new_size = 0;\n\
        if (batch->selected_in_use()) {\n\
            for (int j = 0; j != n; ++j) {\n\
                int i = sel[j];\n\
                if (vector1[i] ${native_op} *value) {\n\
                    sel[new_size++] = i;\n\
                }\n\
            }\n\
            batch->set_size(new_size);\n\
        } else {\n\
            for (int i = 0; i != n; ++i) {\n\
                if (vector1[i] ${native_op} *value) {\n\
                    sel[new_size++] = i;\n\
                }\n\
            }\n\
    \n\
            if (new_size < n) {\n\
                batch->set_size(new_size);\n\
                batch->set_selected_in_use(true);\n\
            }\n\
        }\n\
    } else {\n\
        if (!op1->evaluate(batch) || !op2->evaluate(batch)) return false;\n\
        ${native_type1}* vector1\n\
            = reinterpret_cast<${native_type1}*>(batch->column(op1->output_column())->col_data());\n\
        ${native_type2}* vector2\n\
            = reinterpret_cast<${native_type2}*>(batch->column(op2->output_column())->col_data());\n\
    \n\
        int new_size = 0;\n\
        if (batch->selected_in_use()) {\n\
            for (int j = 0; j != n; ++j) {\n\
                int i = sel[j];\n\
                if (vector1[i] ${native_op} vector2[i]) {\n\
                    sel[new_size++] = i;\n\
                }\n\
            }\n\
            batch->set_size(new_size);\n\
        } else {\n\
            for (int i = 0; i != n; ++i) {\n\
                if (vector1[i] ${native_op} vector2[i]) {\n\
                    sel[new_size++] = i;\n\
                }\n\
            }\n\
            if (new_size < n) {\n\
                batch->set_size(new_size);\n\
                batch->set_selected_in_use(true);\n\
            }\n\
        }\n\
    }\n\
    return true;\n\
}\n\n")

filter_in_op = string.Template("\
bool VectorComputeFunctions::${fn_signature}(\n\
        Expr* expr, VectorizedRowBatch* batch) {\n\
    int n = batch->size();\n\
    if (0 == n) {\n\
        return true;\n\
    }\n\
    batch->add_column(expr->output_column(), expr->type());\n\
    int* sel = batch->selected();\n\
    int num_children = expr->get_num_children();\n\
    Expr* op1 = expr->children()[0];\n\
    InPredicate *in_pred = static_cast<InPredicate*>(expr);\n\
\n\
    if (op1->is_constant()) {\n\
        void* value = op1->get_value(NULL);\n\
        if (!in_pred->hybird_set()->find(value)) {\n\
            batch->set_size(0);\n\
            return true;\n\
        }\n\
\n\
        if (num_children > 1) {\n\
            ${native_type1}* v = reinterpret_cast<${native_type1}*>(value);\n\
            ${native_type1}* vectors[num_children];\n\
            for (int i = 1; i < num_children; ++i) {\n\
                if (expr->get_child(i)->evaluate(batch)) return false;\n\
                vectors[i] = reinterpret_cast<${native_type1}*>(batch->column(expr->get_child(i)->output_column())->col_data());\n\
            }\n\
        \n\
            int new_size = 0;\n\
            if (batch->selected_in_use()) {\n\
                for (int j = 0; j != n; ++j) {\n\
                    int i = sel[j];\n\
                    for (int k = 1; k < num_children; ++k) {\n\
                        if (*v == vectors[k][i]) {\n\
                            sel[new_size++] = i;\n\
                            break;\n\
                        }\n\
                    }\n\
                }\n\
                batch->set_size(new_size);\n\
            } else {\n\
                for (int i = 0; i != n; ++i) {\n\
                    for (int k = 1; k < num_children; ++k) {\n\
                        if (*v == vectors[k][i]) {\n\
                            sel[new_size++] = i;\n\
                            break;\n\
                        }\n\
                    }\n\
                }\n\
        \n\
                if (new_size < n) {\n\
                    batch->set_size(new_size);\n\
                    batch->set_selected_in_use(true);\n\
                }\n\
            }\n\
        }\n\
    } else {\n\
        int c1 = op1->evaluate(batch);\n\
        DCHECK(c1 >= 0);\n\
        ${native_type1}* vector1 \n\
            =reinterpret_cast<${native_type1}*>(batch->column(op1->output_column())->col_data());\n\
        if (0 != in_pred->hybird_set()->size()) {\n\
            int new_size = 0;\n\
            if (batch->selected_in_use()) {\n\
                for (int j = 0; j != n; ++j) {\n\
                    int i = sel[j];\n\
                    if (in_pred->hybird_set()->find(&vector1[i])) {\n\
                        sel[new_size++] = i;\n\
                    }\n\
                }\n\
                batch->set_size(new_size);\n\
            } else {\n\
                for (int i = 0; i != n; ++i) {\n\
                    if (in_pred->hybird_set()->find(&vector1[i])) {\n\
                        sel[new_size++] = i;\n\
                    }\n\
                }\n\
        \n\
                if (new_size < n) {\n\
                    batch->set_size(new_size);\n\
                    batch->set_selected_in_use(true);\n\
                }\n\
            }\n\
        }\n\
\n\
        if (num_children > 1) {\n\
            ${native_type1}* vectors[num_children];\n\
            for (int i = 1; i < num_children; ++i) {\n\
                if (!expr->get_child(i)->evaluate(batch)) return false;\n\
                vectors[i] = reinterpret_cast<${native_type1}*>(batch->column(expr->get_child(i)->output_column())->col_data());\n\
            }\n\
        \n\
            int new_size = 0;\n\
            if (batch->selected_in_use()) {\n\
                for (int j = 0; j != n; ++j) {\n\
                    int i = sel[j];\n\
                    for (int k = 1; k < num_children; ++k) {\n\
                        if (vector1[i] == vectors[k][i]) {\n\
                            sel[new_size++] = i;\n\
                            break;\n\
                        }\n\
                    }\n\
                }\n\
                batch->set_size(new_size);\n\
            } else {\n\
                for (int i = 0; i != n; ++i) {\n\
                    for (int k = 1; k < num_children; ++k) {\n\
                        if (vector1[i] == vectors[k][i]) {\n\
                            sel[new_size++] = i;\n\
                            break;\n\
                        }\n\
                    }\n\
                }\n\
        \n\
                if (new_size < n) {\n\
                    batch->set_size(new_size);\n\
                    batch->set_selected_in_use(true);\n\
                }\n\
            }\n\
        }\n\
    }\n\
    return true;\n\
}\n\n")

python_template = string.Template("\
  ['${fn_name}', '${return_type}', [${args}], 'VectorComputeFunctions::${fn_signature}', []], \n")

# Mapping of function to template
templates = {
  'Filter_Eq': filter_binary_op,
  'Filter_Ne': filter_binary_op,
  'Filter_Gt': filter_binary_op,
  'Filter_Lt': filter_binary_op,
  'Filter_Ge': filter_binary_op,
  'Filter_Le': filter_binary_op,
  'Filter_In': filter_in_op,
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
  'NUMERIC_TYPES': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'NATIVE_TYPES': ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'STRCAST_TYPES': ['BOOLEAN', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'ALL_TYPES': ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'LARGEINT', 'FLOAT',\
                     'DOUBLE', 'VARCHAR', 'DATETIME', 'DECIMALV2'],
  'MAX_TYPES': ['BIGINT', 'LARGEINT', 'DOUBLE', 'DECIMALV2'],
}

# Operation, [ReturnType], [[Args1], [Args2], ... [ArgsN]]
functions = [
  # BinaryPredicates
  ['Filter_Eq', ['BOOLEAN'], [['ALL_TYPES'], ['ALL_TYPES']]],
  ['Filter_Ne', ['BOOLEAN'], [['ALL_TYPES'], ['ALL_TYPES']]],
  ['Filter_Gt', ['BOOLEAN'], [['ALL_TYPES'], ['ALL_TYPES']]],
  ['Filter_Lt', ['BOOLEAN'], [['ALL_TYPES'], ['ALL_TYPES']]],
  ['Filter_Ge', ['BOOLEAN'], [['ALL_TYPES'], ['ALL_TYPES']]],
  ['Filter_Le', ['BOOLEAN'], [['ALL_TYPES'], ['ALL_TYPES']]],

  # InPredicates
  ['Filter_In', ['BOOLEAN'], [['ALL_TYPES']]],
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
  'DATE': 'DateTimeValue',
  'DATETIME': 'DateTimeValue',
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
  'DECIMALV2': 'DecimalV2Value',
}

native_ops = {
  'Filter_Eq': '==',
  'Filter_Ne': '!=',
  'Filter_Gt': '>',
  'Filter_Lt': '<',
  'Filter_Ge': '>=',
  'Filter_Le': '<=',
  'Eq': '==',
  'Ne': '!=',
  'Gt': '>',
  'Lt': '<',
  'Ge': '>=',
  'Le': '<=',
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
  'EQ': 'Eq',
  'LE': 'Le',
  'LT': 'Lt',
  'NE': 'Ne',
  'GE': 'Ge',
  'GT': 'Gt',
}

cc_preamble = '\
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see impala/common/function-registry/gen_vector_functions.py\n\
\n\
#include "gen_cpp/opcode/vector-functions.h"\n\
#include "exprs/case_expr.h"\n\
#include "exprs/expr.h"\n\
#include "exprs/in_predicate.h"\n\
#include "runtime/string_value.hpp"\n\
#include "runtime/vectorized_row_batch.h"\n\
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
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see impala/common/function-registry/gen_vector_functions.py\n\
\n\
#ifndef DORIS_OPCODE_VECTOR_FUNCTIONS_H\n\
#define DORIS_OPCODE_VECTOR_FUNCTIONS_H\n\
\n\
namespace doris {\n\
class Expr;\n\
class OpcodeRegistry;\n\
class VectorizedRowBatch;\n\
\n\
class VectorComputeFunctions {\n\
 public:\n'

h_epilogue = '\
};\n\
\n\
}\n\
\n\
#endif\n'

python_preamble = '\
#!/usr/bin/env python\n\
# Licensed to the Apache Software Foundation (ASF) under one \n\
# or more contributor license agreements.  See the NOTICE file \n\
# distributed with this work for additional information \n\
# regarding copyright ownership.  The ASF licenses this file \n\
# to you under the Apache License, Version 2.0 (the \n\
# "License"); you may not use this file except in compliance \n\
# with the License.  You may obtain a copy of the License at \n\
# \n\
#  http://www.apache.org/licenses/LICENSE-2.0\n\
# \n\
#  Unless required by applicable law or agreed to in writing, software\n\
#  distributed under the License is distributed on an "AS IS" BASIS,\n\
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
#  See the License for the specific language governing permissions and\n\
#  limitations under the License.\n\
\n\
# This is a generated file, DO NOT EDIT IT.\n\
# To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
functions = [\n'

python_epilogue = ']'

header_template = string.Template("\
  static bool ${fn_signature}(\n\
        Expr* e, VectorizedRowBatch* batch);\n")

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
    sub["args"] = ""
    if op in native_ops:
        sub["native_op"] = native_ops[op]
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

    h_file = open(BE_PATH + 'vector-functions.h', 'w')
    cc_file = open(BE_PATH + 'vector-functions.cc', 'w')
    python_file = open('generated_vector_functions.py', 'w')
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

            h_file.write(header_template.substitute(sub))
            cc_file.write(template.substitute(sub))
            python_file.write(python_template.substitute(sub))

    h_file.write(h_epilogue)
    cc_file.write(cc_epilogue)
    python_file.write(python_epilogue)
    h_file.close()
    cc_file.close()
    python_file.close()
