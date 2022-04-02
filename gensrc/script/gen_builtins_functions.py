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
This module is doris builtin functions
"""

import sys
import os
import errno
from string import Template
import doris_builtins_functions

java_registry_preamble = '\
// Licensed to the Apache Software Foundation (ASF) under one \n\
// or more contributor license agreements.  See the NOTICE file \n\
// distributed with this work for additional information \n\
// regarding copyright ownership.  The ASF licenses this file \n\
// to you under the Apache License, Version 2.0 (the \n\
// "License"); you may not use this file except in compliance \n\
// with the License.  You may obtain a copy of the License at \n\
// \n\
//  http://www.apache.org/licenses/LICENSE-2.0\n\
// \n\
//  Unless required by applicable law or agreed to in writing, software\n\
//  distributed under the License is distributed on an "AS IS" BASIS,\n\
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
//  See the License for the specific language governing permissions and\n\
//  limitations under the License.\n\
\n\
// This is a generated file, DO NOT EDIT.\n\
// To add new functions, see the generator at\n\
// common/function-registry/gen_builtins_catalog.py or the function list at\n\
// common/function-registry/doris_builtins_functions.py.\n\
\n\
package org.apache.doris.builtins;\n\
\n\
import org.apache.doris.catalog.ArrayType;\n\
import org.apache.doris.catalog.Type;\n\
import org.apache.doris.catalog.Function;\n\
import org.apache.doris.catalog.FunctionSet;\n\
import com.google.common.collect.Sets;\n\
import java.util.Set;\n\
\n\
public class ScalarBuiltins { \n\
    public static void initBuiltins(FunctionSet functionSet) { \
\n'

java_registry_epilogue = '\
  }\n\
}\n'

FE_PATH = "../../../fe/fe-core/target/generated-sources/build/org/apache/doris/builtins/"
print(FE_PATH)

# This contains all the metadata to describe all the builtins.
# Each meta data entry is itself a map to store all the meta data
#   - fn_name, ret_type, args, symbol, sql_names
meta_data_entries = []

# Read in the function and add it to the meta_data_entries map
def add_function(fn_meta_data, user_visible):
    """add function
    """
    assert len(fn_meta_data) == 8, \
            "Invalid function entry in doris_builtins_functions.py:\n\t" + repr(fn_meta_data)
    entry = {}
    entry["sql_names"] = fn_meta_data[0]
    entry["ret_type"] = fn_meta_data[1]
    entry["args"] = fn_meta_data[2]
    entry["symbol"] = fn_meta_data[3]
    if fn_meta_data[4] != '':
        entry["prepare"] = fn_meta_data[4]
    if fn_meta_data[5] != '':
        entry["close"] = fn_meta_data[5]
    if fn_meta_data[6] != '':
        entry['vec'] = True
    if fn_meta_data[7] != '':
        entry['nullable_mode'] = fn_meta_data[7]
    else:
        entry['nullable_mode'] = 'DEPEND_ON_ARGUMENT'

    entry["user_visible"] = user_visible
    meta_data_entries.append(entry)


"""
generate fe data type, support nested ARRAY type.
for example:
    in[TINYINT]     --> out[Type.TINYINT]
    in[INT]         --> out[Type.INT]
    in[ARRAY_INT]   --> out[new ArrayType(Type.INT)]
"""
def generate_fe_datatype(str_type):
    if str_type.startswith("ARRAY_"):
        vec_type = str_type.split('_', 1);
        if len(vec_type) > 1 and vec_type[0] == "ARRAY":
            return "new ArrayType(" + generate_fe_datatype(vec_type[1]) + ")"
    return "Type." + str_type

"""
Order of params:
name, symbol, user_visible, prepare, close, nullable_mode, ret_type, has_var_args, args
"""
def generate_fe_entry(entry, name):
    """add function
    """
    java_output = ""
    java_output += "\"" + name + "\""
    java_output += ", \"" + entry["symbol"] + "\""
    if entry["user_visible"]:
        java_output += ", true"
    else:
        java_output += ", false"
    if 'prepare' in entry:
        java_output += ', "%s"' % entry["prepare"]
    else:
        java_output += ', null'
    if 'close' in entry:
        java_output += ', "%s"' % entry["close"]
    else:
        java_output += ', null'

    java_output += ", Function.NullableMode." + entry["nullable_mode"]
    java_output += ", " + generate_fe_datatype(entry["ret_type"])

    # Check the last entry for varargs indicator.
    if entry["args"] and entry["args"][-1] == "...":
        entry["args"].pop()
        java_output += ", true"
    else:
        java_output += ", false"
    for arg in entry["args"]:
        java_output += ", " + generate_fe_datatype(arg)
    return java_output

# Generates the FE builtins init file that registers all the builtins.
def generate_fe_registry_init(filename):
    """add function
    """
    java_registry_file = open(filename, "w")
    java_registry_file.write(java_registry_preamble)

    for entry in meta_data_entries:
        for name in entry["sql_names"]:
            java_output = generate_fe_entry(entry, name)
            if ("vec" not in entry):
                java_registry_file.write("        functionSet.addScalarBuiltin(%s);\n" % java_output)
            else:
                java_registry_file.write("        functionSet.addScalarAndVectorizedBuiltin(%s);\n" % java_output)


    java_registry_file.write("\n")

    # add non_null_result_with_null_param_functions
    java_registry_file.write("        Set<String> funcNames = Sets.newHashSet();\n")
    for entry in doris_builtins_functions.null_result_with_one_null_param_functions:
        java_registry_file.write("        funcNames.add(\"%s\");\n" % entry)
    java_registry_file.write("        functionSet.buildNullResultWithOneNullParamFunction(funcNames);\n");

    # add nondeterministic functions
    java_registry_file.write("        Set<String> nondeterministicFuncNames = Sets.newHashSet();\n")
    for entry in doris_builtins_functions.nondeterministic_functions:
        java_registry_file.write("        nondeterministicFuncNames.add(\"%s\");\n" % entry)
    java_registry_file.write("        functionSet.buildNondeterministicFunctions(nondeterministicFuncNames);\n");

    java_registry_file.write("        funcNames = Sets.newHashSet();\n")
    for entry in doris_builtins_functions.null_result_with_one_null_param_functions:
        java_registry_file.write("        funcNames.add(\"%s\");\n" % entry)
    java_registry_file.write("        functionSet.buildNullResultWithOneNullParamFunction(funcNames);\n");

    java_registry_file.write(java_registry_epilogue)
    java_registry_file.close()

if __name__ == "__main__":

    try:
        os.makedirs(FE_PATH)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    # Read the function metadata inputs
    for function in doris_builtins_functions.visible_functions:
        add_function(function, True)
    for function in doris_builtins_functions.invisible_functions:
        add_function(function, False)

    generate_fe_registry_init(FE_PATH + "ScalarBuiltins.java")
