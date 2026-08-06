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

suite("test_pythonudf_object_types_inline") {
    def runtime_version = getPythonUdfRuntimeVersion()

    test {
        sql """
        CREATE FUNCTION py_obj_udf_bitmap_arg(bitmap)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v):
    return 1
\$\$;
        """
        exception "does not support argument 1 type bitmap"
    }

    test {
        sql """
        CREATE FUNCTION py_obj_udf_hll_ret(int)
        RETURNS HLL
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v):
    return None
\$\$;
        """
        exception "does not support return type hll"
    }

    test {
        sql """
        CREATE FUNCTION py_obj_udf_array_bitmap(array<int>)
        RETURNS ARRAY<BITMAP>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v):
    return None
\$\$;
        """
        exception "ARRAY unsupported sub-type: bitmap"
    }

    test {
        sql """
        CREATE FUNCTION py_obj_udf_map_bitmap(map<int, bitmap>)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v):
    return 1
\$\$;
        """
        exception "MAP unsupported sub-type: bitmap"
    }

    test {
        sql """
        CREATE FUNCTION py_obj_udf_struct_bitmap(INT)
        RETURNS STRUCT<plain:INT, nested:ARRAY<STRUCT<b:BITMAP>>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v):
    return None
\$\$;
        """
        exception "STRUCT unsupported sub-type: bitmap"
    }
}
