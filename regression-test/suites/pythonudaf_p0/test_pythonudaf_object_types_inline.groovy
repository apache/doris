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

suite("test_pythonudaf_object_types_inline") {
    def runtime_version = getPythonUdfRuntimeVersion()

    test {
        sql """
        CREATE AGGREGATE FUNCTION py_obj_udaf_bitmap_arg(bitmap)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Agg",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class Agg:
    def __init__(self):
        self.sum = 0
    def accumulate(self, v):
        pass
    def merge(self, other):
        pass
    def finish(self):
        return self.sum
    @property
    def aggregate_state(self):
        return self.sum
\$\$;
        """
        exception "does not support argument 1 type bitmap"
    }

    test {
        sql """
        CREATE AGGREGATE FUNCTION py_obj_udaf_hll_ret(int)
        RETURNS HLL
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Agg",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class Agg:
    def __init__(self):
        self.state = None
    def accumulate(self, v):
        pass
    def merge(self, other):
        pass
    def finish(self):
        return self.state
    @property
    def aggregate_state(self):
        return self.state
\$\$;
        """
        exception "does not support return type hll"
    }

    test {
        sql """
        CREATE AGGREGATE FUNCTION py_obj_udaf_quantile_state(quantile_state)
        RETURNS BIGINT
        INTERMEDIATE BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Agg",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class Agg:
    def __init__(self):
        self.state = 0
    def accumulate(self, v):
        pass
    def merge(self, other):
        pass
    def finish(self):
        return self.state
    @property
    def aggregate_state(self):
        return self.state
\$\$;
        """
        exception "does not support argument 1 type quantile_state"
    }

    test {
        sql """
        CREATE AGGREGATE FUNCTION py_obj_udaf_bitmap_intermediate(int)
        RETURNS BIGINT
        INTERMEDIATE BITMAP
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Agg",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class Agg:
    def __init__(self):
        self.state = 0
    def accumulate(self, v):
        pass
    def merge(self, other):
        pass
    def finish(self):
        return self.state
    @property
    def aggregate_state(self):
        return self.state
\$\$;
        """
        exception "does not support intermediate type bitmap"
    }

    test {
        sql """
        CREATE AGGREGATE FUNCTION py_obj_udaf_array_bitmap(int)
        RETURNS ARRAY<BITMAP>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Agg",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class Agg:
    def __init__(self):
        self.state = None
    def accumulate(self, v):
        pass
    def merge(self, other):
        pass
    def finish(self):
        return self.state
    @property
    def aggregate_state(self):
        return self.state
\$\$;
        """
        exception "ARRAY unsupported sub-type: bitmap"
    }

    test {
        sql """
        CREATE AGGREGATE FUNCTION py_obj_udaf_struct_bitmap(int)
        RETURNS STRUCT<plain:INT, nested:MAP<INT, ARRAY<HLL>>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Agg",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class Agg:
    def __init__(self):
        self.state = None
    def accumulate(self, v):
        pass
    def merge(self, other):
        pass
    def finish(self):
        return self.state
    @property
    def aggregate_state(self):
        return self.state
\$\$;
        """
        exception "ARRAY unsupported sub-type: hll"
    }
}
