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

suite("test_pythonudf_complex_data_type") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())

    // TEST ARRAY INLINE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS array_to_csv(
                ARRAY<INT>,
                ARRAY<STRING>,
                ARRAY<ARRAY<INT>>
            );
        """
        sql """
CREATE FUNCTION array_to_csv(
    ARRAY<INT>,
    ARRAY<STRING>,
    ARRAY<ARRAY<INT>>
)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "array_to_csv_impl",
    "always_nullable" = "true",
    "runtime_version" = "${runtime_version}"
)
AS \$\$
def array_to_csv_impl(int_arr, str_arr, nested_arr):
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    def format_array(arr):
        if arr is None:
            return 'NULL'
        return '[' + ','.join(safe_str(item) for item in arr) + ']'
    
    def format_nested_array(arr):
        if arr is None:
            return 'NULL'
        return '[' + ','.join(format_array(inner) for inner in arr) + ']'
    
    parts = [
        format_array(int_arr),
        format_array(str_arr),
        format_nested_array(nested_arr)
    ]
    return '|'.join(parts)
\$\$;
        """
        sql """ DROP TABLE IF EXISTS test_array_table; """
        sql """
            CREATE TABLE test_array_table (
                id INT,
                int_array ARRAY<INT>,
                string_array ARRAY<STRING>,
                nested_array ARRAY<ARRAY<INT>>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_array_table VALUES
            (1, [1, 2, 3], ['a', 'b', 'c'], [[1,2], [3,4]]),
            (2, [], [], []),
            (3, NULL, ['x', NULL, 'z'], NULL),
            (4, [0, -1, 2147483647], ['hello', 'world'], [[], [1]]);
        """

        qt_select_1 """
            SELECT array_to_csv(int_array, string_array, nested_array) AS result FROM test_array_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS array_to_csv(
                ARRAY<INT>,
                ARRAY<STRING>,
                ARRAY<ARRAY<INT>>
            );""")
        try_sql("DROP TABLE IF EXISTS test_array_table;")
    }

    // TEST ARRAY MODULE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS array_to_csv(
                ARRAY<INT>,
                ARRAY<STRING>,
                ARRAY<ARRAY<INT>>
            );
        """
        sql """
            CREATE FUNCTION array_to_csv(
                ARRAY<INT>,
                ARRAY<STRING>,
                ARRAY<ARRAY<INT>>
            )
            RETURNS STRING
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file"="file://${pyPath}",
                "symbol" = "python_udf_array_type.array_to_csv_impl",
                "always_nullable" = "true",
                "runtime_version" = "${runtime_version}"
            );
        """
        sql """ DROP TABLE IF EXISTS test_array_table; """
        sql """
            CREATE TABLE test_array_table (
                id INT,
                int_array ARRAY<INT>,
                string_array ARRAY<STRING>,
                nested_array ARRAY<ARRAY<INT>>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_array_table VALUES
            (1, [1, 2, 3], ['a', 'b', 'c'], [[1,2], [3,4]]),
            (2, [], [], []),
            (3, NULL, ['x', NULL, 'z'], NULL),
            (4, [0, -1, 2147483647], ['hello', 'world'], [[], [1]]);
        """


        qt_select_2 """
            SELECT array_to_csv(int_array, string_array, nested_array) AS result FROM test_array_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS array_to_csv(
                ARRAY<INT>,
                ARRAY<STRING>,
                ARRAY<ARRAY<INT>>
            );""")
        try_sql("DROP TABLE IF EXISTS test_array_table;")
    }

    // TEST MAP INLINE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS map_to_csv(
                MAP<INT, STRING>,
                MAP<STRING, DOUBLE>
            );
        """
        sql """
CREATE FUNCTION map_to_csv(
    MAP<INT, STRING>,
    MAP<STRING, DOUBLE>
)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "map_to_csv_impl",
    "always_nullable" = "true",
    "runtime_version" = "${runtime_version}"
)
AS \$\$
def map_to_csv_impl(map1, map2):
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    def format_map(m):
        if m is None:
            return 'NULL'
        # Doris passes MAP as Python dict
        items = [f"{safe_str(k)}:{safe_str(v)}" for k, v in m.items()]
        return '{' + ','.join(sorted(items)) + '}'
    
    return '|'.join([format_map(map1), format_map(map2)])
\$\$;
        """
        sql """ DROP TABLE IF EXISTS test_map_table; """
        sql """
            CREATE TABLE test_map_table (
                id INT,
                int_string_map MAP<INT, STRING>,
                string_double_map MAP<STRING, DOUBLE>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_map_table VALUES
            (1, {1:'one', 2:'two'}, {'pi':3.14, 'e':2.718}),
            (2, {}, {}),
            (3, NULL, {'null_key': NULL}),
            (4, {0:'zero', -1:'minus_one'}, {'max':1.79769e308});
        """

        qt_select_3 """
            SELECT map_to_csv(int_string_map, string_double_map) AS result FROM test_map_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS map_to_csv(
                    MAP<INT, STRING>,
                    MAP<STRING, DOUBLE>
                );""")
        try_sql("DROP TABLE IF EXISTS test_map_table;")
    }

    // TEST MAP MODULE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS map_to_csv(
                MAP<INT, STRING>,
                MAP<STRING, DOUBLE>
            );
        """
        sql """
            CREATE FUNCTION map_to_csv(
                MAP<INT, STRING>,
                MAP<STRING, DOUBLE>
            )
            RETURNS STRING
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file"="file://${pyPath}",
                "symbol" = "python_udf_map_type.map_to_csv_impl",
                "always_nullable" = "true",
                "runtime_version" = "${runtime_version}"
            );
        """
        sql """ DROP TABLE IF EXISTS test_map_table; """
        sql """
            CREATE TABLE test_map_table (
                id INT,
                int_string_map MAP<INT, STRING>,
                string_double_map MAP<STRING, DOUBLE>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_map_table VALUES
            (1, {1:'one', 2:'two'}, {'pi':3.14, 'e':2.718}),
            (2, {}, {}),
            (3, NULL, {'null_key': NULL}),
            (4, {0:'zero', -1:'minus_one'}, {'max':1.79769e308});
        """

        qt_select_4 """
            SELECT map_to_csv(int_string_map, string_double_map) AS result FROM test_map_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS map_to_csv(
                    MAP<INT, STRING>,
                    MAP<STRING, DOUBLE>
                );""")
        try_sql("DROP TABLE IF EXISTS test_map_table;")
    }

    // TEST STRUCT INLINE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS struct_to_csv(
                STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
            );
        """
        sql """
CREATE FUNCTION struct_to_csv(
    STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
    STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "struct_to_csv_impl",
    "always_nullable" = "true",
    "runtime_version" = "${runtime_version}"
)
AS \$\$
def struct_to_csv_impl(person, point):
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    def format_array(arr):
        if arr is None:
            return 'NULL'
        return '[' + ','.join(safe_str(item) for item in arr) + ']'
    
    def format_struct_dict(s, field_names):
        if s is None:
            return 'NULL'
        parts = []
        for field in field_names:
            val = s.get(field)
            parts.append(safe_str(val))
        return '(' + ','.join(parts) + ')'
    
    person_str = format_struct_dict(person, ['name', 'age', 'salary'])
    
    if point is None:
        point_str = 'NULL'
    else:
        x_val = safe_str(point.get('x'))
        y_val = safe_str(point.get('y'))
        tags_val = format_array(point.get('tags'))
        point_str = f"({x_val},{y_val},{tags_val})"
    
    return '|'.join([person_str, point_str])
\$\$;
        """
        sql """ DROP TABLE IF EXISTS test_struct_table; """
        sql """
            CREATE TABLE test_struct_table(
                id INT,
                person STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                point STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_struct_table VALUES
            (1, {'Alice', 30, 75000.50}, {1.5, 2.5, ['red', 'blue']}),
            (2, {NULL, NULL, NULL}, {0.0, 0.0, []}),
            (3, {'Bob', 25, 60000.00}, {NULL, 3.14, ['tag1', NULL, 'tag3']}),
            (4, {'', 0, 0.0}, {-1.0, -2.0, NULL});
        """

        qt_select_5 """
            SELECT struct_to_csv(person, point) AS result FROM test_struct_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS struct_to_csv(
                    STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                    STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
                );""")
        try_sql("DROP TABLE IF EXISTS test_struct_table;")
    }

    // TEST STRUCT MODULE CASE
    try {
        sql """
            DROP FUNCTION IF EXISTS struct_to_csv(
                STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
            );
        """
        sql """
            CREATE FUNCTION struct_to_csv(
                STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
            )
            RETURNS STRING
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file"="file://${pyPath}",
                "symbol" = "python_udf_struct_type.struct_to_csv_impl",
                "always_nullable" = "true",
                "runtime_version" = "${runtime_version}"
            );
        """
        sql """ DROP TABLE IF EXISTS test_struct_table; """
        sql """
            CREATE TABLE test_struct_table(
                id INT,
                person STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                point STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO test_struct_table VALUES
            (1, {'Alice', 30, 75000.50}, {1.5, 2.5, ['red', 'blue']}),
            (2, {NULL, NULL, NULL}, {0.0, 0.0, []}),
            (3, {'Bob', 25, 60000.00}, {NULL, 3.14, ['tag1', NULL, 'tag3']}),
            (4, {'', 0, 0.0}, {-1.0, -2.0, NULL});
        """

        qt_select_6 """
            SELECT struct_to_csv(person, point) AS result FROM test_struct_table;
        """
    } finally {
        try_sql("""DROP FUNCTION IF EXISTS struct_to_csv(
                    STRUCT<name: STRING, age: INT, salary: DECIMAL(12,2)>,
                    STRUCT<x: DOUBLE, y: DOUBLE, tags: ARRAY<STRING>>
                );""")
        try_sql("DROP TABLE IF EXISTS test_struct_table;")
    }
}
