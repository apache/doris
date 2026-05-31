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

suite("test_pythonudf_nested_complex_type") {
    def runtime_version = getPythonUdfRuntimeVersion()

    sql """ DROP TABLE IF EXISTS test_pythonudf_nested_complex_type; """
    sql """
        CREATE TABLE test_pythonudf_nested_complex_type (
            id INT,
            array_map ARRAY<MAP<STRING, INT>>,
            map_array_map MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            struct_nested STRUCT<
                label: STRING,
                maps: ARRAY<MAP<STRING, INT>>,
                attrs: MAP<STRING, ARRAY<INT>>
            >,
            map_struct_nested MAP<STRING, STRUCT<
                tag: STRING,
                metrics: ARRAY<MAP<STRING, INT>>
            >>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_pythonudf_nested_complex_type VALUES
        (
            1,
            [{'a': 1, 'b': 2}, {'c': 3}],
            {'left': [{'x': 10}], 'right': [{'y': 20}, {'z': 30}]},
            {'row1', [{'s': 7}, {'t': 8}], {'nums': [1, 2], 'empty': []}},
            {'first': {'tagA', [{'m': 1}, {'n': 2}]}, 'second': {'tagB', []}}
        ),
        (
            2,
            [],
            {'empty': []},
            {'row2', [], {'none': NULL}},
            {'empty': {'tagEmpty', []}}
        ),
        (
            3,
            NULL,
            NULL,
            NULL,
            NULL
        );
    """

    sql """
        DROP FUNCTION IF EXISTS py_nested_complex_scalar(
            ARRAY<MAP<STRING, INT>>,
            MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        );
    """
    sql """
        CREATE FUNCTION py_nested_complex_scalar(
            ARRAY<MAP<STRING, INT>>,
            MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        )
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def format_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{m[k]}' for k in sorted(m)) + '}'

def format_array_map(arr):
    if arr is None:
        return 'NULL'
    return '[' + ','.join(format_map(item) for item in arr) + ']'

def format_map_array_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP_ARRAY_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{format_array_map(m[k])}' for k in sorted(m)) + '}'

def format_attrs(attrs):
    if attrs is None:
        return 'NULL'
    if not isinstance(attrs, dict):
        return 'BAD_ATTRS:' + type(attrs).__name__
    parts = []
    for key in sorted(attrs):
        val = attrs[key]
        if val is None:
            parts.append(f'{key}:NULL')
        else:
            parts.append(f'{key}:[' + ','.join(str(x) for x in val) + ']')
    return '{' + ','.join(parts) + '}'

def format_struct(s):
    if s is None:
        return 'NULL'
    if not isinstance(s, dict):
        return 'BAD_STRUCT:' + type(s).__name__
    return '(' + str(s.get('label')) + ',' + format_array_map(s.get('maps')) + ',' + format_attrs(s.get('attrs')) + ')'

def format_map_struct_nested(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP_STRUCT:' + type(m).__name__
    parts = []
    for key in sorted(m):
        val = m[key]
        if val is None:
            parts.append(f'{key}:NULL')
        elif not isinstance(val, dict):
            parts.append(f'{key}:BAD_STRUCT:' + type(val).__name__)
        else:
            parts.append(f'{key}:(' + str(val.get('tag')) + ',' + format_array_map(val.get('metrics')) + ')')
    return '{' + ','.join(parts) + '}'

def evaluate(array_map, map_array_map, struct_nested, map_struct_nested):
    return '|'.join([
        format_array_map(array_map),
        format_map_array_map(map_array_map),
        format_struct(struct_nested),
        format_map_struct_nested(map_struct_nested),
    ])
\$\$;
    """

    qt_scalar_constant_nested_complex """
        SELECT result
        FROM (
            SELECT 1 AS ord, py_nested_complex_scalar(
                CAST([{'a': 1, 'b': 2}, {'c': 3}] AS ARRAY<MAP<STRING, INT>>),
                CAST({'left': [{'x': 10}], 'right': [{'y': 20}, {'z': 30}]} AS MAP<STRING, ARRAY<MAP<STRING, INT>>>),
                CAST({'const', [{'s': 7}], {'nums': [1, 2], 'empty': []}} AS STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>),
                CAST({'const_key': {'constTag', [{'cm': 11}]}} AS MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>)
            ) AS result
            UNION ALL
            SELECT 2 AS ord, py_nested_complex_scalar(
                CAST([] AS ARRAY<MAP<STRING, INT>>),
                CAST({'empty': []} AS MAP<STRING, ARRAY<MAP<STRING, INT>>>),
                CAST({'empty', [], {'none': NULL}} AS STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>),
                CAST({'empty': {'emptyTag', []}} AS MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>)
            ) AS result
            UNION ALL
            SELECT 3 AS ord, py_nested_complex_scalar(
                CAST(NULL AS ARRAY<MAP<STRING, INT>>),
                CAST(NULL AS MAP<STRING, ARRAY<MAP<STRING, INT>>>),
                CAST(NULL AS STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>),
                CAST(NULL AS MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>)
            ) AS result
        ) ordered_result
        ORDER BY ord;
    """

    sql """
        DROP FUNCTION IF EXISTS py_nested_complex_vector_list(
            ARRAY<MAP<STRING, INT>>,
            MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        );
    """
    sql """
        CREATE FUNCTION py_nested_complex_vector_list(
            ARRAY<MAP<STRING, INT>>,
            MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        )
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def format_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{m[k]}' for k in sorted(m)) + '}'

def format_array_map(arr):
    if arr is None:
        return 'NULL'
    return '[' + ','.join(format_map(item) for item in arr) + ']'

def format_map_array_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP_ARRAY_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{format_array_map(m[k])}' for k in sorted(m)) + '}'

def format_attrs(attrs):
    if attrs is None:
        return 'NULL'
    if not isinstance(attrs, dict):
        return 'BAD_ATTRS:' + type(attrs).__name__
    parts = []
    for key in sorted(attrs):
        val = attrs[key]
        if val is None:
            parts.append(f'{key}:NULL')
        else:
            parts.append(f'{key}:[' + ','.join(str(x) for x in val) + ']')
    return '{' + ','.join(parts) + '}'

def format_struct(s):
    if s is None:
        return 'NULL'
    if not isinstance(s, dict):
        return 'BAD_STRUCT:' + type(s).__name__
    return '(' + str(s.get('label')) + ',' + format_array_map(s.get('maps')) + ',' + format_attrs(s.get('attrs')) + ')'

def format_map_struct_nested(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP_STRUCT:' + type(m).__name__
    parts = []
    for key in sorted(m):
        val = m[key]
        if val is None:
            parts.append(f'{key}:NULL')
        elif not isinstance(val, dict):
            parts.append(f'{key}:BAD_STRUCT:' + type(val).__name__)
        else:
            parts.append(f'{key}:(' + str(val.get('tag')) + ',' + format_array_map(val.get('metrics')) + ')')
    return '{' + ','.join(parts) + '}'

def evaluate(array_maps: list, map_array_maps: list, struct_nesteds: list, map_struct_nesteds: list):
    result = []
    for array_map, map_array_map, struct_nested, map_struct_nested in zip(array_maps, map_array_maps, struct_nesteds, map_struct_nesteds):
        result.append('|'.join([
            format_array_map(array_map),
            format_map_array_map(map_array_map),
            format_struct(struct_nested),
            format_map_struct_nested(map_struct_nested),
        ]))
    return result
\$\$;
    """

    qt_vector_list_nested_complex """
        SELECT py_nested_complex_vector_list(array_map, map_array_map, struct_nested, map_struct_nested) AS result
        FROM test_pythonudf_nested_complex_type
        ORDER BY id;
    """

    sql """
        DROP FUNCTION IF EXISTS py_nested_complex_vector_series(
            ARRAY<MAP<STRING, INT>>,
            MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        );
    """
    sql """
        CREATE FUNCTION py_nested_complex_vector_series(
            ARRAY<MAP<STRING, INT>>,
            MAP<STRING, ARRAY<MAP<STRING, INT>>>,
            STRUCT<label: STRING, maps: ARRAY<MAP<STRING, INT>>, attrs: MAP<STRING, ARRAY<INT>>>,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        )
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def format_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{m[k]}' for k in sorted(m)) + '}'

def format_array_map(arr):
    if arr is None:
        return 'NULL'
    return '[' + ','.join(format_map(item) for item in arr) + ']'

def format_map_array_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP_ARRAY_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{format_array_map(m[k])}' for k in sorted(m)) + '}'

def format_attrs(attrs):
    if attrs is None:
        return 'NULL'
    if not isinstance(attrs, dict):
        return 'BAD_ATTRS:' + type(attrs).__name__
    parts = []
    for key in sorted(attrs):
        val = attrs[key]
        if val is None:
            parts.append(f'{key}:NULL')
        else:
            parts.append(f'{key}:[' + ','.join(str(x) for x in val) + ']')
    return '{' + ','.join(parts) + '}'

def format_struct(s):
    if s is None:
        return 'NULL'
    if not isinstance(s, dict):
        return 'BAD_STRUCT:' + type(s).__name__
    return '(' + str(s.get('label')) + ',' + format_array_map(s.get('maps')) + ',' + format_attrs(s.get('attrs')) + ')'

def format_map_struct_nested(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP_STRUCT:' + type(m).__name__
    parts = []
    for key in sorted(m):
        val = m[key]
        if val is None:
            parts.append(f'{key}:NULL')
        elif not isinstance(val, dict):
            parts.append(f'{key}:BAD_STRUCT:' + type(val).__name__)
        else:
            parts.append(f'{key}:(' + str(val.get('tag')) + ',' + format_array_map(val.get('metrics')) + ')')
    return '{' + ','.join(parts) + '}'

def evaluate(array_maps: pd.Series, map_array_maps: pd.Series, struct_nesteds: pd.Series, map_struct_nesteds: pd.Series) -> pd.Series:
    if not all(isinstance(arg, pd.Series) for arg in [array_maps, map_array_maps, struct_nesteds, map_struct_nesteds]):
        return pd.Series(['BAD_VECTOR_ARGS'] * len(array_maps))
    result = []
    for array_map, map_array_map, struct_nested, map_struct_nested in zip(array_maps, map_array_maps, struct_nesteds, map_struct_nesteds):
        result.append('|'.join([
            format_array_map(array_map),
            format_map_array_map(map_array_map),
            format_struct(struct_nested),
            format_map_struct_nested(map_struct_nested),
        ]))
    return pd.Series(result)
\$\$;
    """

    qt_vector_series_nested_complex """
        SELECT py_nested_complex_vector_series(array_map, map_array_map, struct_nested, map_struct_nested) AS result
        FROM test_pythonudf_nested_complex_type
        ORDER BY id;
    """

    sql """
        DROP FUNCTION IF EXISTS py_nested_complex_vector_mixed(
            INT,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        );
    """
    sql """
        CREATE FUNCTION py_nested_complex_vector_mixed(
            INT,
            MAP<STRING, STRUCT<tag: STRING, metrics: ARRAY<MAP<STRING, INT>>>>
        )
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def format_map(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MAP:' + type(m).__name__
    return '{' + ','.join(f'{k}:{m[k]}' for k in sorted(m)) + '}'

def format_array_map(arr):
    if arr is None:
        return 'NULL'
    return '[' + ','.join(format_map(item) for item in arr) + ']'

def format_map_struct_nested(m):
    if m is None:
        return 'NULL'
    if not isinstance(m, dict):
        return 'BAD_MIXED_SCALAR:' + type(m).__name__
    parts = []
    for key in sorted(m):
        val = m[key]
        if val is None:
            parts.append(f'{key}:NULL')
        elif not isinstance(val, dict):
            parts.append(f'{key}:BAD_STRUCT:' + type(val).__name__)
        else:
            parts.append(f'{key}:(' + str(val.get('tag')) + ',' + format_array_map(val.get('metrics')) + ')')
    return '{' + ','.join(parts) + '}'

def evaluate(ids: pd.Series, mixed_map_struct_nested) -> pd.Series:
    if not isinstance(ids, pd.Series):
        return pd.Series(['BAD_VECTOR_ARG'])
    formatted = format_map_struct_nested(mixed_map_struct_nested)
    return pd.Series([str(id_value) + '|' + formatted for id_value in ids])
\$\$;
    """

    qt_vector_mixed_scalar_nested_complex """
        SELECT py_nested_complex_vector_mixed(id, map_struct_nested) AS result
        FROM test_pythonudf_nested_complex_type
        ORDER BY id;
    """
}
