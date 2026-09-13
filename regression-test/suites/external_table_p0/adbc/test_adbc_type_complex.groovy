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

// ############################################################################
// ARRAY, MAP and STRUCT through ADBC, including nesting several levels deep.
//
// These are the types where the mapping is recursive, so a defect does not
// have to be in the type itself -- it can be in how a container asks about its
// child. AdbcTypeMapper recurses through five list flavours, a struct, and a
// map modelled as list<struct<key,value>>, and each level re-enters the same
// switch. So the cases that matter are the COMBINATIONS: array of struct, map
// of array, struct holding a map, and a four-level array/map/struct/array
// tower that no single-level test reaches.
//
// Two behaviours are pinned on purpose:
//   * struct child names come back LOWERCASED. AdbcTypeMapper does that
//     deliberately (BE indexes struct children by lowercase key and a
//     mixed-case name crashes it), so a mixed-case field in the source is the
//     only way to notice if that ever stops happening. Values are read with
//     struct_element by INDEX so the assertions do not depend on the name.
//   * an empty collection is not a null one. A source that maps [] to NULL
//     loses a distinction no aggregate would reveal.
//
// Sections run simplest first: a fixture the source cannot build for a deeply
// nested literal fails at the END, after the flat cases have already been
// compared.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_type_complex", "p0,external") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String driverPath = context.config.otherConfigs.get("adbcDriverPath")
    if (driverPath == null || driverPath.isEmpty()) {
        driverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }

    if (!new File(driverPath).canRead()) {
        logger.info("SKIPPED test_adbc_type_complex: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "NESTED ADBC TYPES ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_type_complex_catalog"
    String dbName = "test_adbc_type_complex_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // ---- fixtures ----

    sql """
        CREATE TABLE internal.${dbName}.t_array (
          `id` int NOT NULL,
          `a_int` array<int> NULL,
          `a_str` array<string> NULL,
          `a_dec` array<decimalv3(10, 2)> NULL,
          `a_date` array<date> NULL
        ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Row 2 holds a null INSIDE an array, row 3 an empty array, row 4 a null array. All three read back as
    // something plausible if the offsets are off by one, and only together do they pin the difference.
    sql """
        INSERT INTO internal.${dbName}.t_array VALUES
          (1, [1, 2, 3], ['a', 'b'], [1.25, -2.50], ['2024-01-01', '2024-12-31']),
          (2, [1, NULL, 3], ['a', NULL], [NULL, 0.01], [NULL]),
          (3, [], [], [], []),
          (4, NULL, NULL, NULL, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_map (
          `id` int NOT NULL,
          `m_si` map<string, int> NULL,
          `m_is` map<int, string> NULL,
          `m_sd` map<string, double> NULL
        ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.t_map VALUES
          (1, {'a': 1, 'b': 2}, {1: 'x', 2: 'y'}, {'pi': 3.5}),
          (2, {'a': NULL}, {1: NULL}, {'nan_free': NULL}),
          (3, {}, {}, {}),
          (4, NULL, NULL, NULL)
    """

    // The child names are mixed case ON PURPOSE: the connector lowercases them, and a source that spells
    // them any other way is the only thing that can show that rule is still in force.
    sql """
        CREATE TABLE internal.${dbName}.t_struct (
          `id` int NOT NULL,
          `s_flat` struct<FieldInt: int, FieldStr: string, FieldDbl: double> NULL
        ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // The nulls are cast explicitly: a bare NULL in a struct constructor leaves the source to guess the
    // field's type, and a fixture that fails to build says nothing about ADBC.
    sql """
        INSERT INTO internal.${dbName}.t_struct VALUES
          (1, struct(1, 'one', 1.5)),
          (2, struct(CAST(NULL AS int), CAST(NULL AS string), CAST(NULL AS double))),
          (3, NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.t_nested (
          `id` int NOT NULL,
          `aa_int` array<array<int>> NULL,
          `a_of_s` array<struct<sid: int, sname: string>> NULL,
          `m_of_a` map<string, array<int>> NULL,
          `s_of_m` struct<inner_map: map<string, int>, inner_arr: array<string>> NULL
        ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Row 2 keeps the empty and null cases where the source can still type them: an empty array INSIDE a
    // map value, a null array element, and cast nulls in a struct. Empty collections directly inside a
    // struct constructor are left to the top-level tables above, where the column type states what they
    // are.
    // array(...) rather than [...] around the struct elements: Doris's bracket literal takes values,
    // not constructor calls, so [struct(1, 'a')] is a parse error ("no viable alternative at input
    // '[struct'"). The bracket form is kept everywhere it does parse, which is most of this fixture.
    sql """
        INSERT INTO internal.${dbName}.t_nested VALUES
          (1, [[1, 2], [3]], array(struct(1, 'a'), struct(2, 'b')), {'k1': [1, 2], 'k2': [3]},
              struct({'a': 1}, ['x', 'y'])),
          (2, [[], [NULL]], array(struct(CAST(NULL AS int), CAST(NULL AS string))), {'empty': []},
              struct({'z': 9}, ['q'])),
          (3, NULL, NULL, NULL, NULL)
    """

    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "required"
        )
    """

    try {
        // Compares an ADBC read against a native read of the same source table. Independent of the .out
        // baselines: a baseline generated from a run that already flattened a nested value would record
        // the flattened form and pass forever, whereas the native read is a second path to a known answer.
        def sameAsSource = { String table, String columns ->
            def viaAdbc = sql("SELECT ${columns} FROM ${catalogName}.${dbName}.${table} ORDER BY id")
            def viaSource = sql("SELECT ${columns} FROM internal.${dbName}.${table} ORDER BY id")
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "reading ${table}(${columns}) through ADBC returned different values than a native "
                            + "read of the same source table")
        }

        // ---- arrays ----

        qt_desc_array """DESC ${catalogName}.${dbName}.t_array"""
        qt_select_array """
            SELECT id, a_int, a_str, a_dec, a_date FROM ${catalogName}.${dbName}.t_array ORDER BY id
        """
        sameAsSource("t_array", "id, a_int, a_str, a_dec, a_date")

        // Sizes separate "the array is wrong" from "the array is empty or null", which print alike enough
        // to be confused when reading a baseline.
        qt_select_array_shape """
            SELECT id, size(a_int), size(a_str), a_int IS NULL, array_contains(a_int, 2)
            FROM ${catalogName}.${dbName}.t_array ORDER BY id
        """

        // Element access, so a wrong offset inside the child buffer cannot hide behind a whole-array print.
        qt_select_array_element """
            SELECT id, element_at(a_int, 1), element_at(a_int, 2), element_at(a_str, 1)
            FROM ${catalogName}.${dbName}.t_array ORDER BY id
        """

        // ---- maps ----

        qt_desc_map """DESC ${catalogName}.${dbName}.t_map"""
        qt_select_map """
            SELECT id, m_si, m_is, m_sd FROM ${catalogName}.${dbName}.t_map ORDER BY id
        """
        sameAsSource("t_map", "id, m_si, m_is, m_sd")

        // Arrow models a map as list<struct<key,value>>; keys and values are separate child arrays, so a
        // map read with the pair mis-paired still prints the right key set.
        qt_select_map_parts """
            SELECT id, map_keys(m_si), map_values(m_si), element_at(m_si, 'a'), size(m_si)
            FROM ${catalogName}.${dbName}.t_map ORDER BY id
        """

        // ---- structs ----

        // The mapping is what this baseline is for: the child names must be lowercase here even though the
        // source spells them FieldInt / FieldStr / FieldDbl.
        qt_desc_struct """DESC ${catalogName}.${dbName}.t_struct"""
        qt_select_struct """SELECT id, s_flat FROM ${catalogName}.${dbName}.t_struct ORDER BY id"""
        sameAsSource("t_struct", "id, s_flat")

        // By index, not by name: the value assertions must not double as name assertions, or a change to
        // the lowercasing rule would fail here for a reason that has nothing to do with the values.
        qt_select_struct_element """
            SELECT id, struct_element(s_flat, 1), struct_element(s_flat, 2), struct_element(s_flat, 3)
            FROM ${catalogName}.${dbName}.t_struct ORDER BY id
        """

        // The lowercasing rule, spelled out rather than only baselined: a baseline records whatever
        // happened, so this is the line that says what SHOULD happen.
        def structColumns = sql("""DESC ${catalogName}.${dbName}.t_struct""")
        String structType = structColumns.find { it[0] == "s_flat" }[1].toString()
        assertTrue(structType.toLowerCase().contains("fieldint"),
                "the struct column lost its children: ${structType}")
        assertEquals(structType, structType.toLowerCase(),
                "struct child names reached Doris with upper case in them, which BE cannot index: "
                        + structType)

        // ---- two and three levels ----

        qt_desc_nested """DESC ${catalogName}.${dbName}.t_nested"""
        qt_select_nested """
            SELECT id, aa_int, a_of_s, m_of_a, s_of_m FROM ${catalogName}.${dbName}.t_nested ORDER BY id
        """
        sameAsSource("t_nested", "id, aa_int, a_of_s, m_of_a, s_of_m")

        qt_select_nested_reach """
            SELECT id,
                   element_at(element_at(aa_int, 1), 2),
                   struct_element(element_at(a_of_s, 1), 2),
                   element_at(element_at(m_of_a, 'k1'), 1),
                   size(struct_element(s_of_m, 2))
            FROM ${catalogName}.${dbName}.t_nested ORDER BY id
        """

        // ---- four levels: array -> map -> struct -> array ----
        //
        // Last, because it is the fixture most likely to defeat the source's own literal support. Every
        // level here is a different branch of the recursion, which is what makes the tower worth building:
        // three levels of the same container would re-enter the same case three times.
        sql """
            CREATE TABLE internal.${dbName}.t_deep (
              `id` int NOT NULL,
              `deep` array<map<string, struct<x: int, y: array<double>>>> NULL,
              `deep_struct` struct<lv1: struct<lv2: struct<lv3: int>>> NULL
            ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        // The null at the bottom of the tower is a cast scalar, which the source can type; the levels above
        // it always carry a real value, so a fixture failure here would be about literal support rather
        // than about anything this suite tests.
        // array(map(...)) for the same reason as t_nested above: a bracket/brace literal cannot hold a
        // struct() call. Row 3's empty [] stays a literal -- there is no constructor call inside it.
        sql """
            INSERT INTO internal.${dbName}.t_deep VALUES
              (1, array(map('k', struct(1, [1.5, 2.5])), map('j', struct(2, []))),
                  struct(struct(struct(42)))),
              (2, array(map('k', struct(2, [3.5]))), struct(struct(struct(CAST(NULL AS int))))),
              (3, [], struct(struct(struct(7)))),
              (4, NULL, NULL)
        """

        qt_desc_deep """DESC ${catalogName}.${dbName}.t_deep"""
        qt_select_deep """SELECT id, deep, deep_struct FROM ${catalogName}.${dbName}.t_deep ORDER BY id"""
        sameAsSource("t_deep", "id, deep, deep_struct")

        qt_select_deep_reach """
            SELECT id,
                   struct_element(element_at(element_at(deep, 1), 'k'), 1),
                   element_at(struct_element(element_at(element_at(deep, 1), 'k'), 2), 2),
                   struct_element(struct_element(struct_element(deep_struct, 1), 1), 1)
            FROM ${catalogName}.${dbName}.t_deep ORDER BY id
        """
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
