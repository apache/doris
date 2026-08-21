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

// Covers the JNI writer path of a TVF sink, i.e. the "writer_class" property, which is the only
// entry point of the java-writer plugin. test_insert_into_local_tvf next door covers the native
// writers; everything here goes through Java instead.
//
// Two things make this path different from the native one and both are asserted below:
//   - writer_class names a plugin factory ("<plugin>:<factory>"), not a Java class. A class name
//     cannot identify a writer once each plugin loads its own classes.
//   - the Java writer opens file_path literally, so there is no query id and no extension in the
//     name. Each case therefore writes to a path of its own rather than a shared prefix.
suite("test_insert_into_local_tvf_jni", "p0,external") {

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def be_host = backends[0][1]
    def basePath = "/tmp/test_insert_into_local_tvf_jni"

    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sshExec("root", be_host, "mkdir -p ${basePath}")
    sshExec("root", be_host, "chmod 777 ${basePath}")

    // ============ Source tables ============

    sql """ DROP TABLE IF EXISTS test_local_tvf_jni_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_local_tvf_jni_src (
            c_int       INT,
            c_bool      BOOLEAN,
            c_tinyint   TINYINT,
            c_smallint  SMALLINT,
            c_bigint    BIGINT,
            c_largeint  LARGEINT,
            c_float     FLOAT,
            c_double    DOUBLE,
            c_decimal   DECIMAL(10,2),
            c_decimal38 DECIMAL(38,10),
            c_date      DATE,
            c_datetime  DATETIME,
            c_datetime6 DATETIME(6),
            c_char      CHAR(10),
            c_varchar   VARCHAR(100),
            c_string    STRING
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_local_tvf_jni_src VALUES
            (1, true,  1,    100,   100000,   123456789012345678901234567890,
             1.25,  2.5,  123.45,  1234567890.1234567890,
             '2024-01-01', '2024-01-01 10:00:00', '2024-01-01 10:00:00.123456',
             'char1', 'varchar1', 'string1'),
            (2, false, -1,  -100,  -100000,  -123456789012345678901234567890,
             -1.25, -2.5, -123.45, -1234567890.1234567890,
             '2020-02-29', '2020-02-29 00:00:00', '2020-02-29 00:00:00.000001',
             '', '', ''),
            (3, NULL,  NULL, NULL,  NULL,     NULL,
             NULL,  NULL, NULL,    NULL,
             NULL,         NULL,                  NULL,
             NULL,    NULL,       NULL);
    """

    sql """ DROP TABLE IF EXISTS test_local_tvf_jni_complex_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_local_tvf_jni_complex_src (
            c_int    INT,
            c_array  ARRAY<INT>,
            c_arrstr ARRAY<STRING>,
            c_map    MAP<STRING, INT>,
            c_struct STRUCT<f1:INT, f2:STRING>
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_local_tvf_jni_complex_src VALUES
            (1, [1, 2, 3], ['a', 'b'], {'a': 1, 'b': 2}, {1, 'hello'}),
            (2, [],        [],         {},               {2, ''}),
            (3, NULL,      NULL,       NULL,             NULL);
    """

    // ============ 1. All basic types, written by the plugin and read back ============

    sql """
        INSERT INTO local(
            "file_path"        = "${basePath}/basic",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = ",",
            "writer_class"     = "java-writer:local-file"
        ) SELECT * FROM test_local_tvf_jni_src ORDER BY c_int;
    """

    order_qt_jni_basic_types """
        SELECT * FROM local(
            "file_path"        = "${basePath}/basic",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = ","
        ) ORDER BY c1;
    """

    // Same rows again, this time read as one column per line so the baseline pins the bytes the
    // plugin actually wrote: the separator it was handed, and \N for a null. The writer renders a
    // value with Java's toString(), so booleans read true/false and datetimes are ISO-8601 - it
    // does not match the native CSV writer's rendering, and never has.
    order_qt_jni_raw_lines """
        SELECT * FROM local(
            "file_path"        = "${basePath}/basic",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = "|@|"
        ) ORDER BY c1;
    """

    // ============ 2. A separator the caller chose, not the default ============
    // column_separator and line_delimiter reach the plugin as scan parameters; a wrong or missing
    // one shows up here as a single unsplit column.

    sql """
        INSERT INTO local(
            "file_path"        = "${basePath}/pipe_sep",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = "|",
            "line_delimiter"   = "\n",
            "writer_class"     = "java-writer:local-file"
        ) SELECT c_int, c_varchar, c_double FROM test_local_tvf_jni_src ORDER BY c_int;
    """

    order_qt_jni_custom_separator """
        SELECT * FROM local(
            "file_path"        = "${basePath}/pipe_sep",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = "|"
        ) ORDER BY c1;
    """

    // ============ 3. Complex types ============
    // These go through VectorTable.getMaterializedData() on nested columns, which is the part of
    // the SPI that a writer exercises and a scanner does not.

    sql """
        INSERT INTO local(
            "file_path"        = "${basePath}/complex",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = "|@|",
            "writer_class"     = "java-writer:local-file"
        ) SELECT * FROM test_local_tvf_jni_complex_src ORDER BY c_int;
    """

    order_qt_jni_complex_types """
        SELECT * FROM local(
            "file_path"        = "${basePath}/complex",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = "|@|"
        ) ORDER BY c1;
    """

    // ============ 4. Every row survives more than one batch ============
    // The plugin is handed one VectorTable per batch; this is enough rows to need several.

    sql """ DROP TABLE IF EXISTS test_local_tvf_jni_bulk_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_local_tvf_jni_bulk_src (
            c_int INT,
            c_str VARCHAR(32)
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 4
        PROPERTIES("replication_num" = "1");
    """
    sql """ INSERT INTO test_local_tvf_jni_bulk_src SELECT number, concat('row_', number) FROM numbers("number" = "50000"); """

    sql """
        INSERT INTO local(
            "file_path"        = "${basePath}/bulk",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = ",",
            "writer_class"     = "java-writer:local-file"
        ) SELECT * FROM test_local_tvf_jni_bulk_src;
    """

    qt_jni_bulk_row_count """
        SELECT count(*) FROM local(
            "file_path"        = "${basePath}/bulk",
            "backend_id"       = "${be_id}",
            "format"           = "csv",
            "column_separator" = ","
        );
    """

    // ============ 5. writer_class has to name a plugin factory ============
    // A Java class name is what this property held before plugins were isolated, so the first of
    // these is exactly what an unmigrated statement looks like. All three must come back as a
    // readable argument error rather than a crash or a hang.

    test {
        sql """
            INSERT INTO local(
                "file_path"    = "${basePath}/rejected",
                "backend_id"   = "${be_id}",
                "format"       = "csv",
                "writer_class" = "org.apache.doris.writer.LocalFileJniWriter"
            ) SELECT * FROM test_local_tvf_jni_src;
        """
        exception "writer_class must name a plugin factory"
    }

    test {
        sql """
            INSERT INTO local(
                "file_path"    = "${basePath}/rejected",
                "backend_id"   = "${be_id}",
                "format"       = "csv",
                "writer_class" = ":local-file"
            ) SELECT * FROM test_local_tvf_jni_src;
        """
        exception "writer_class must name a plugin factory"
    }

    test {
        sql """
            INSERT INTO local(
                "file_path"    = "${basePath}/rejected",
                "backend_id"   = "${be_id}",
                "format"       = "csv",
                "writer_class" = "java-writer:"
            ) SELECT * FROM test_local_tvf_jni_src;
        """
        exception "writer_class must name a plugin factory"
    }

    // ============ 6. The factory and the plugin both have to exist ============
    // Well-formed but wrong: these two reach the plugin registry, and each error should say which
    // of the two halves was not found.

    test {
        sql """
            INSERT INTO local(
                "file_path"    = "${basePath}/rejected",
                "backend_id"   = "${be_id}",
                "format"       = "csv",
                "writer_class" = "java-writer:no-such-factory"
            ) SELECT * FROM test_local_tvf_jni_src;
        """
        exception "has no factory named 'no-such-factory'"
    }

    test {
        sql """
            INSERT INTO local(
                "file_path"    = "${basePath}/rejected",
                "backend_id"   = "${be_id}",
                "format"       = "csv",
                "writer_class" = "no-such-plugin:local-file"
            ) SELECT * FROM test_local_tvf_jni_src;
        """
        exception "Java plugin 'no-such-plugin' is not deployed"
    }

    // ============ 7. The named factory has to produce a WRITER ============
    // Well-formed, and both halves exist - but "jdbc:reader" is a SCANNER factory. Without the kind
    // check in PluginRegistry::create_writer this reaches the point where JniWriter's method ids are
    // called on a JniScanner instance; those methods are final, so HotSpot dispatches them
    // non-virtually against a foreign receiver, which is undefined behaviour rather than an error.
    // The other negatives above all stop earlier - at parsing, or at "no such name" - so this is the
    // only one that exercises it.
    test {
        sql """
            INSERT INTO local(
                "file_path"    = "${basePath}/rejected",
                "backend_id"   = "${be_id}",
                "format"       = "csv",
                "writer_class" = "jdbc:reader"
            ) SELECT * FROM test_local_tvf_jni_src;
        """
        exception "does not produce a writer"
    }
    // No cleanup here on purpose: the directory is wiped at the start of the suite instead, so the
    // files a failing run wrote are still there to look at.
}
