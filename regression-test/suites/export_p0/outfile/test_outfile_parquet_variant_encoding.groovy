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

// Exports a Variant V2 column with the Parquet VARIANT logical type
// ("parquet.variant_encoding" = "variant") and reads it back through the local()
// table valued function, INSERT ... SELECT and EXPORT.
suite("test_outfile_parquet_variant_encoding", "p0") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Please set enable_outfile_to_local to true to run test_outfile_parquet_variant_encoding")
        return
    }
    List<List<Object>> backends = sql "show backends"
    assertTrue(backends.size() > 0)
    def beId = backends[0][0]
    def uuid = UUID.randomUUID().toString().replace("-", "")
    // OUTFILE/EXPORT write file prefixes straight under /tmp of the BE (the directory always
    // exists), and local() resolves paths under user_files_secure_path, which the regression
    // cluster sets to "/".
    def filePrefix = "test_outfile_parquet_variant_encoding_${uuid}"
    def outDir = "/tmp"
    def tvfDir = "tmp"

    def localTvf = { String prefix ->
        """local("file_path" = "${tvfDir}/${filePrefix}_${prefix}_*", "backend_id" = "${beId}", "format" = "parquet")"""
    }
    def outfile = { String prefix, String properties ->
        def res = sql """
            SELECT id, v FROM test_outfile_parquet_variant_encoding_src
            INTO OUTFILE "file://${outDir}/${filePrefix}_${prefix}_"
            FORMAT AS parquet
            PROPERTIES (${properties})
        """
        assertEquals(1, res.size())
        assertEquals("9", res[0][1].toString())
    }
    def waitExport = { String label ->
        while (true) {
            def res = sql """ show export where label = "${label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                break
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(2000)
            }
        }
    }

    // The Parquet VARIANT logical type is written from the ColumnVariantV2 bytes and read back
    // without any JSON round trip, so the whole suite runs with Variant V2 execution.
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "DROP TABLE IF EXISTS test_outfile_parquet_variant_encoding_src"
        sql """
            CREATE TABLE test_outfile_parquet_variant_encoding_src (
                id INT NOT NULL,
                v VARIANT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO test_outfile_parquet_variant_encoding_src VALUES
            (1, parse_to_variant('{"name":"alice","age":30,"score":1.5,"ok":true,"tags":["a","b"],"nested":{"city":"hz","zip":310000}}')),
            (2, parse_to_variant('{"name":"bob","age":-7,"big":1234567890123,"ok":false,"tags":[],"nested":{}}')),
            (3, parse_to_variant('[1,"two",3.0,null,{"k":"v"}]')),
            (4, parse_to_variant('"plain string"')),
            (5, parse_to_variant('42')),
            (6, parse_to_variant('3.25')),
            (7, parse_to_variant('true')),
            (8, parse_to_variant('null')),
            (9, NULL)
        """

        outfile("variant", """"parquet.variant_encoding" = "variant\"""")
        qt_desc_variant """desc function ${localTvf('variant')}"""
        qt_tvf_variant """select id, v from ${localTvf('variant')} order by id"""
        qt_tvf_variant_subcolumn """
            select id, cast(v['name'] as string) as name, cast(v['nested']['zip'] as int) as zip,
                   variant_type(v) as type
            from ${localTvf('variant')} order by id
        """

        sql "DROP TABLE IF EXISTS test_outfile_parquet_variant_encoding_reload"
        sql """
            CREATE TABLE test_outfile_parquet_variant_encoding_reload (
                id INT NOT NULL,
                v VARIANT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """INSERT INTO test_outfile_parquet_variant_encoding_reload SELECT id, v FROM ${localTvf('variant')}"""
        qt_reload """
            select id, v, cast(v['name'] as string) as name, cast(v['nested']['city'] as string) as city
            from test_outfile_parquet_variant_encoding_reload order by id
        """

        // The default keeps the UTF-8 JSON representation.
        outfile("json", """"parquet.disable_dictionary" = "false\"""")
        qt_desc_json """desc function ${localTvf('json')}"""
        qt_tvf_json """select id, v from ${localTvf('json')} order by id"""

        // EXPORT forwards the property to its OUTFILE statements.
        def label = "test_outfile_parquet_variant_encoding_${uuid}"
        sql """
            EXPORT TABLE test_outfile_parquet_variant_encoding_src TO "file://${outDir}/${filePrefix}_exp_"
            PROPERTIES (
                "label" = "${label}",
                "format" = "parquet",
                "parquet.variant_encoding" = "variant"
            )
        """
        waitExport(label)
        qt_desc_export """desc function ${localTvf('exp')}"""
        qt_tvf_export """select id, v from ${localTvf('exp')} order by id"""

        test {
            sql """
                SELECT id, v FROM test_outfile_parquet_variant_encoding_src
                INTO OUTFILE "file://${outDir}/${filePrefix}_bad_"
                FORMAT AS parquet
                PROPERTIES ("parquet.variant_encoding" = "binary")
            """
            exception "should be json or variant"
        }
        test {
            sql """
                EXPORT TABLE test_outfile_parquet_variant_encoding_src TO "file://${outDir}/${filePrefix}_bad_"
                PROPERTIES ("format" = "parquet", "parquet.variant_encoding" = "binary")
            """
            exception "should be json or variant"
        }
    }
}
