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

// Exports a Variant column as the Parquet VARIANT logical type and reads it back through the
// local() table valued function, INSERT ... SELECT and EXPORT.
suite("test_outfile_parquet_variant", "p0") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Please set enable_outfile_to_local to true to run test_outfile_parquet_variant")
        return
    }
    List<List<Object>> backends = sql "show backends"
    assertTrue(backends.size() > 0)
    def beId = backends[0][0]
    def uuid = UUID.randomUUID().toString().replace("-", "")
    // OUTFILE/EXPORT write file prefixes straight under /tmp of the BE (the directory always
    // exists), and local() resolves paths under user_files_secure_path, which the regression
    // cluster sets to "/".
    def filePrefix = "test_outfile_parquet_variant_${uuid}"
    def outDir = "/tmp"
    def tvfDir = "tmp"

    def localTvf = { String prefix ->
        """local("file_path" = "${tvfDir}/${filePrefix}_${prefix}_*", "backend_id" = "${beId}", "format" = "parquet")"""
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

    sql "DROP TABLE IF EXISTS test_outfile_parquet_variant_src"
    sql """
        CREATE TABLE test_outfile_parquet_variant_src (
            id INT NOT NULL,
            v VARIANT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_outfile_parquet_variant_src VALUES
        (1, '{"name":"alice","age":30,"score":1.5,"ok":true,"tags":["a","b"],"nested":{"city":"hz","zip":310000}}'),
        (2, '{"name":"bob","age":-7,"big":1234567890123,"ok":false,"tags":[],"nested":{}}'),
        (3, '[1,"two",3.0,null,{"k":"v"}]'),
        (4, '"plain string"'),
        (5, '42'),
        (6, '3.25'),
        (7, 'true'),
        (8, 'null'),
        (9, NULL)
    """

    // OUTFILE writes the Variant column as the Parquet VARIANT logical type.
    def res = sql """
        SELECT id, v FROM test_outfile_parquet_variant_src
        INTO OUTFILE "file://${outDir}/${filePrefix}_out_"
        FORMAT AS parquet
    """
    assertEquals(1, res.size())
    assertEquals("9", res[0][1].toString())
    qt_desc """desc function ${localTvf('out')}"""
    qt_tvf """select id, v from ${localTvf('out')} order by id"""
    qt_tvf_subcolumn """
        select id, cast(v['name'] as string) as name, cast(v['nested']['zip'] as int) as zip,
               variant_type(v) as type
        from ${localTvf('out')} order by id
    """

    sql "DROP TABLE IF EXISTS test_outfile_parquet_variant_reload"
    sql """
        CREATE TABLE test_outfile_parquet_variant_reload (
            id INT NOT NULL,
            v VARIANT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_outfile_parquet_variant_reload SELECT id, v FROM ${localTvf('out')}"""
    qt_reload """
        select id, v, cast(v['name'] as string) as name, cast(v['nested']['city'] as string) as city
        from test_outfile_parquet_variant_reload order by id
    """

    // EXPORT goes through the same Parquet writer.
    def label = "test_outfile_parquet_variant_${uuid}"
    sql """
        EXPORT TABLE test_outfile_parquet_variant_src TO "file://${outDir}/${filePrefix}_exp_"
        PROPERTIES ("label" = "${label}", "format" = "parquet")
    """
    waitExport(label)
    qt_desc_export """desc function ${localTvf('exp')}"""
    qt_tvf_export """select id, v from ${localTvf('exp')} order by id"""
}
