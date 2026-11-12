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

suite("test_outfile_parquet_variant", "p0,external") {
    def uri = "s3://${getS3BucketName()}/outfile/parquet/variant/${UUID.randomUUID()}/"
    def pathStyle = context.config.otherConfigs.getOrDefault("s3UsePathStyle", "false")
    def storage = """
        "s3.endpoint" = "${getS3Endpoint()}",
        "s3.region" = "${getS3Region()}",
        "s3.access_key" = "${getS3AK()}",
        "s3.secret_key" = "${getS3SK()}",
        "use_path_style" = "${pathStyle}",
        "provider" = "${getS3Provider()}"
    """
    def file = """s3("uri" = "${uri}*.parquet", "format" = "parquet", ${storage})"""

    sql "DROP TABLE IF EXISTS outfile_parquet_variant_src"
    sql "DROP TABLE IF EXISTS outfile_parquet_variant_dst"
    sql """
        CREATE TABLE outfile_parquet_variant_src (id INT NOT NULL, v VARIANT NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "CREATE TABLE outfile_parquet_variant_dst LIKE outfile_parquet_variant_src"
    sql """
        INSERT INTO outfile_parquet_variant_src VALUES
            (1, parse_to_variant('{"nested":{"n":17},"a":[true,null,"x"]}')),
            (2, parse_to_variant('[1,{"n":2}]')),
            (3, parse_to_variant('"true"')),
            (4, parse_to_variant('true')),
            (5, parse_to_variant('{}')),
            (6, NULL),
            (7, parse_to_variant('"hello"'))
    """
    // Generate a Variant null after the scan. The table stores a root Variant null as an empty object.
    def projection = "id, IF(id = 5, parse_to_variant('null'), v) AS v"
    sql """
        SELECT ${projection} FROM outfile_parquet_variant_src
        INTO OUTFILE "${uri}part_" FORMAT AS PARQUET PROPERTIES (${storage})
    """

    def fileColumn = sql("DESC FUNCTION ${file}").find { it[0] == "v" }
    assertTrue(fileColumn[1].toLowerCase(Locale.ROOT).startsWith("variant"), fileColumn.toString())

    def readRows = { relation ->
        sql """SELECT id, CAST(v AS STRING), variant_type(v), v IS NULL,
                      CAST(v['nested']['n'] AS INT)
               FROM ${relation} ORDER BY id"""
    }
    assertEquals(readRows("(SELECT ${projection} FROM outfile_parquet_variant_src) t"), readRows(file))
    sql "INSERT INTO outfile_parquet_variant_dst SELECT id, v FROM ${file}"
    assertEquals(readRows("outfile_parquet_variant_src"), readRows("outfile_parquet_variant_dst"))

    // A source filter adds another consumer of v; it must still see the typed source slot.
    for (boolean filterNulls : [false, true]) {
        sql "TRUNCATE TABLE outfile_parquet_variant_dst"
        def label = "parquet_variant_" + UUID.randomUUID().toString().replace("-", "")
        def preceding = filterNulls ? "PRECEDING FILTER v IS NOT NULL" : ""
        sql """
            LOAD LABEL ${label} (
                DATA INFILE("${uri}part_*.parquet")
                INTO TABLE outfile_parquet_variant_dst FORMAT AS "parquet" (id, v)
                ${preceding}
            ) WITH S3 (${storage})
        """
        waitForBrokerLoadDone(label)
        def job = sql("SHOW LOAD WHERE LABEL = '${label}'")[0]
        assertEquals("FINISHED", job[2], job.toString())
        def expected = filterNulls
                ? "(SELECT * FROM outfile_parquet_variant_src WHERE v IS NOT NULL) t"
                : "outfile_parquet_variant_src"
        assertEquals(readRows(expected), readRows("outfile_parquet_variant_dst"))
    }
}
