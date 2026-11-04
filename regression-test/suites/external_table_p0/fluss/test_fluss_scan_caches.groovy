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

// Compare repeated cached reads with live metadata and a Fluss-only read of the same fixtures.
suite("test_fluss_scan_caches", "p0,external") {
    if (!context.config.otherConfigs.get("enableFlussTest")?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("fluss_coordinator_port")
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    for (String catalog : ["test_fluss_scan_cached", "test_fluss_scan_live"]) {
        String ttl = catalog.endsWith("cached") ? "600" : "0"
        sql """drop catalog if exists ${catalog}"""
        sql """
            create catalog ${catalog} properties (
                "type" = "fluss",
                "fluss.bootstrap.servers" = "${host}:${port}",
                "fluss.lake.paimon.s3.endpoint" = "http://${host}:${minioPort}",
                "fluss.lake.paimon.s3.access-key" = "minioadmin",
                "fluss.lake.paimon.s3.secret-key" = "minioadmin",
                "fluss.metadata.cache.ttl.sec" = "${ttl}",
                "fluss.partition.cache.ttl.sec" = "${ttl}"
            )
        """
    }
    sql """set enable_file_scanner_v2 = true"""
    try {
        for (String zone : ["UTC", "Asia/Shanghai", "America/New_York"]) {
            sql """set time_zone = '${zone}'"""
            for (String table : ["lake_pk", "lake_pk_multi", "lake_pk_comma", "lake_pk_ltz",
                                 "lake_pk_part", "lake_pk_part_int", "log_types"]) {
                String projection = table == "log_types"
                        ? "id, f_boolean, f_int, f_string, hex(f_binary), hex(f_bytes), f_date, f_timestamp, f_timestamp_ltz"
                        : "*"
                String cachedQuery = "select ${projection} from test_fluss_scan_cached.fluss_test.${table} order by 1, 2"
                String liveQuery = "select ${projection} from test_fluss_scan_live.fluss_test.${table} order by 1, 2"
                sql """set fluss_union_read_mode = 'disabled'"""
                def expected = sql(liveQuery)
                sql """set fluss_union_read_mode = 'auto'"""
                // Separate statements/scans share a BE tail entry; compare every row, not just counts.
                3.times {
                    assertEquals(expected, sql(cachedQuery), "cached union read differs: ${table}, ${zone}")
                    assertEquals(expected, sql(liveQuery), "live union read differs: ${table}, ${zone}")
                }
                sql """refresh table test_fluss_scan_cached.fluss_test.${table}"""
                assertEquals(expected, sql(cachedQuery), "refresh changed rows: ${table}, ${zone}")
            }
        }
        // A join builds several connector sessions in one statement and must keep both predicates.
        String joinQuery = """
            select a.id, a.name, b.name
            from test_fluss_scan_cached.fluss_test.lake_pk a
            join test_fluss_scan_cached.fluss_test.lake_pk b on a.id = b.id
            order by a.id, a.name, b.name
        """
        sql """set fluss_union_read_mode = 'disabled'"""
        def expectedJoin = sql(joinQuery)
        sql """set fluss_union_read_mode = 'auto'"""
        assertEquals(expectedJoin, sql(joinQuery))
    } finally {
        sql """set fluss_union_read_mode = ''"""
        sql """set time_zone = 'Asia/Shanghai'"""
    }
}
