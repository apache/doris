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

import org.apache.doris.regression.action.ProfileAction

suite("test_deltalake_reader_edges", "p0,external,deltalake,external_docker") {
    def config = context.config.otherConfigs
    def catalogs = []
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeTest"))) {
        String catalog = "test_deltalake_reader_edges_fs"
        String warehouse = config.get("deltalakeWarehouse")
        String storage = ""
        if (warehouse.startsWith("s3")) {
            storage = """, 's3.endpoint'='http://${config.get('externalEnvIp')}:${config.get('deltalakeMinioPort')}',
                's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'"""
        }
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='filesystem', 'warehouse'='${warehouse}'${storage})"""
        catalogs.add(catalog)
    }
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeUcTest"))) {
        String catalog = "test_deltalake_reader_edges_uc"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='uc',
            'deltalake.uc.uri'='${config.get('deltalakeUcUri')}', 'deltalake.uc.catalog'='doris_delta')"""
        catalogs.add(catalog)
    }
    if (catalogs.isEmpty()) {
        return
    }

    sql "set time_zone='UTC'"
    sql "set enable_sql_cache=false"
    sql "set num_files_in_batch_mode=1"
    def profiles = new ProfileAction(context)
    for (String catalog : catalogs) {
        String db = "${catalog}.delta_reader_edges"
        for (boolean batch : [false, true]) {
            sql "set enable_external_table_batch_mode=${batch}"
            def checkCase = { String label, Closure check ->
                String tag = "${catalog}_${batch}_${label}"
                lazyCheck {
                    try {
                        check(tag)
                    } catch (Exception e) {
                        logger.warn("Delta reader check ${tag} failed: ${e.message}")
                        throw e
                    }
                }
            }
            def compare = { String label, String query, String expected ->
                checkCase(label) { tag -> quickTest(tag, expected, query) }
            }

            // ID mapping must distinguish two same-named Parquet fields with different IDs.
            compare("matching_id", "select logical_x from ${db}.id_correct", "select 701")
            compare("wrong_id", "select logical_x from ${db}.id_old_id", "select cast(null as bigint)")
            compare("name_mapping", "select logical_x from ${db}.name_untagged", "select 705")
            checkCase("missing_id") { tag ->
                // For a file without field IDs the protocol permits NULL or refusal to read the file.
                try {
                    quickTest(tag, "select cast(null as bigint)",
                            "select logical_x from ${db}.id_untagged")
                } catch (java.sql.SQLException e) {
                    boolean mentionsIds = (e.message =~ /(?i)(?:field|column)[ _-]?ids?/).find()
                    boolean missing = (e.message =~ /(?i)missing|absent|without|no |not |lack|requir|doesn't/).find()
                    if (!(mentionsIds && missing)) {
                        throw e
                    }
                }
            }

            String bothIds = "select 1 as id union all select 2 order by id"
            compare("widen_partition", "select id, p from ${db}.date_widen order by id",
                    """select 1 as id, cast('2024-02-29 00:00:00' as datetimev2(6))
                       union all select 2, cast('2024-03-01 12:34:56' as datetimev2(6)) order by id""")
            compare("widen_id_only", "select id from ${db}.date_widen order by id", bothIds)
            String midnight = "cast('2024-02-29 00:00:00' as datetimev2(6))"
            compare("widen_midnight", "select id from ${db}.date_widen where p = ${midnight} order by id",
                    "select 1")
            compare("widen_range", """select id from ${db}.date_widen where p > ${midnight}
                    and p <= cast('2024-03-01 12:34:56' as datetimev2(6)) order by id""", "select 2")
            compare("widen_microsecond", """select count(*) from ${db}.date_widen
                    where p = cast('2024-02-29 00:00:00.000001' as datetimev2(6))""", "select 0")

            // Spark reads an explicit empty STRING partition as '' and JSON null as SQL NULL (delta-io/delta#7091).
            compare("string_values", "select id, p from ${db}.empty_string order by id",
                    """select 1 as id, cast('' as string) union all select 2, null
                       union all select 3, 'known' order by id""")
            compare("string_is_null", "select id from ${db}.empty_string where p is null order by id", "select 2")
            compare("string_eq_empty", "select id from ${db}.empty_string where p = '' order by id", "select 1")
            compare("string_is_not_null", "select id from ${db}.empty_string where p is not null order by id",
                    "select 1 as id union all select 3 order by id")
            String dates = "select 1 as id, cast(null as date) union all select 2, date '2024-02-29' order by id"
            compare("json_null_date", "select id, p from ${db}.date_control order by id", dates)
            compare("empty_date", "select id, p from ${db}.empty_date order by id", dates)

            // The FLOAT key gives ordinary queries native partition planning. Each ID is a separate file.
            String typed = "${db}.empty_typed_partitions"
            String instant = "cast('2024-06-01 00:00:00+00:00' as timestamptz(6))"
            compare("typed_empty_projection", "select id, p_int, p_bool, p_ts from ${typed} order by id",
                    """select 1 as id, cast(null as int), cast(null as boolean), cast(null as timestamptz(6))
                       union all select 2, null, null, null union all select 3, 7, true, ${instant} order by id""")
            def checkFiles = { String label, String predicate, String expected, int selectedFiles ->
                checkCase(label) { tag ->
                    String token = UUID.randomUUID().toString()
                    quickTest(tag, expected, """select /*+ SET_VAR(enable_profile=true) */ /* ${token} */ id
                            from ${typed} where ${predicate} order by id""")
                    String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
                    assertTrue(profile.readLines()*.trim().contains("- SelectedFiles: " + selectedFiles), profile)
                }
            }
            [p_int: "7", p_bool: "true", p_ts: instant].each { key, value ->
                checkFiles("typed_empty_${key}_null", "${key} is null", bothIds, 2)
                checkFiles("typed_empty_${key}_eq", "${key} = ${value}", "select 3", 1)
            }
            checkFiles("typed_empty_bool_false", "p_bool = false", "select 1 where false", 0)
            checkFiles("typed_empty_partition_or", "p_int = 7 or bucket = cast('1.0' as float)", "select 3", 1)
            checkFiles("typed_empty_data_or", "p_int is null or id = 3",
                    "select 1 as id union all select 2 union all select 3 order by id", 3)

            compare("plain_name", "select id, p from ${db}.plain order by id", "select 41, 'west'")
            compare("plain_filter", "select id from ${db}.plain where p='west' order by id", "select 41")
        }
    }
}
