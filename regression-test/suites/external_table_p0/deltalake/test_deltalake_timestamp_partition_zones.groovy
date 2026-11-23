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

suite("test_deltalake_timestamp_partition_zones", "p0,external,deltalake,external_docker") {
    def config = context.config.otherConfigs
    def catalogs = [:]
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeTest"))) {
        String warehouse = config.get("deltalakeWarehouse")
        String storage = ""
        if (warehouse.startsWith("s3")) {
            storage = """, 's3.endpoint'='http://${config.get('externalEnvIp')}:${config.get('deltalakeMinioPort')}',
                's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'"""
        }
        catalogs.fs = "'deltalake.catalog.type'='filesystem', 'warehouse'='${warehouse}'${storage}"
    }
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeUcTest"))) {
        catalogs.uc = """'deltalake.catalog.type'='uc', 'deltalake.uc.uri'='${config.get('deltalakeUcUri')}',
            'deltalake.uc.catalog'='doris_delta'"""
    }

    def profiles = new ProfileAction(context)
    catalogs.each { kind, properties ->
        String catalog = "test_deltalake_timestamp_partition_zones_${kind}"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties ('type'='deltalake', ${properties})"""
        String table = "${catalog}.delta_reader_edges.timestamp_partition_zones"
        sql "set enable_sql_cache=false"
        sql "set enable_profile=true"
        sql "set num_files_in_batch_mode=1"
        String fixed = "cast('2024-01-15 20:00:00+00:00' as timestamptz(6))"
        String localNoon = "cast('2024-01-15 12:00:00' as timestamptz(6))"
        String wallNoon = "cast('2024-01-15 12:00:00' as datetimev2(6))"
        for (boolean batch : [false, true]) {
            sql "set enable_external_table_batch_mode=${batch}"
            // On the same snapshot, Spark interprets the legacy noon as 12Z, 20Z and 04Z respectively.
            [UTC: "12", "America/Los_Angeles": "20", "Asia/Shanghai": "04"].each { zone, legacyHour ->
                sql "set time_zone='${zone}'"
                String tag = "${kind}_${batch}_${zone}"
                String legacy = "cast('2024-01-15 ${legacyHour}:00:00+00:00' as timestamptz(6))"
                // IDs 2 and 3 explicitly encode the same instant with Z and -08:00.
                quickTest("${tag}_projection", """select 1 as id, ${legacy} as p, ${wallNoon} as wall
                        union all select 2, ${fixed}, ${wallNoon}
                        union all select 3, ${fixed}, cast('2024-01-15 13:00:00' as datetimev2(6)) order by id""",
                        "select id, p, wall from ${table} order by id")
                def checkFiles = { String label, String filter, List<Integer> ids ->
                    String token = UUID.randomUUID().toString()
                    quickTest("${tag}_${label}",
                            """select number from numbers('number'='4') where number in (${ids.join(',')})
                               order by number""",
                            "select /* ${token} */ id from ${table} where ${filter} order by id")
                    String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
                    assertTrue(profile.readLines()*.trim().contains("- SelectedFiles: " + ids.size()), profile)
                }
                boolean sameInstant = legacyHour == "20"
                checkFiles("fixed", "p = ${fixed}", sameInstant ? [1, 2, 3] : [2, 3])
                checkFiles("local_noon", "p = ${localNoon}", sameInstant ? [1, 2, 3] : [1])
                checkFiles("ntz", "wall = ${wallNoon}", [1, 2])
            }
        }
    }
}
