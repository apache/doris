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

suite("test_orc_lazy_mat_profile", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    String catalogName = "test_orc_lazy_mat_profile"
    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """CREATE CATALOG ${catalogName} PROPERTIES (
        'type' = 'hms',
        'hadoop.username' = 'hadoop',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
    )"""
    sql """USE ${catalogName}.global_lazy_mat_db"""

    def previousSettings = sql """SELECT @@enable_orc_filter_by_min_max,
        @@enable_orc_lazy_materialization, @@enable_condition_cache, @@enable_sql_cache"""
    try {
        sql "SET enable_condition_cache=false"
        sql "SET enable_sql_cache=false"
        sql "SET enable_orc_filter_by_min_max=false"
        sql "SET enable_orc_lazy_materialization=false"

        def referenceRows = sql """SELECT id, name, value, active, score, file_id
            FROM orc_topn_lazy_mat_table WHERE file_id = 1 ORDER BY id"""
        assertFalse(referenceRows.isEmpty(), "The ORC fixture must contain rows")

        // Empty scanners can omit merged reader counters. Derive expected full rows from an
        // unfiltered eager read, independently of predicate pruning and lazy column reads.
        def queries = [
            [predicate: "id = 1", expected: referenceRows.findAll { it[0] == 1 }],
            [predicate: "id <= 2", expected: referenceRows.findAll { it[0] <= 2 }],
            [predicate: "id <= 3", expected: referenceRows.findAll { it[0] <= 3 }],
            [predicate: "id < 0", expected: referenceRows.findAll { it[0] < 0 }],
            [predicate: "id >= 7", expected: referenceRows.findAll { it[0] >= 7 }],
            [predicate: "id >= 3 AND id <= 7", expected: referenceRows.findAll { it[0] >= 3 && it[0] <= 7 }]
        ]

        for (boolean minMax : [true, false]) {
            for (boolean lazyMaterialization : [true, false]) {
                sql "SET enable_orc_filter_by_min_max=${minMax}"
                sql "SET enable_orc_lazy_materialization=${lazyMaterialization}"
                for (def query : queries) {
                    def actualRows = sql """SELECT id, name, value, active, score, file_id
                        FROM orc_topn_lazy_mat_table
                        WHERE file_id = 1 AND ${query.predicate} ORDER BY id"""
                    assertEquals(query.expected, actualRows,
                            "min/max=${minMax}, lazy=${lazyMaterialization}, predicate=${query.predicate}")
                }
            }
        }
    } finally {
        sql "SET enable_orc_filter_by_min_max=${previousSettings[0][0]}"
        sql "SET enable_orc_lazy_materialization=${previousSettings[0][1]}"
        sql "SET enable_condition_cache=${previousSettings[0][2]}"
        sql "SET enable_sql_cache=${previousSettings[0][3]}"
        sql """DROP CATALOG ${catalogName}"""
    }
}
