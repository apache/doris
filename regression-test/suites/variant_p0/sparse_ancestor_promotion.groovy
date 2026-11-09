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

suite("sparse_ancestor_promotion", "p0") {
    sql "SET default_variant_enable_doc_mode = false"
    def backendIPs = [:]
    def backendHttpPorts = [:]
    getBackendIpHttpPort(backendIPs, backendHttpPorts)
    def checkFullStatus = { String tableName ->
        // Waiting for completion alone also accepts a failed compaction with intact old
        // rowsets. Record its status so unchanged query results cannot hide that failure.
        for (def tablet : sql_return_maparray("SHOW TABLETS FROM ${tableName}")) {
            def (code, out, err) = be_show_tablet_status(
                backendIPs["${tablet.BackendId}"], backendHttpPorts["${tablet.BackendId}"], tablet.TabletId)
            if (code != 0) {
                throw new Exception("Unable to read compaction status: ${err}")
            }
            def status = parseJson(out)["last full status"].toString().replace("'", "''")
            qt_full_status "SELECT '${status}'"
        }
    }
    for (def storageFormat : ["V2", "V3"]) {
        for (def shardCount : [1, 3]) {
            for (def keyModel : ["DUPLICATE", "UNIQUE"]) {
                def tableName = "sparse_ancestor_promotion_${storageFormat}_${shardCount}_${keyModel}"
                def mowProperty = keyModel == "UNIQUE" ? ', "enable_unique_key_merge_on_write"="true"' : ''
                sql "DROP TABLE IF EXISTS ${tableName}"
                sql """
                    CREATE TABLE ${tableName} (
                        id INT,
                        v VARIANT<PROPERTIES(
                            "variant_max_subcolumns_count"="1",
                            "variant_enable_nested_group"="false",
                            "variant_sparse_hash_shard_count"="${shardCount}"
                        )>
                    ) ${keyModel} KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                               "storage_format"="${storageFormat}"${mowProperty})
                """
                // Each rowset materializes hot_i (5 rows), leaving a (2) and a.b (1)
                // sparse. Across rowsets a (6) wins over every hot_i (5) and is promoted.
                // Its physical count must stay 6: a.b must not become another non-null a.
                for (int batch = 0; batch < 3; ++batch) {
                    int id = batch * 5
                    int value = batch * 100
                    sql """INSERT INTO ${tableName} VALUES
                        (${id + 1}, parse_to_variant('{"a":${value + 1},"hot_${batch}":1}')),
                        (${id + 2}, parse_to_variant('{"a":${value + 2},"hot_${batch}":2}')),
                        (${id + 3}, parse_to_variant('{"hot_${batch}":3}')),
                        (${id + 4}, parse_to_variant('{"hot_${batch}":4}')),
                        (${id + 5}, parse_to_variant('{"a":{"b":${value + 5}},"hot_${batch}":5}'))
                    """
                }
                def projection = """SELECT id, CAST(v['a'] AS STRING), v['a']['b'],
                    CAST(v AS STRING) FROM ${tableName} ORDER BY id"""
                qt_before projection
                trigger_and_wait_compaction(tableName, "full", 1800)
                checkFullStatus(tableName)
                qt_after_first projection

                // Add another source with a sparse ancestor and compact against the new
                // materialized ancestor, exercising both sides of the promotion boundary.
                sql """INSERT INTO ${tableName} VALUES
                    (16, parse_to_variant('{"a":301,"hot_3":1}')),
                    (17, parse_to_variant('{"a":302,"hot_3":2}')),
                    (18, parse_to_variant('{"hot_3":3}')),
                    (19, parse_to_variant('{"hot_3":4}')),
                    (20, parse_to_variant('{"a":{"b":305},"hot_3":5}'))
                """
                qt_before_second projection
                trigger_and_wait_compaction(tableName, "full", 1800)
                checkFullStatus(tableName)
                qt_after_second projection
            }
        }
    }
}
