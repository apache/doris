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

suite("test_mongodb_snapshot", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoSnapshotTest"))) {
        return
    }
    def original = sql("SELECT @@mongodb_read_concern,@@mongodb_read_preference,@@time_zone,@@batch_size")[0]
    String uri = context.config.otherConfigs.get("mongodb_snapshot_uri")
    def bankColumns = ["seq", "amount", "epoch"].collect {
        [name: it, path: [it], bson_types: ["long"], doris_type: "BIGINT"]
    }
    def manifest = [:]
    for (String name : ["left_records", "right_records", "record_view"]) {
        manifest["doris_mongodb_snapshot.${name}"] = [columns: bankColumns]
    }
    manifest["doris_mongodb_snapshot.measurements"] = [columns: [
        [name: "seq", path: ["seq"], bson_types: ["long"], doris_type: "BIGINT"],
        [name: "amount", path: ["amount"], bson_types: ["long"], doris_type: "BIGINT"],
        [name: "observed", path: ["observed"], bson_types: ["date"], doris_type: "DATETIME(3)"]]]
    manifest["doris_mongodb_snapshot.precision_records"] = [columns: [
        [name: "seq", path: ["seq"], bson_types: ["long"], doris_type: "BIGINT"],
        [name: "observed", path: ["observed"], bson_types: ["date"], doris_type: "DATETIME(3)"],
        [name: "precise_time", path: ["precise_time"], bson_types: ["date"], doris_type: "DATETIME(6)"],
        [name: "amount", path: ["amount"], bson_types: ["decimal"], doris_type: "DECIMAL(38,4)"]]]
    def create = { String catalog, String concern ->
        sql "DROP CATALOG IF EXISTS ${catalog}"
        def properties = ["type": "mongodb", "mongodb.uri": uri,
            "mongodb.databases": "doris_mongodb_snapshot", "mongodb.schema.mode": "explicit",
            "mongodb.schema": groovy.json.JsonOutput.toJson(manifest), "mongodb.read_concern": concern]
        String values = properties.collect { key, value ->
            "'${key}'='${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'"
        }.join(",")
        sql "CREATE CATALOG ${catalog} PROPERTIES (${values})"
    }
    String catalog = "mongodb_snapshot_regression"
    String ordinary = "mongodb_snapshot_ordinary"
    String namespace = "${catalog}.doris_mongodb_snapshot"
    try {
        create(catalog, "snapshot")
        create(ordinary, "majority")
        sql "SET batch_size=1024"
        sql "SET mongodb_read_preference='primary'"
        sql "SET mongodb_read_concern='inherit'"
        qt_catalog_snapshot "SELECT count(*),sum(seq) FROM ${namespace}.left_records"
        qt_cross_scan """SELECT l.n,r.n,l.amount+r.amount FROM
            (SELECT count(*) n,sum(amount) amount FROM ${namespace}.left_records) l CROSS JOIN
            (SELECT count(*) n,sum(amount) amount FROM ${namespace}.right_records) r"""
        for (String mode : ["primary", "secondary", "secondaryPreferred", "nearest"]) {
            sql "SET mongodb_read_preference='${mode}'"
            "qt_snapshot_route_${mode}" "SELECT count(*),sum(seq) FROM ${namespace}.left_records"
        }
        sql "SET mongodb_read_preference='primary'"
        for (String zone : ["UTC", "+08:00", "America/Los_Angeles", "Asia/Kathmandu"]) {
            sql "SET time_zone='${zone}'"
            def baseline = sql """SELECT /*+ SET_VAR(mongodb_read_concern='majority') */
                * FROM ${namespace}.precision_records ORDER BY seq"""
            for (boolean pushdown : [false, true]) {
                def observed = sql """SELECT /*+ SET_VAR(enable_mongodb_predicate_pushdown=${pushdown},
                    enable_mongodb_projection_pushdown=${pushdown}) */ * FROM ${namespace}.precision_records
                    WHERE seq > 0 ORDER BY seq"""
                assertEquals(baseline, observed)
            }
        }
        qt_exact_precision "SELECT * FROM ${namespace}.precision_records ORDER BY seq"
        def measurements = sql """SELECT /*+ SET_VAR(mongodb_read_concern='majority') */
            seq,amount,observed FROM ${namespace}.measurements ORDER BY seq,amount,observed"""
        assertEquals(measurements, sql("SELECT seq,amount,observed FROM ${namespace}.measurements "
            + "ORDER BY seq,amount,observed"))
        qt_timeseries "SELECT count(*),sum(seq),count(DISTINCT observed) FROM ${namespace}.measurements"
        test {
            sql "SELECT count(*) FROM ${namespace}.record_view"
            exception "standard views"
        }
        test {
            sql """SELECT l.seq FROM ${namespace}.left_records l JOIN
                ${ordinary}.doris_mongodb_snapshot.right_records r ON l.seq=r.seq"""
            exception "snapshot and non-snapshot"
        }
        sql "SET mongodb_read_concern='majority'"
        qt_statement_snapshot """SELECT /*+ SET_VAR(mongodb_read_concern='snapshot') */
            count(*),sum(seq) FROM ${namespace}.left_records"""
        qt_statement_restored "SELECT @@mongodb_read_concern"
        String plan = sql("EXPLAIN SELECT /*+ SET_VAR(mongodb_read_concern='snapshot') */ "
            + "count(*) FROM ${namespace}.left_records").toString()
        assertTrue(plan.contains("snapshot"))
        String token = "mongodb_snapshot_" + UUID.randomUUID().toString().replace("-", "")
        sql """SELECT /*+ SET_VAR(enable_profile=true,profile_level=2,mongodb_read_concern='snapshot') */
            /* ${token} */ l.n+r.n FROM
            (SELECT count(*) n FROM ${namespace}.left_records) l CROSS JOIN
            (SELECT count(*) n FROM ${namespace}.right_records) r"""
        String text = new org.apache.doris.regression.action.ProfileAction(context)
            .getProfileBySql(token, ["MongoSnapshotTimestamp"])
        def pins = (text =~ /MongoSnapshotTimestamp:\s*(\d+:\d+)/).collect { it[1] }
        assertTrue(pins.size() >= 2)
        assertEquals(1, pins.unique().size())
    } finally {
        sql "SET mongodb_read_concern='${original[0]}'"
        sql "SET mongodb_read_preference='${original[1]}'"
        sql "SET time_zone='${original[2]}'"
        sql "SET batch_size=${original[3]}"
    }
}
