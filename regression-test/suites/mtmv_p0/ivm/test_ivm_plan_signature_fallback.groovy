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

suite("test_ivm_plan_signature_fallback", "nonConcurrent") {
    def tableName = "test_ivm_plan_signature_fallback_t"
    def mvName = "test_ivm_plan_signature_fallback_mv"
    def signatureSaltDebugPoint = "IvmPlanSignatureGenerator.generate.signature_salt"

    GetDebugPoint().disableDebugPointForAllFEs(signatureSaltDebugPoint)

    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists ${tableName};"""

    sql """
        CREATE TABLE ${tableName} (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ${tableName} VALUES (1, 10), (2, 20);"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT k1, v1 FROM ${tableName};
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)

    sql """INSERT INTO ${tableName} VALUES (3, 30);"""

    try {
        GetDebugPoint().enableDebugPointForAllFEs(signatureSaltDebugPoint, [value: "plan_changed"])
        sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
        waitingMTMVTaskFinishedByMvName(mvName)
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(signatureSaltDebugPoint)
    }

    def refreshMode = sql """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    assertEquals("COMPLETE", refreshMode[0][0].toString())

    def rows = sql """SELECT k1, v1 FROM ${mvName} ORDER BY k1"""
    assertEquals("[[1, 10], [2, 20], [3, 30]]", rows.toString())
}
