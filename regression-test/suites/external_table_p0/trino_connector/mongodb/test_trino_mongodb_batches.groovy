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

suite("test_trino_mongodb_batches", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableMongoDBTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mongodb_port")
    String user = context.config.otherConfigs.get("mongodb_user")
    String password = context.config.otherConfigs.get("mongodb_password")
    String encodedUser = java.net.URLEncoder.encode(user, "UTF-8").replace("+", "%20")
    String encodedPassword = java.net.URLEncoder.encode(password, "UTF-8").replace("+", "%20")
    sql "DROP CATALOG IF EXISTS mongodb_batches_test"
    sql """CREATE CATALOG mongodb_batches_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://${encodedUser}:${encodedPassword}@${host}:${port}/?authSource=admin",
        "trino.mongodb.cursor-batch-size" = "127"
    )"""
    sql "SWITCH mongodb_batches_test"
    sql "USE doris_mongodb_test"

    order_qt_multi_page_count """SELECT count(*), count(owner), min(seq), max(seq), sum(seq) FROM large_records"""

    order_qt_multi_page_groups """SELECT bucket, count(*), min(seq), max(seq), count(owner) FROM large_records GROUP BY bucket"""

    order_qt_batch_boundaries """SELECT seq, from_binary(_id), from_binary(owner), payload FROM large_records WHERE seq IN (0,126,127,128,1023,1024,1025,2047,2048,2080)"""

    order_qt_multi_page_bytes """SELECT count(*) FROM large_records WHERE owner IS NOT NULL AND from_binary(owner) = '00FF807F0100020304050607'"""

    order_qt_multi_page_null """SELECT count(*) FROM large_records WHERE owner IS NULL"""

    qt_multi_page_offset """SELECT seq, from_binary(_id) FROM large_records ORDER BY seq LIMIT 5 OFFSET 1022"""

    order_qt_multi_page_aggregate """SELECT sum(length(cast(_id AS STRING))), count(DISTINCT payload) FROM large_records"""

    order_qt_multi_page_empty """SELECT seq FROM large_records WHERE seq >= 2081"""

    // Repeated early termination must not poison the next cursor or scan.
    for (int i = 0; i < 3; i++) {
        "qt_repeated_limit_${i}" "SELECT seq, from_binary(_id) FROM large_records ORDER BY seq LIMIT 3"
    }
    order_qt_count_after_limits "SELECT count(*) FROM large_records"
}
