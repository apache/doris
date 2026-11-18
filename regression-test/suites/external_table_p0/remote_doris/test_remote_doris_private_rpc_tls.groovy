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

suite("test_remote_doris_private_rpc_tls", "p0,external") {
    if (context.config.otherConfigs.get("enablePrivateRpcRemoteDorisTest") != "true") {
        return
    }

    // The fixture uses a TLS local cluster and an independent plaintext remote cluster.
    String host = context.config.otherConfigs.get("privateRpcRemoteDorisHost")
    String httpPort = context.config.otherConfigs.get("privateRpcRemoteDorisHttpPort")
    String thriftPort = context.config.otherConfigs.get("privateRpcRemoteDorisThriftPort")
    String arrowPort = context.config.otherConfigs.get("privateRpcRemoteDorisArrowPort")
    String queryPort = context.config.otherConfigs.get("privateRpcRemoteDorisQueryPort")
    String user = context.config.otherConfigs.get("privateRpcRemoteDorisUser")
    String password = context.config.otherConfigs.get("privateRpcRemoteDorisPassword")
    String remoteJdbc = "jdbc:mysql://${host}:${queryPort}/?useLocalSessionState=true"

    connect(user, password, remoteJdbc) {
        sql "CREATE DATABASE IF NOT EXISTS private_rpc_exchange_test"
        sql "DROP TABLE IF EXISTS private_rpc_exchange_test.remote_data"
        sql """CREATE TABLE private_rpc_exchange_test.remote_data (id INT, value INT)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("replication_num" = "1")"""
        sql "INSERT INTO private_rpc_exchange_test.remote_data VALUES (1, 100), (2, 200), (3, 300)"
    }
    sql "CREATE DATABASE IF NOT EXISTS private_rpc_exchange_test"
    sql "DROP TABLE IF EXISTS private_rpc_exchange_test.local_data"
    sql """CREATE TABLE private_rpc_exchange_test.local_data (id INT, value INT)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO private_rpc_exchange_test.local_data VALUES (1, 10), (2, 20), (4, 40)"
    sql "SET enable_sql_cache = false"
    sql "SET disable_join_reorder = true"

    ["private_rpc_remote_direct": "false", "private_rpc_remote_flight": "true"].each { catalog, flight ->
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
                "type" = "doris",
                "fe_http_hosts" = "http://${host}:${httpPort}",
                "fe_thrift_hosts" = "${host}:${thriftPort}",
                "fe_arrow_hosts" = "${host}:${arrowPort}",
                "user" = "${user}", "password" = "${password}",
                "use_arrow_flight" = "${flight}")"""
    }

    qt_remote_only """SELECT id, value FROM private_rpc_remote_flight.private_rpc_exchange_test.remote_data
                      ORDER BY id"""
    qt_local_only "SELECT id, value FROM private_rpc_exchange_test.local_data ORDER BY id"

    test {
        sql "SELECT id, value FROM private_rpc_remote_direct.private_rpc_exchange_test.remote_data ORDER BY id"
        exception "use_arrow_flight=true"
    }

    // Direct OLAP mode would require incompatible BE-to-BE exchanges in either direction.
    ["broadcast", "shuffle"].each { distribution ->
        test {
            sql """SELECT l.id, l.value, r.value FROM private_rpc_exchange_test.local_data l
                   JOIN [${distribution}] private_rpc_remote_direct.private_rpc_exchange_test.remote_data r
                   ON l.id = r.id ORDER BY l.id"""
            exception "use_arrow_flight=true"
        }
        test {
            sql """SELECT r.id, l.value, r.value
                   FROM private_rpc_remote_direct.private_rpc_exchange_test.remote_data r
                   JOIN [${distribution}] private_rpc_exchange_test.local_data l
                   ON r.id = l.id ORDER BY r.id"""
            exception "use_arrow_flight=true"
        }
    }

    // Arrow Flight fetches external results into local workers; all query exchanges keep local TLS.
    qt_flight_broadcast """SELECT l.id, l.value, r.value FROM private_rpc_exchange_test.local_data l
                          JOIN [broadcast] private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                          ON l.id = r.id ORDER BY l.id"""
    qt_flight_broadcast_reverse """SELECT r.id, l.value, r.value
                                  FROM private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                                  JOIN [broadcast] private_rpc_exchange_test.local_data l
                                  ON r.id = l.id ORDER BY r.id"""
    qt_flight_shuffle """SELECT l.id, l.value, r.value FROM private_rpc_exchange_test.local_data l
                        JOIN [shuffle] private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                        ON l.id = r.id ORDER BY l.id"""
    qt_flight_shuffle_reverse """SELECT r.id, l.value, r.value
                                FROM private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                                JOIN [shuffle] private_rpc_exchange_test.local_data l
                                ON r.id = l.id ORDER BY r.id"""
}
