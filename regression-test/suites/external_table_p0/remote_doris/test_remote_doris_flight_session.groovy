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

// A remote Doris scan opens an Arrow Flight SQL session on the remote frontend for the query the BE
// reads, and has to close it once the local query is over. A session left behind stays in the remote
// frontend's connection pool until wait_timeout and counts against the catalog user's
// max_user_connections there, so that user - MySQL clients included - is refused after a hundred
// scans. The remote frontend here is this one, and the catalog logs in as a user of its own, so the
// Flight sessions of that user in the processlist are exactly the ones this suite's scans opened.
suite("test_remote_doris_flight_session", "p0,external") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]
    String httpPort = frontends[0][3]
    String thriftPort = frontends[0][5]
    log.info("show frontends = ${frontends}, arrow: ${arrowPort}, http: ${httpPort}, thrift: ${thriftPort}")

    String user = "test_remote_doris_flight_session_user"
    String pwd = "C123_567p"
    String db = "test_remote_doris_flight_session_db"
    String table = "test_remote_doris_flight_session_t"
    String catalog = "test_remote_doris_flight_session_catalog"

    sql """DROP CATALOG IF EXISTS `${catalog}`"""
    sql """DROP USER IF EXISTS '${user}'@'%'"""
    sql """DROP DATABASE IF EXISTS `${db}`"""
    sql """CREATE DATABASE `${db}`"""
    sql """
        CREATE TABLE `${db}`.`${table}` (
          `id` int NOT NULL,
          `v` varchar(16) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """INSERT INTO `${db}`.`${table}` VALUES (1, 'a'), (2, 'b'), (3, 'c')"""

    // What the remote frontend asks of the catalog user: the Flight handshake takes any user, the
    // metadata REST calls need SHOW on the database, the query it runs needs SELECT on the table.
    sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${pwd}'"""
    sql """GRANT SELECT_PRIV ON internal.`${db}`.* TO '${user}'@'%'"""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${user}'@'%'"""
    }

    sql """
        CREATE CATALOG `${catalog}` PROPERTIES (
            'type' = 'doris',
            'fe_thrift_hosts' = '${host}:${thriftPort}',
            'fe_http_hosts' = 'http://${host}:${httpPort}',
            'fe_arrow_hosts' = '${host}:${arrowPort}',
            'user' = '${user}',
            'password' = '${pwd}',
            'use_arrow_flight' = 'true'
        );
    """

    def flightSessionsOfCatalogUser = { ->
        def rows = sql """
            SELECT COUNT(*) FROM information_schema.processlist
            WHERE User = '${user}' AND Protocol = 'ArrowFlightSQL'
        """
        return rows[0][0] as long
    }
    assertEquals(0L, flightSessionsOfCatalogUser())

    try {
        // Every scan opens one session on the remote frontend. It is closed when the local query's
        // coordinator closes, which happens before the result reaches the client, so none is left by
        // the time the next statement runs - however many scans in a row.
        for (int i = 0; i < 5; i++) {
            def rows = sql """SELECT id, v FROM `${catalog}`.`${db}`.`${table}` ORDER BY id"""
            assertEquals([[1, 'a'], [2, 'b'], [3, 'c']], rows)
            assertEquals(0L, flightSessionsOfCatalogUser())
        }

        // A plan no coordinator ever takes is released when the statement ends instead. INSERT
        // OVERWRITE plans the query once only to find the target partitions and discards that plan
        // (its scan opened a session on the remote frontend) before the insert plans again ...
        sql """DROP TABLE IF EXISTS `${db}`.`${table}_sink`"""
        sql """
            CREATE TABLE `${db}`.`${table}_sink` (
              `id` int NOT NULL,
              `v` varchar(16) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        sql """INSERT OVERWRITE TABLE `${db}`.`${table}_sink` SELECT id, v FROM `${catalog}`.`${db}`.`${table}`"""
        assertEquals([[3L]], sql("""SELECT COUNT(*) FROM `${db}`.`${table}_sink`"""))
        assertEquals(0L, flightSessionsOfCatalogUser())

        // ... and a statement that fails after planning - here an INSERT whose label was already
        // used, refused when its transaction begins - has built no coordinator to close the session.
        sql """INSERT INTO `${db}`.`${table}_sink` WITH LABEL test_remote_doris_flight_session_label SELECT id, v FROM `${catalog}`.`${db}`.`${table}`"""
        assertEquals(0L, flightSessionsOfCatalogUser())
        test {
            sql """INSERT INTO `${db}`.`${table}_sink` WITH LABEL test_remote_doris_flight_session_label SELECT id, v FROM `${catalog}`.`${db}`.`${table}`"""
            exception "already been used"
        }
        assertEquals(0L, flightSessionsOfCatalogUser())
    } finally {
        sql """DROP CATALOG IF EXISTS `${catalog}`"""
        sql """DROP USER IF EXISTS '${user}'@'%'"""
        sql """DROP DATABASE IF EXISTS `${db}`"""
    }
}
