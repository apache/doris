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
// scans. The scan runs its query when the coordinator dispatches the plan and the coordinator ends the
// session when it closes, so a plan that never runs never reaches the remote frontend at all. The
// remote frontend here is this one, and the catalog logs in as a user of its own: the Flight sessions
// of that user in the processlist are exactly the ones this suite's scans opened, and a SQL block rule
// bound to that user refuses every query a scan runs there.
suite("test_remote_doris_flight_session", "p0,external") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]
    String httpPort = frontends[0][3]
    String thriftPort = frontends[0][5]
    log.info("show frontends = ${frontends}, arrow: ${arrowPort}, http: ${httpPort}, thrift: ${thriftPort}")

    sql """DROP JOB IF EXISTS where jobname = 'test_remote_doris_flight_session_job'"""
    sql """DROP JOB IF EXISTS where jobname = 'test_remote_doris_flight_session_later_job'"""
    sql """DROP CATALOG IF EXISTS test_remote_doris_flight_session_catalog"""
    sql """DROP USER IF EXISTS 'test_remote_doris_flight_session_user'@'%'"""
    sql """DROP SQL_BLOCK_RULE IF EXISTS test_remote_doris_flight_session_rule"""
    sql """DROP DATABASE IF EXISTS test_remote_doris_flight_session_db"""
    sql """CREATE DATABASE test_remote_doris_flight_session_db"""
    sql """
        CREATE TABLE test_remote_doris_flight_session_db.test_remote_doris_flight_session_t (
          `id` int NOT NULL,
          `v` varchar(16) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """INSERT INTO test_remote_doris_flight_session_db.test_remote_doris_flight_session_t VALUES (1, 'a'), (2, 'b'), (3, 'c')"""
    sql """
        CREATE TABLE test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink (
          `id` int NOT NULL,
          `v` varchar(16) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // What the remote frontend asks of the catalog user: the Flight handshake takes any user, the
    // metadata REST calls need SHOW on the database, the query it runs needs SELECT on the table.
    sql """CREATE USER 'test_remote_doris_flight_session_user'@'%' IDENTIFIED BY 'C123_567p'"""
    sql """GRANT SELECT_PRIV ON internal.test_remote_doris_flight_session_db.* TO 'test_remote_doris_flight_session_user'@'%'"""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO 'test_remote_doris_flight_session_user'@'%'"""
    }

    sql """
        CREATE CATALOG test_remote_doris_flight_session_catalog PROPERTIES (
            'type' = 'doris',
            'fe_thrift_hosts' = '${host}:${thriftPort}',
            'fe_http_hosts' = 'http://${host}:${httpPort}',
            'fe_arrow_hosts' = '${host}:${arrowPort}',
            'user' = 'test_remote_doris_flight_session_user',
            'password' = 'C123_567p',
            'use_arrow_flight' = 'true'
        );
    """

    // The live count of the sessions this suite's scans left open on the remote frontend.
    def flightSessionsOfCatalogUser = { ->
        def rows = sql """
            SELECT COUNT(*) FROM information_schema.processlist
            WHERE User = 'test_remote_doris_flight_session_user' AND Protocol = 'ArrowFlightSQL'
        """
        return rows[0][0] as long
    }
    assertEquals(0L, flightSessionsOfCatalogUser())

    // Every scan opens one session on the remote frontend. It is closed when the local query's
    // coordinator closes, which happens before the result reaches the client, so none is left by
    // the time the next statement runs - however many scans in a row.
    for (int i = 0; i < 5; i++) {
        order_qt_scan """SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
        assertEquals(0L, flightSessionsOfCatalogUser())
    }

    // A plan that never runs never reaches the remote frontend. INSERT OVERWRITE plans the query
    // once only to find the target partitions and discards that plan before the insert plans again
    // and runs: one query on the remote frontend, its session ended with the insert ...
    sql """INSERT OVERWRITE TABLE test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
    qt_overwrite_rows """SELECT COUNT(*) FROM test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink"""
    assertEquals(0L, flightSessionsOfCatalogUser())

    // ... and a statement that fails after planning - here an INSERT whose label was already
    // used, refused when its transaction begins - fails before any coordinator dispatches its plan.
    sql """INSERT INTO test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink WITH LABEL test_remote_doris_flight_session_label SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
    assertEquals(0L, flightSessionsOfCatalogUser())
    test {
        sql """INSERT INTO test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink WITH LABEL test_remote_doris_flight_session_label SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
        exception "already been used"
    }
    assertEquals(0L, flightSessionsOfCatalogUser())

    // With every query the catalog user runs on the remote frontend refused - the query of a
    // remote Doris scan carries this hint - a scan that runs fails ...
    sql """CREATE SQL_BLOCK_RULE test_remote_doris_flight_session_rule PROPERTIES ("sql" = "enable_parallel_result_sink", "global" = "false", "enable" = "true")"""
    sql """SET PROPERTY FOR 'test_remote_doris_flight_session_user' 'sql_block_rules' = 'test_remote_doris_flight_session_rule'"""
    test {
        sql """SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
        exception "Failed to execute query"
    }
    // ... while what only plans one does not reach the remote frontend: an EXPLAIN, which shows
    // the query the scan would run, whatever comes before the keyword ...
    explain {
        sql """SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
        contains "enable_parallel_result_sink"
    }
    sql """/* not a plain EXPLAIN */ EXPLAIN SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
    // ... a statement failing after planning, which reports its own error ...
    test {
        sql """INSERT INTO test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink WITH LABEL test_remote_doris_flight_session_label SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
        exception "already been used"
    }
    // ... and CREATE JOB, which plans the job's statement only to validate it.
    sql """CREATE JOB test_remote_doris_flight_session_later_job ON SCHEDULE AT '2099-01-01 00:00:00' DO INSERT INTO test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
    assertEquals(0L, flightSessionsOfCatalogUser())
    sql """SET PROPERTY FOR 'test_remote_doris_flight_session_user' 'sql_block_rules' = ''"""

    // A job's insert task plans its statement with no SQL text of its own; it reads the remote
    // table like any other insert.
    sql """CREATE JOB test_remote_doris_flight_session_job ON SCHEDULE AT CURRENT_TIMESTAMP DO INSERT INTO test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink SELECT id, v FROM test_remote_doris_flight_session_catalog.test_remote_doris_flight_session_db.test_remote_doris_flight_session_t"""
    awaitUntil(60) {
        def tasks = sql """SELECT status FROM tasks("type"="insert") WHERE JobName = 'test_remote_doris_flight_session_job'"""
        tasks.size() == 1 && (tasks[0][0] == "SUCCESS" || tasks[0][0] == "FAILED")
    }
    def tasks = sql """SELECT status, ErrorMsg FROM tasks("type"="insert") WHERE JobName = 'test_remote_doris_flight_session_job'"""
    assert tasks[0][0] == "SUCCESS" : "the insert task failed: ${tasks[0][1]}"
    qt_job_rows """SELECT COUNT(*) FROM test_remote_doris_flight_session_db.test_remote_doris_flight_session_sink"""
    assertEquals(0L, flightSessionsOfCatalogUser())
}
