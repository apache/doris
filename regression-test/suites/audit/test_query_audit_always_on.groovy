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

import org.apache.doris.regression.suite.ClusterOptions

// query_audit_always_on changes only in fe.conf, so it needs a cluster of its own. The suite talks to the follower
// of two FEs: SET GLOBAL is forwarded to the master and rejected there, and a prepared EXECUTE forwarded to the
// master must leave one audit record, the follower's, and none of the master's.
suite("test_query_audit_always_on", "docker") {
    def skippedUser = "audit_always_on_skipped"
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.connectToFollower = true
    options.feConfigs += [
        'query_audit_always_on=true',
        // Values the switch does not allow: startup ignores them.
        "skip_audit_user_list=${skippedUser}".toString(),
        'audit_log_modules=slow_query,load',
        'audit_event_log_queue_size=10',
    ]

    docker(options) {
        def master = cluster.getMasterFe()
        def follower = cluster.getOneFollowerFe()
        // The markers are assembled with concat() in the queries that look for them, so that those queries, which
        // are audited as well, do not match themselves.
        def rejectedMarker = "audit_always_on_rejected"
        def preparedMarker = "audit_always_on_prepared"
        def skippedMarker = "audit_always_on_skipped_user"
        def like = { String marker -> "concat('%', '${marker.substring(0, 6)}', '${marker.substring(6)}', '%')" }

        // 1. The switch is on, and the conflicting values from fe.conf gave way.
        order_qt_always_on "ADMIN SHOW FRONTEND CONFIG LIKE 'query_audit_always_on'"
        order_qt_skip_audit_user_list "ADMIN SHOW FRONTEND CONFIG LIKE 'skip_audit_user_list'"
        order_qt_audit_log_modules "ADMIN SHOW FRONTEND CONFIG LIKE 'audit_log_modules'"
        order_qt_audit_event_log_queue_size "ADMIN SHOW FRONTEND CONFIG LIKE 'audit_event_log_queue_size'"

        // The audit table is created once the BE is up.
        awaitUntil(300, 2) {
            (sql """SELECT count(*) FROM information_schema.tables
                    WHERE table_schema = '__internal_schema' AND table_name = 'audit_log'""")[0][0] as long > 0
        }
        sql "TRUNCATE TABLE __internal_schema.audit_log"

        // 2. Every supported way to turn query audit off or to weaken it is rejected.
        def rejected = [
            "SET GLOBAL enable_audit_plugin = false",
            "SET enable_prepared_stmt_audit_log = false",
            "SET GLOBAL enable_prepared_stmt_audit_log = false",
            "SET enable_prepared_stmt_audit_log = DEFAULT",
            "UNSET VARIABLE enable_prepared_stmt_audit_log",
            "SET GLOBAL audit_plugin_max_sql_length = 100",
            "SET GLOBAL audit_plugin_max_insert_stmt_length = 100",
            "SET GLOBAL audit_plugin_load_timeout = 1",
            "SET GLOBAL audit_plugin_max_batch_bytes = 1",
            "SET GLOBAL audit_plugin_max_batch_interval_sec = 3600",
            'ADMIN SET FRONTEND CONFIG ("audit_event_log_queue_size" = "0")',
            'ADMIN SET ALL FRONTENDS CONFIG ("query_audit_log_timeout_ms" = "100000")',
        ]
        rejected.each { stmt ->
            test {
                sql "${stmt} /* ${rejectedMarker} */"
                exception "query_audit_always_on"
            }
        }
        // SET_VAR reports a variable it cannot set without the reason.
        test {
            sql "SELECT /*+ SET_VAR(enable_prepared_stmt_audit_log = false) */ 1"
            exception "Can not set session variable 'enable_prepared_stmt_audit_log'"
        }
        test {
            sql 'ADMIN SET FRONTEND CONFIG ("query_audit_always_on" = "false")'
            exception "is not mutable"
        }
        // Changes in the safe direction are allowed. The global enable_prepared_stmt_audit_log is true as well: the
        // master rewrote its default when it took over.
        sql "SET enable_prepared_stmt_audit_log = true"
        sql 'ADMIN SET FRONTEND CONFIG ("audit_event_log_queue_size" = "300000")'
        order_qt_variables_kept """SELECT @@global.enable_audit_plugin, @@global.enable_prepared_stmt_audit_log,
                @@enable_prepared_stmt_audit_log, @@global.audit_plugin_max_sql_length,
                @@global.audit_plugin_max_batch_bytes"""

        // 3. Each COM_STMT_EXECUTE is audited once, by the FE that received it, also when it is forwarded to the
        //    master, whose session asks for prepared statement audit too. It is prepared before forwarding is on,
        //    since a forwarded COM_STMT_PREPARE is refused.
        def followerUrl = "jdbc:mysql://${follower.host}:${follower.queryPort}/?useServerPrepStmts=true" +
                "&emulateUnsupportedPstmts=false&socketTimeout=30000"
        connect(context.config.jdbcUser, context.config.jdbcPassword, followerUrl) {
            def connection = context.getConnection()
            connection.createStatement().withCloseable { control ->
                control.execute("SET enable_prepared_stmt_audit_log = true")
                connection.prepareStatement("SELECT ?, '${preparedMarker}'").withCloseable { prepared ->
                    assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
                    prepared.setInt(1, 1)
                    prepared.executeQuery().close()
                    control.execute("SET force_forward_all_queries = true")
                    try {
                        prepared.setInt(1, 2)
                        prepared.executeQuery().close()
                    } finally {
                        control.execute("SET force_forward_all_queries = false")
                    }
                }
            }
        }

        // 4. The users in skip_audit_user_list are audited.
        sql "DROP USER IF EXISTS ${skippedUser}"
        sql "CREATE USER ${skippedUser}"
        connect(skippedUser, "", "jdbc:mysql://${follower.host}:${follower.queryPort}/") {
            sql "SELECT '${skippedMarker}'"
        }

        def flushAuditLog = {
            [master, follower].each { fe ->
                connect(context.config.jdbcUser, context.config.jdbcPassword,
                        "jdbc:mysql://${fe.host}:${fe.queryPort}/") {
                    sql "CALL flush_audit_log()"
                }
            }
        }
        def countAudit = { String condition ->
            return (sql "SELECT count(*) FROM __internal_schema.audit_log WHERE ${condition}")[0][0] as long
        }
        awaitUntil(180, 3) {
            flushAuditLog()
            countAudit("stmt LIKE ${like(rejectedMarker)}") >= rejected.size()
                    && countAudit("stmt LIKE ${like(preparedMarker)} AND stmt LIKE 'EXECUTE%'") >= 2
                    && countAudit("stmt LIKE ${like(skippedMarker)}") >= 1
        }
        // A record the master wrote for the forwarded execution would have arrived by now.
        sleep(15000)
        flushAuditLog()

        // Every rejected attempt is audited, as a failed statement.
        order_qt_rejected_audited """SELECT state, stmt FROM __internal_schema.audit_log
                WHERE stmt LIKE ${like(rejectedMarker)}"""
        order_qt_prepared_execute_audited """SELECT state, substring_index(stmt, ' USING ', -1)
                FROM __internal_schema.audit_log
                WHERE stmt LIKE ${like(preparedMarker)} AND stmt LIKE 'EXECUTE%'"""
        order_qt_skipped_user_audited """SELECT `user`, state, stmt FROM __internal_schema.audit_log
                WHERE stmt LIKE ${like(skippedMarker)}"""
    }
}
