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
import groovy.json.JsonSlurper

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.sql.DriverManager
import java.sql.SQLException

suite("test_bootstrap_authentication", "docker,account") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 1
    options.beNum = 1
    options.feConfigs.removeAll { it.startsWith('allow_empty_password_for_root_admin=') }
    options.feConfigs.add('allow_empty_password_for_root_admin=false')
    options.feConfigs.add('skip_localhost_auth_check=false')
    options.feConfigs.add('enable_job_schedule_second_for_test=true')
    String password = 'Bootstrap_root_123'
    File secretDir = new File(context.config.realDataPath)
    secretDir.mkdirs()
    def secret = Files.createTempFile(secretDir.toPath(), 'bootstrap-root-', '.secret',
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString('rw-------')))
    Files.writeString(secret, password)
    options.rootPasswordFile = secret.toString()

    try {
        docker(options) {
            String url = cluster.jdbcUrl
            def fe = cluster.getFeByIndex(1)
            // Handshake errors precede SQL, so record their wire error codes as query-test data.
            def loginCode = { String user, String suppliedPassword ->
                try {
                    DriverManager.getConnection(url, user, suppliedPassword).close()
                    return 0
                } catch (SQLException error) {
                    return error.errorCode
                }
            }
            int rootEmptyCode = loginCode('root', '')
            int adminEmptyCode = loginCode('admin', '')
            def http = new URL("http://${fe.host}:${fe.httpPort}/api/show_meta_info?action=SHOW_HA").openConnection()
            http.setRequestProperty('Authorization', 'Basic ' + 'admin:'.bytes.encodeBase64().toString())
            int adminHttpCode = new JsonSlurper().parse(http.inputStream).code as int
            http.disconnect()

            long countBeforeRestart
            connect('root', password, url) {
                sql 'CREATE DATABASE IF NOT EXISTS bootstrap_authentication'
                sql 'USE bootstrap_authentication'
                qt_login_codes "SELECT ${rootEmptyCode}, ${adminEmptyCode}, ${adminHttpCode}"
                sql 'SET GLOBAL enable_audit_plugin = true'
                sql 'SET GLOBAL audit_plugin_max_batch_interval_sec = 1'
                sql 'DROP JOB IF EXISTS WHERE jobname = "bootstrap_auth_insert"'
                sql 'DROP MATERIALIZED VIEW IF EXISTS bootstrap_auth_mv'
                sql 'DROP TABLE IF EXISTS bootstrap_auth_source'
                sql 'DROP TABLE IF EXISTS bootstrap_auth_job_sink'
                sql 'DROP TABLE IF EXISTS bootstrap_auth_group_commit'
                sql '''CREATE TABLE bootstrap_auth_source (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1
                        PROPERTIES ('replication_num' = '1')'''
                sql '''CREATE TABLE bootstrap_auth_job_sink (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1
                        PROPERTIES ('replication_num' = '1')'''
                sql '''CREATE TABLE bootstrap_auth_group_commit (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1
                        PROPERTIES ('replication_num' = '1')'''
                sql "SET group_commit = 'sync_mode'"
                sql 'INSERT INTO bootstrap_auth_group_commit VALUES (8), (9)'
                sql "SET group_commit = 'off_mode'"
                order_qt_group_commit 'SELECT k FROM bootstrap_auth_group_commit'
                sql 'INSERT INTO bootstrap_auth_source VALUES (1), (2)'
                sql '''CREATE MATERIALIZED VIEW bootstrap_auth_mv
                        BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 10 SECOND
                        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')
                        AS SELECT k FROM bootstrap_auth_source'''
                awaitUntil(90, 1) { sql('SELECT COUNT(*) FROM bootstrap_auth_mv')[0][0] == 2 }
                order_qt_mtmv_before_restart 'SELECT k FROM bootstrap_auth_mv'
                sql '''CREATE JOB bootstrap_auth_insert ON SCHEDULE EVERY 1 SECOND
                        DO INSERT INTO bootstrap_authentication.bootstrap_auth_job_sink VALUES (7)'''
                awaitUntil(60, 1) { sql('SELECT COUNT(*) FROM bootstrap_auth_job_sink')[0][0] > 0 }
                qt_job_before_restart 'SELECT MIN(k), MAX(k), COUNT(*) > 0 FROM bootstrap_auth_job_sink'
                awaitUntil(90, 1) {
                    sql("SELECT COUNT(*) FROM __internal_schema.audit_log "
                            + "WHERE stmt LIKE '%bootstrap_auth_source%'")[0][0] > 0
                }
                qt_audit_import '''SELECT COUNT(*) > 0 FROM __internal_schema.audit_log
                                   WHERE stmt LIKE '%bootstrap_auth_source%' '''
                countBeforeRestart = (sql 'SELECT COUNT(*) FROM bootstrap_auth_job_sink')[0][0] as long
            }

            cluster.restartFrontends()
            connect('root', password, url) {
                sql 'USE bootstrap_authentication'
                int adminAfterRestart = loginCode('admin', '')
                qt_admin_after_restart "SELECT ${adminAfterRestart}"
                sql 'INSERT INTO bootstrap_auth_source VALUES (3)'
                awaitUntil(90, 1) { sql('SELECT COUNT(*) FROM bootstrap_auth_mv')[0][0] == 3 }
                order_qt_mtmv_after_restart 'SELECT k FROM bootstrap_auth_mv'
                awaitUntil(60, 1) {
                    sql('SELECT COUNT(*) FROM bootstrap_auth_job_sink')[0][0] > countBeforeRestart
                }
                qt_job_after_restart 'SELECT MIN(k), MAX(k), COUNT(*) > 0 FROM bootstrap_auth_job_sink'
                sql 'ANALYZE TABLE bootstrap_auth_source WITH SYNC'
                sql "ALTER USER 'admin' IDENTIFIED BY 'Bootstrap_admin_123'"
            }
            connect('admin', 'Bootstrap_admin_123', url) {
                qt_enabled_admin 'SELECT 1'
            }
        }
    } finally {
        Files.delete(secret)
    }
}
