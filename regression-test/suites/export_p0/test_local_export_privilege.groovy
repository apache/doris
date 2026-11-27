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

suite("test_local_export_privilege", "p0") {
    def db = (sql "SELECT DATABASE()")[0][0]
    def localEnabled = (sql "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_outfile_to_local'")[0][1].toBoolean()
    def denied = localEnabled ? "Local file export requires ADMIN_PRIV" : "enable_outfile_to_local"
    def user = "local_export_privilege_user"
    def password = "LocalExport_123"
    def outfile = "EXPLAIN SELECT id FROM local_export_privilege_tbl INTO OUTFILE 'file:///tmp/results_'"

    sql "DROP TABLE IF EXISTS local_export_privilege_tbl"
    sql '''CREATE TABLE local_export_privilege_tbl (id INT) DUPLICATE KEY(id)
           DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num"="1")'''
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '${password}'"
    try {
        sql "GRANT SELECT_PRIV ON ${db}.local_export_privilege_tbl TO ${user}"
        if (isCloudMode()) {
            def cluster = (sql "SHOW CLUSTERS")[0][0]
            sql "GRANT USAGE_PRIV ON CLUSTER `${cluster}` TO ${user}"
        }
        connect(user, password, context.config.jdbcUrl) {
            sql "USE ${db}"
            test {
                sql outfile
                exception denied
            }
            test {
                sql "EXPORT TABLE local_export_privilege_tbl TO 'file:///tmp/results_'"
                exception denied
            }
            sql '''EXPLAIN SELECT id FROM local_export_privilege_tbl INTO OUTFILE 'hdfs://nn/results_'
                   PROPERTIES ("hadoop.username"="doris")'''
        }

        sql "GRANT ADMIN_PRIV ON *.*.* TO ${user}"
        connect(user, password, context.config.jdbcUrl) {
            sql "USE ${db}"
            if (localEnabled) {
                sql outfile
            } else {
                test {
                    sql outfile
                    exception "enable_outfile_to_local"
                }
            }
        }
    } finally {
        sql "DROP USER ${user}"
    }
}
