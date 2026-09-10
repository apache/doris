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

suite("test_mow_time_travel_row_policy", "nonConcurrent,p0,auth") {
    if (isCloudMode()) {
        return
    }

    String dbName = "test_mow_time_travel_row_policy_db"
    String tableName = "test_mow_time_travel_row_policy_table"
    String user = "test_mow_time_travel_row_policy_user"
    String password = "C123_567p"
    String restrictivePolicy = "test_mow_time_travel_value_policy"
    String firstKeyPolicy = "test_mow_time_travel_first_key_policy"
    String thirdKeyPolicy = "test_mow_time_travel_third_key_policy"

    try_sql "DROP ROW POLICY IF EXISTS ${restrictivePolicy} ON ${dbName}.${tableName} FOR ${user}"
    try_sql "DROP ROW POLICY IF EXISTS ${firstKeyPolicy} ON ${dbName}.${tableName} FOR ${user}"
    try_sql "DROP ROW POLICY IF EXISTS ${thirdKeyPolicy} ON ${dbName}.${tableName} FOR ${user}"
    try_sql "DROP USER IF EXISTS ${user}"
    sql "DROP DATABASE IF EXISTS ${dbName}"

    try {
        sql "CREATE DATABASE ${dbName}"
        sql """
            CREATE TABLE ${dbName}.${tableName} (
                k INT,
                v INT
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                'replication_num' = '1',
                'enable_unique_key_merge_on_write' = 'true',
                'binlog.enable' = 'true',
                'binlog.format' = 'ROW',
                'binlog.need_historical_value' = 'true'
            )
        """

        sql "INSERT INTO ${dbName}.${tableName} VALUES (1, 10), (2, 20), (3, 30), (5, 50)"
        sql "SET show_hidden_columns = true"
        long snapshotTso = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM ${dbName}.${tableName}")[0][0] as Long
        sql "SET show_hidden_columns = false"

        // The historical left branch contains unchanged keys 1 and 5. The row-binlog right branch
        // restores updated key 2 and deleted key 3. A new key 4 must not appear in the snapshot.
        sql "INSERT INTO ${dbName}.${tableName} VALUES (2, 200)"
        sql "DELETE FROM ${dbName}.${tableName} WHERE k = 3"
        sql "INSERT INTO ${dbName}.${tableName} VALUES (4, 40)"

        sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
        sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ${user}"
        sql """
            CREATE ROW POLICY ${restrictivePolicy} ON ${dbName}.${tableName}
            AS RESTRICTIVE TO ${user} USING (v <= 30)
        """
        sql """
            CREATE ROW POLICY ${firstKeyPolicy} ON ${dbName}.${tableName}
            AS PERMISSIVE TO ${user} USING (k = 1)
        """
        sql """
            CREATE ROW POLICY ${thirdKeyPolicy} ON ${dbName}.${tableName}
            AS PERMISSIVE TO ${user} USING (k = 3)
        """

        String userJdbcUrl = org.apache.doris.regression.Config.buildUrlWithDb(
                context.config.jdbcUrl, dbName)
        connect(user, password, userJdbcUrl) {
            sql "SET enable_nereids_planner = true"
            sql "SET enable_fallback_to_original_planner = false"
            sql "SET enable_sql_cache = false"

            // Restrictive AND (permissive OR permissive) leaves only key 1 in the latest image.
            assertEquals([[1, 10]], sql("SELECT k, v FROM ${tableName} ORDER BY k"))

            // key 1 is supplied by the base-scan branch and key 3 by the row-binlog branch. A
            // missing policy marker on either scan leaks key 5 or key 2 respectively.
            assertEquals([[1, 10], [3, 30]], sql("""
                SELECT k, v FROM ${tableName} FOR VERSION AS OF ${snapshotTso} ORDER BY k
            """))
        }
    } finally {
        try_sql "DROP ROW POLICY IF EXISTS ${restrictivePolicy} ON ${dbName}.${tableName} FOR ${user}"
        try_sql "DROP ROW POLICY IF EXISTS ${firstKeyPolicy} ON ${dbName}.${tableName} FOR ${user}"
        try_sql "DROP ROW POLICY IF EXISTS ${thirdKeyPolicy} ON ${dbName}.${tableName} FOR ${user}"
        try_sql "DROP USER IF EXISTS ${user}"
        sql "DROP DATABASE IF EXISTS ${dbName}"
    }
}
