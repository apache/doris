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

suite("test_timestamptz_user_variable_ctas") {
    // Run this suite through a follower FE to test user variable forwarding.
    // The same queries must give the same results through the master FE.
    sql "SET time_zone = '+08:00'"
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS timestamptz_user_variable_src"
    sql "DROP TABLE IF EXISTS timestamptz_user_variable_filtered"
    sql "DROP TABLE IF EXISTS timestamptz_user_variable_projected"
    sql """
        CREATE TABLE timestamptz_user_variable_src (pk INT, ts TIMESTAMPTZ(6))
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO timestamptz_user_variable_src VALUES
        (1, CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6)))
    """
    sql "SET @ts = CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6))"

    order_qt_select "SELECT pk FROM timestamptz_user_variable_src WHERE ts = @ts"
    sql """
        CREATE TABLE timestamptz_user_variable_filtered
        PROPERTIES ('replication_num' = '1')
        AS SELECT pk FROM timestamptz_user_variable_src WHERE ts = @ts
    """
    order_qt_filtered "SELECT pk FROM timestamptz_user_variable_filtered"

    // One row checks the value and the hour difference. Its type must retain scale 6.
    sql """
        CREATE TABLE timestamptz_user_variable_projected
        PROPERTIES ('replication_num' = '1')
        AS SELECT @ts AS v, timestampdiff(HOUR, @ts, ts) AS hours
        FROM timestamptz_user_variable_src
    """
    order_qt_type """
        SELECT column_name, column_type FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'timestamptz_user_variable_projected'
    """
    order_qt_projected "SELECT v, hours FROM timestamptz_user_variable_projected"
}
