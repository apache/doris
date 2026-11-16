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

suite("test_date_user_variable_ctas") {
    // Run this suite through a follower FE to test user variable forwarding.
    // A single result row checks all values. No source table is needed.
    sql "SET enable_sql_cache = false"
    sql "SET @d = CAST('2024-01-01' AS DATE)"
    sql "SET @midnight = CAST('2024-01-01 00:00:00.000000' AS DATETIME(6))"
    sql "SET @noon = CAST('2024-01-01 12:00:00.000000' AS DATETIME(6))"
    sql "SET @fraction = CAST('2024-01-01 12:00:00.120000' AS DATETIME(6))"

    order_qt_select "SELECT @d, @midnight, @noon, @fraction"
    sql "DROP TABLE IF EXISTS date_user_variable_ctas"
    sql """
        CREATE TABLE date_user_variable_ctas
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT @d AS d, @midnight AS midnight, @noon AS noon, @fraction AS fraction
    """
    // Midnight must remain DATETIME(6). The values .000000 and .120000 must retain scale 6.
    order_qt_types """
        SELECT column_name, column_type FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'date_user_variable_ctas'
    """
    order_qt_values "SELECT d, midnight, noon, fraction FROM date_user_variable_ctas"
}
