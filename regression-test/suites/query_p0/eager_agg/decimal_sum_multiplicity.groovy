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

suite("decimal_sum_multiplicity") {
    sql """
        DROP TABLE IF EXISTS decimal_sum_l;
        DROP TABLE IF EXISTS decimal_sum_r;
        DROP TABLE IF EXISTS decimal_sum_out;
        CREATE TABLE decimal_sum_l (k VARCHAR(20) NOT NULL, v DECIMAL(38,18) NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1");
        CREATE TABLE decimal_sum_r (k VARCHAR(20) NOT NULL, w BIGINT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1");
        CREATE TABLE decimal_sum_out (k VARCHAR(20), sv DECIMAL(38,18), sw BIGINT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1");
        INSERT INTO decimal_sum_l VALUES
            ('x', 0.000000000100000000), ('x', 0.000000000100000000),
            ('negative', -1.123456789012345678), ('negative', NULL),
            ('null', NULL), ('null', NULL);
        INSERT INTO decimal_sum_r VALUES
            ('x', 1), ('x', 1), ('negative', 1), ('negative', 2), ('null', 3);
    """
    sql "SET disable_join_reorder=true"
    sql "SET decimal_overflow_scale=6"
    for (def decimal256 : [false, true]) {
        sql "SET enable_decimal256=${decimal256}"
        for (def checkOverflow : [false, true]) {
            sql "SET check_overflow_for_decimal=${checkOverflow}"
            for (def mode : [-1, 1]) {
                sql "SET eager_aggregation_mode=${mode}"
                def suffix = "${decimal256}_${checkOverflow}_${mode == -1 ? 'off' : 'on'}"
                // The alias used to expose the stale output type; without it, scale could be lost silently.
                "order_qt_alias_${suffix}" """
                    SELECT l.k AS g, SUM(l.v) AS sv, SUM(r.w) AS sw
                    FROM decimal_sum_l l JOIN [shuffle] decimal_sum_r r ON l.k=r.k GROUP BY l.k
                """
                "order_qt_no_alias_${suffix}" """
                    SELECT l.k, SUM(l.v), SUM(r.w)
                    FROM decimal_sum_l l JOIN [shuffle] decimal_sum_r r ON l.k=r.k GROUP BY l.k
                """
                sql """
                    INSERT OVERWRITE TABLE decimal_sum_out
                    SELECT l.k AS g, CAST(SUM(l.v) AS DECIMAL(38,18)), SUM(r.w)
                    FROM decimal_sum_l l JOIN [shuffle] decimal_sum_r r ON l.k=r.k GROUP BY l.k
                """
                "order_qt_insert_${suffix}" "SELECT * FROM decimal_sum_out"
            }
        }
    }
    // Both decimal-range overflow and native Int128 overflow must still be detected.
    sql "SET enable_decimal256=false"
    sql "SET check_overflow_for_decimal=true"
    sql "SET eager_aggregation_mode=1"
    sql """INSERT INTO decimal_sum_l VALUES
        ('precision_overflow', 60000000000000000000.000000000000000000),
        ('native_overflow', 90000000000000000000.000000000000000000)"""
    sql """INSERT INTO decimal_sum_r VALUES
        ('precision_overflow', 1), ('precision_overflow', 1),
        ('native_overflow', 1), ('native_overflow', 1)"""
    for (def key : ['precision_overflow', 'native_overflow']) {
        test {
            sql """SELECT l.k AS g, SUM(l.v), SUM(r.w)
                FROM decimal_sum_l l JOIN [shuffle] decimal_sum_r r ON l.k=r.k
                WHERE l.k='${key}' GROUP BY l.k"""
            exception "Arithmetic overflow"
        }
    }
}
