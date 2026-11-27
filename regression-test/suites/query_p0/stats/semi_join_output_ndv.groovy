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

suite("semi_join_output_ndv") {
    sql "DROP TABLE IF EXISTS semi_join_output_ndv_left"
    sql "DROP TABLE IF EXISTS semi_join_output_ndv_right"
    sql """
        CREATE TABLE semi_join_output_ndv_left (k INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE semi_join_output_ndv_right (k INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql 'INSERT INTO semi_join_output_ndv_left SELECT number % 20 FROM numbers("number" = "4000")'
    sql 'INSERT INTO semi_join_output_ndv_right SELECT number % 10 FROM numbers("number" = "4000")'
    sql "ANALYZE TABLE semi_join_output_ndv_left WITH SYNC"
    sql "ANALYZE TABLE semi_join_output_ndv_right WITH SYNC"

    def groupedSemiJoin = """
        SELECT l.k, COUNT(*)
        FROM semi_join_output_ndv_left l LEFT SEMI JOIN semi_join_output_ndv_right r ON l.k = r.k
        GROUP BY l.k
    """
    explain {
        sql "OPTIMIZED PLAN ${groupedSemiJoin}"
        contains "stats=10,"
    }
    order_qt_semi_join_groups groupedSemiJoin
}
