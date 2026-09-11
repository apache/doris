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
suite("infer_datetimev2_cast_precision", "p0") {
    sql "DROP TABLE IF EXISTS infer_datetimev2_cast_l"
    sql "DROP TABLE IF EXISTS infer_datetimev2_cast_r"
    sql """
        CREATE TABLE infer_datetimev2_cast_l (id INT, a DATETIMEV2(6))
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE infer_datetimev2_cast_r (id INT, b DATETIMEV2(6))
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO infer_datetimev2_cast_l VALUES (1, '2025-01-01 00:00:00.100000')"
    sql "INSERT INTO infer_datetimev2_cast_r VALUES (10, '2025-01-01 00:00:00.200000')"

    // Distinct raw values become equal after narrowing the fractional-second precision.
    sql "SET disable_nereids_rules = 'INFER_PREDICATES'"
    order_qt_without_inference """
        SELECT COUNT(*) AS cnt, MIN(l.id) AS left_id, MIN(r.id) AS right_id
        FROM infer_datetimev2_cast_l l JOIN infer_datetimev2_cast_r r
          ON CAST(l.a AS DATETIMEV2(0)) = CAST(r.b AS DATETIMEV2(0))
        WHERE l.a IN ('2025-01-01 00:00:00.100000', '2025-01-01 00:00:00.300000')
    """
    sql "SET disable_nereids_rules = ''"
    order_qt_with_inference """
        SELECT COUNT(*) AS cnt, MIN(l.id) AS left_id, MIN(r.id) AS right_id
        FROM infer_datetimev2_cast_l l JOIN infer_datetimev2_cast_r r
          ON CAST(l.a AS DATETIMEV2(0)) = CAST(r.b AS DATETIMEV2(0))
        WHERE l.a IN ('2025-01-01 00:00:00.100000', '2025-01-01 00:00:00.300000')
    """
}
