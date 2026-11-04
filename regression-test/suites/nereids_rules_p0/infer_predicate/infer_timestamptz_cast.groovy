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

suite("infer_timestamptz_cast", "p0") {
    sql "DROP TABLE IF EXISTS infer_timestamptz_l"
    sql "DROP TABLE IF EXISTS infer_timestamptz_r"
    sql """
        CREATE TABLE infer_timestamptz_l (id INT, tz TIMESTAMPTZ(6))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE infer_timestamptz_r (id INT, dt DATETIMEV2(3))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO infer_timestamptz_l VALUES
        (1, CAST('2024-11-03 06:30:00 +00:00' AS TIMESTAMPTZ(6))),
        (2, CAST('2024-01-01 00:00:00.123600 +00:00' AS TIMESTAMPTZ(6)))
    """
    sql """
        INSERT INTO infer_timestamptz_r VALUES
        (10, '2024-11-03 01:30:00'), (20, '2024-01-01 00:00:00.124')
    """
    def originalTimeZone = sql "SELECT @@time_zone"
    try {
        sql "SET time_zone = 'America/New_York'"
        // 05:30Z and 06:30Z both map to 01:30 during the fall-back overlap.
        order_qt_dst_not_equal """
            SELECT l.id, r.id FROM infer_timestamptz_l l JOIN infer_timestamptz_r r
              ON CAST(l.tz AS DATETIMEV2(0)) = r.dt
            WHERE NOT (l.tz = CAST('2024-11-03 05:30:00 +00:00' AS TIMESTAMPTZ(6)))
        """
        order_qt_dst_greater_than """
            SELECT l.id, r.id FROM infer_timestamptz_l l JOIN infer_timestamptz_r r
              ON CAST(l.tz AS DATETIMEV2(0)) = r.dt
            WHERE l.tz > CAST('2024-11-03 05:30:00 +00:00' AS TIMESTAMPTZ(6))
        """
        sql "SET time_zone = '+00:00'"
        // .123600 and .124000 become equal after rounding to milliseconds.
        order_qt_scale_not_equal """
            SELECT l.id, r.id FROM infer_timestamptz_l l JOIN infer_timestamptz_r r
              ON CAST(l.tz AS DATETIMEV2(3)) = r.dt
            WHERE NOT (l.tz = CAST('2024-01-01 00:00:00.124000 +00:00' AS TIMESTAMPTZ(6)))
        """
        order_qt_scale_less_than """
            SELECT l.id, r.id FROM infer_timestamptz_l l JOIN infer_timestamptz_r r
              ON CAST(l.tz AS DATETIMEV2(3)) = r.dt
            WHERE l.tz < CAST('2024-01-01 00:00:00.124000 +00:00' AS TIMESTAMPTZ(6))
        """
    } finally {
        sql "SET time_zone = '${originalTimeZone[0][0]}'"
    }
}
