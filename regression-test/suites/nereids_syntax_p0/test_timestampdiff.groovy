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

suite("test_timestampdiff") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    qt_select """
        SELECT TIMESTAMPDIFF(year,'2003-02-01','2004-05-01');
    """
    qt_select """
        SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');
    """
    qt_select """
        SELECT TIMESTAMPDIFF(day,'2003-02-01','2003-02-03');
    """
    qt_select """
        SELECT TIMESTAMPDIFF(hour,'2003-02-03 10:00:00','2003-02-03 11:00:00');
    """

    qt_select """
        SELECT TIMESTAMPDIFF(minute,'2003-02-03 11:00:00','2003-02-03 11:03:00');
    """
    qt_select """
        SELECT TIMESTAMPDIFF(second,'2003-02-03 11:00:00','2003-02-03 11:00:40');
    """

    qt_select """
        SELECT TIMESTAMPDIFF(microsecond,
                CAST('2024-01-01 10:00:00.123456' AS DATETIMEV2(6)),
                CAST('2024-01-01 10:00:00.999999' AS DATETIMEV2(6)));
    """

    sql """drop table if exists test_timestampdiff_microsecond"""
    sql """
        create table test_timestampdiff_microsecond (
            id int,
            t datetimev2(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """

    sql """
        insert into test_timestampdiff_microsecond values
            (1, '2024-01-01 10:00:00.123456'),
            (2, '2024-01-01 10:00:00.999999');
    """

    qt_select """
        SELECT MAX(t), MIN(t), TIMESTAMPDIFF(MICROSECOND, MIN(t), MAX(t))
        FROM test_timestampdiff_microsecond;
    """
}