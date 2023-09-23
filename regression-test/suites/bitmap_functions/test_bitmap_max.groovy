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

suite("test_bitmap_max") {
    sql """DROP TABLE IF EXISTS  t1;"""

    sql """
            CREATE TABLE `t1`
            (
                `col1` INT,
                `col2` bitmap BITMAP_UNION NOT NULL COMMENT ""
            ) ENGINE = OLAP AGGREGATE KEY(`col1`)
                DISTRIBUTED BY HASH(`col1`) BUCKETS 4
                PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1"
            );
    """

    sql """
            INSERT INTO t1 VALUES(1, to_bitmap(1)),
                (2, to_bitmap(2)),
                (3, to_bitmap(3));
    """

    order_qt_select """
            SELECT /*+ SET_VAR(query_timeout = 600) */
                Coalesce(Bitmap_max(Cast(
                Bitmap_empty() AS BITMAP)), a.`col1`)
                AS c4
            FROM   t1 AS a
            WHERE  Bitmap_max(Cast(NULL AS BITMAP)) IS NULL;
    """

}