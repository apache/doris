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

// when rows is bigger than block bytes, it might cause diving zero error
suite("merge_sort_rows_bigger_than_bytes") {

    sql """ DROP TABLE IF EXISTS B """
    sql """
        CREATE TABLE IF NOT EXISTS B
        (
            b_id int
        )
        DISTRIBUTED BY HASH(b_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql " INSERT INTO B values (1);"

    qt_sql """
        SELECT subq_0.`c1` AS c1
        FROM
        (SELECT version() AS c0,
                  ref_0.`id` AS c1
        FROM test_streamload_action1 AS ref_0
        LEFT JOIN B AS ref_9 ON (ref_0.`id` = ref_9.`b_id`)
        WHERE ref_9.`b_id` IS NULL) AS subq_0
        WHERE subq_0.`c0` IS NOT NULL
        ORDER BY subq_0.`c0` DESC
        LIMIT 5;
    """
}