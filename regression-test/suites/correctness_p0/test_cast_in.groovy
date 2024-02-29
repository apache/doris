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

suite("test_cast_in") {
    sql """
        set enable_nereids_planner=false;
    """
    sql """DROP TABLE IF EXISTS date_dim_table_x;"""
    sql """
        CREATE TABLE IF NOT EXISTS date_dim_table_x (
        d_date date,
        d_week_seq integer
    )
    UNIQUE KEY(d_date)
    DISTRIBUTED BY HASH(d_date) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    """

    sql """SELECT d_date
            FROM date_dim_table_x
            WHERE (d_date IN 
                (SELECT d_date
                FROM date_dim_table_x
                WHERE (d_week_seq IN 
                    (SELECT d_week_seq
                    FROM date_dim_table_x
                    WHERE (d_date IN (CAST('2000-06-30' AS DATE) , CAST('2000-09-27' AS DATE) , CAST('2000-11-17' AS DATE))) )) ));"""

    sql """DROP TABLE IF EXISTS date_dim_table_x;"""
}
