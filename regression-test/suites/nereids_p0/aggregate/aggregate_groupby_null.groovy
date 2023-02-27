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

suite("aggregate_groupby_null") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def leftTable = "agg_groupby_null_left"
    sql """ DROP TABLE IF EXISTS ${leftTable} """
    sql """
            CREATE TABLE IF NOT EXISTS ${leftTable} (
                id INT NULL,
                device_id STRING NULL
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    sql """ INSERT INTO ${leftTable} VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4') """

    def rightTable = "agg_groupby_null_right"
    sql """ DROP TABLE IF EXISTS ${rightTable} """
    sql """
            CREATE TABLE IF NOT EXISTS ${rightTable} (
                id INT NULL,
                device_name STRING NULL
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    sql """ INSERT INTO ${rightTable} VALUES (1,'name'),(3,null) """

    qt_groupby_null """ SELECT rt.device_name, COUNT(${leftTable}.id) FROM ${leftTable}
                        LEFT JOIN ${rightTable} rt ON ${leftTable}.id = rt.id
		        WHERE rt.device_name is NULL group by rt.device_name """
}
