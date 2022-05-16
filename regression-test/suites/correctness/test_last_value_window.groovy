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

suite("test_last_value_window") {
    def tableName = "state"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
            `myday` INT,
            `time` VARCHAR(40) NOT NULL,
            `state` INT
            ) ENGINE=OLAP
            DUPLICATE KEY(`myday`,time,state)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`myday`) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName} VALUES 
            (21,"04-21-11",1),
            (22,"04-22-10-21",0),
            (22,"04-22-10-21",1),
            (23,"04-23-10",1),
            (24,"02-24-10-21",1); """

    // not_vectorized
    sql """ set enable_vectorized_engine = false; """

    qt_select_default """ select *,last_value(state) over(partition by myday order by time) from ${tableName}; """

}