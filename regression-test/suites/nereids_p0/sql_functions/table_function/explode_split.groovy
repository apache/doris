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

suite("explode_split") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_lv_str"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} 
        (k1 INT, k2 STRING) 
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 8  
        PROPERTIES("replication_num" = "1")
    """

    sql """ INSERT INTO ${tableName} VALUES (1, 'a,b,c') """

    qt_explode_split """ select * from ${tableName} 
                        lateral view explode_split(k2, ',') tmp1 as e1 """

    qt_explode_split """ select * from ${tableName}
                        lateral view explode_split(k2, ',') tmp1 as e1 
                        lateral view explode_split(k2, ',') tmp2 as e2 """

    qt_explode_split """ WITH example1  AS ( select  6 AS k1 ,'a,b,c' AS k2)
                         select  k1, e1 from example1
                         lateral view explode_split(k2, ',') tmp as  e1 """

}
