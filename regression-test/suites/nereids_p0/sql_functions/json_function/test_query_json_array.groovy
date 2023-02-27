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

suite("test_query_json_array", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_query_json_array"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
              `k0` int(11) not null,
              `k1` int(11) NULL,
              `k2` boolean NULL,
              `k3` varchar(255),
              `k4` datetime
            ) ENGINE=OLAP
            DUPLICATE KEY(`k0`,`k1`,`k2`,`k3`,`k4`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """
    sql "insert into ${tableName} values(1,null,null,null,null);"
    sql "insert into ${tableName} values(2,1,null,null,null);"
    sql "insert into ${tableName} values(3,null,true,null,null);"
    sql "insert into ${tableName} values(4,null,null,'test','2022-01-01 11:11:11');"
    sql "insert into ${tableName} values(5,1,true,'test','2022-01-01 11:11:11');"
    // Nereids does't support array function
    // qt_sql2 "select json_array('k0',k0,'k1',k1,'k2',k2,'k3',k3,'k4',k4,'k5', null,'k6','k6') from ${tableName};"
    sql "DROP TABLE ${tableName};"
}
