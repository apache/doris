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

suite("test_group_commit_interval_ms_property") {

    def tableName =  "test_group_commit_interval_ms_property_tbl"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k bigint,  
                v bigint
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (v) BUCKETS 8
                PROPERTIES(  
                "replication_num" = "1",
                "group_commit_interval_ms"="8000"
                );
        """

    sql "set enable_insert_group_commit = true;"
    
    qt_1 "show create table ${tableName}"

    sql "insert into ${tableName} values(1,1);"

    qt_2 "select * from ${tableName} order by k"

    Thread.sleep(5000);

    sql "insert into ${tableName} values(2,2)"

    qt_3 "select * from ${tableName} order by k"

    Thread.sleep(8000);

    qt_4 "select * from ${tableName} order by k"

    sql "ALTER TABLE ${tableName} SET (\"group_commit_interval_ms\"=\"1000\");"

    qt_5 "show create table ${tableName}"

    sql "insert into ${tableName} values(3,3)"

    qt_6 "select * from ${tableName} order by k"

    Thread.sleep(2000);

    qt_7 "select * from ${tableName} order by k"

    sql "insert into ${tableName} values(4,4)"

    qt_8 "select * from ${tableName} order by k"

    Thread.sleep(2000);
    
    qt_9 "select * from ${tableName} order by k"
    
    sql "DROP TABLE ${tableName}"
}