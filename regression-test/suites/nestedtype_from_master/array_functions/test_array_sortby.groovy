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

suite("test_array_sortby") {

    def tableName = "test_array_sortby"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
                `id` int(11) NULL,
                `c_array1` array<int(11)> NULL,
                `c_array2` array<int(11)> NULL
            ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
        """


        sql """INSERT INTO ${tableName} values
            (0,[3],[2]),
            (1, [1,2,3,4,5], [10,20,-40,80,-100]),
            (2, [6,7,8],[10,12,13]), (3, [1],[-100]), (4, null,null),
            (5,NULL,NULL),(6,[1,2],[2,1]),(7,[NULL],[NULL]),(8,[1,2,3],[3,2,1])
        """
        qt_select_00 " select array_sortby([1,2,3,4,5],[5,4,3,2,1]);"
        qt_select_01 " select array_sortby([1,2,3,4,5],NULL);"
        qt_select_03 " select array_sortby([1,2,3,4,5],[10,5,1,20,80]);"
        qt_select_04 " select array_sortby(['a','b','c'],['c','b','a']);"
        qt_select_05 " select array_sortby(['a','b','c'],[3,2,1]);"

        qt_select_06  "select * from ${tableName} order by id;"

        qt_select_07 " select *,array_sortby(c_array1,c_array2) from test_array_sortby order by id;"
        qt_select_08 " select id,c_array2,c_array1,array_sortby(c_array2,c_array1) from test_array_sortby order by id;"
        
        qt_select_09 " select *,array_map((x,y)->(x+y),c_array1,c_array2),array_sortby((x,y)->(x+y),c_array1,c_array2) from test_array_sortby order by id;"
        qt_select_10 " select *,array_map((x,y)->(x-y),c_array1,c_array2),array_sortby((x,y)->(x-y),c_array1,c_array2) from test_array_sortby order by id;"
        sql "DROP TABLE IF EXISTS ${tableName}"
}