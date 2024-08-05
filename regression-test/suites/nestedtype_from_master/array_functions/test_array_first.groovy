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

suite("test_array_first") {

    def tableName = "test_array_first"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
                `id` int(11) NULL,
                `c_array1` array<int(11)> NULL, 
                `c_array2` array<varchar(20)> NULL
            ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
        """


        sql """INSERT INTO ${tableName} values
            (0, [2], ['123', '124', '125']),
            (1, [1,2,3,4,5], ['234', '124', '125']),
            (2, [1,2,10,12,10], ['345', '234', '123']), 
            (3, [1,3,4,2], ['222', '444', '555'])
        """
        qt_select_00 " select array_first(x -> x>3, [1,2,3,4,5]);"
        qt_select_01 " select array_first(x -> x<1, [1,2,3,4,5]);"
        qt_select_03 " select array_first(x -> x>=5,[1,2,3,4,5]);"
        qt_select_04 " select array_first(x -> x > 'abc', ['a','b','c']);"
        qt_select_05 " select array_first(x -> x > 5.2 , [10.2, 5.3, 4]);"

        qt_select_06  "select * from ${tableName} order by id;"
        
        qt_select_07 " select array_first(x->x>3,c_array1), array_first(x-> x>'124',c_array2) from test_array_first order by id;"        
        sql "DROP TABLE IF EXISTS ${tableName}"
}