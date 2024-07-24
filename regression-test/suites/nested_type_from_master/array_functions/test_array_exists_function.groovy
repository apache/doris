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

suite("test_array_exists_function") {

    def tableName = "array_exists_table"
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
            (1, [1,2,3,4,5], [10,20,-40,80,-100]),
            (2, [6,7,8],[10,12,13]), (3, [1],[-100]), (4, null,null)
        """
        qt_select_1  "select * from ${tableName} order by id;"

        qt_select_2  "select *, array_exists(x->x>2,[1,2,3]) from ${tableName} order by id;"
        qt_select_3  "select *, array_exists(x->x+1,[1,2,3]) from ${tableName} order by id;"
        qt_select_4  "select c_array1, c_array2, array_exists(x->x%2=0,[1,2,3]) from ${tableName} order by id;"

        qt_select_5  "select c_array1, c_array2, array_exists(x->x,c_array1) from ${tableName} order by id;"
        qt_select_6  "select c_array1, c_array2, array_exists(x->x+2,c_array1) from ${tableName} order by id;"
        qt_select_7  "select c_array1, c_array2, array_exists(x->power(x,2)>10,c_array1) from ${tableName} order by id;"

        qt_select_8 "select array_exists(x -> x,[]);"
        qt_select_9 "select array_exists(x -> x,[null]);"
        qt_select_10 "select array_exists(x -> x,[1, 0]);"
        qt_select_11 "select array_exists(x -> x is null, [null, 1, 2]);"

        qt_select_12 "select *, array_exists([1,2,3]) from ${tableName} order by id;"
        qt_select_13 "select c_array1, c_array2, array_exists(c_array1) from ${tableName} order by id;"

        qt_select_14 "select array_exists([]);"
        qt_select_15 "select array_exists([null, 1, 2]);"
        
        sql "DROP TABLE IF EXISTS ${tableName}"
}
