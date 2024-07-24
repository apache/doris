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

suite("test_array_first_index_function") {

    def tableName = "array_first_index_table"
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


    sql """
        INSERT INTO ${tableName} values
        (1, [1,2,3,4,5], [10,20,-40,80,-100]),
        (2, [6,7,8],[10,12,13]), (3, [1],[-100]), 
        (4, [1, null, 2], [null, 3, 1]), (5, [], []), (6, null, null)
    """

    qt_select "select array_first_index(x-> x + 1 > 2, [1, 2, 3])"
    qt_select "select array_first_index(x -> x > 1,[]);"
    qt_select "select array_first_index(x -> x > 1, [null]);"
    qt_select "select array_first_index(x -> x is null, [null, 1, 2]);"
    qt_select "select array_first_index(x -> x > 2, array_map(x->power(x,2),[1,2,3]));"

    qt_select "select *, array_first_index(x->x>2,[1,2,3]) from ${tableName} order by id;"
    qt_select "select *, array_first_index(x->x+1,[1,2,3]) from ${tableName} order by id;"
    qt_select "select *, array_first_index(x->x%2=0,[1,2,3]) from ${tableName} order by id;"

    qt_select "select c_array1, array_first_index(x->x,c_array1) from ${tableName} order by id;"
    qt_select "select c_array1, array_first_index(x->x>3,c_array1) from ${tableName} order by id;"
    qt_select "select c_array2, array_first_index(x->power(x,2)>100,c_array2) from ${tableName} order by id;"

    qt_select "select c_array1, c_array2, array_first_index((x,y)->x>y, c_array1, c_array2) from ${tableName} order by id;"
    qt_select "select c_array1, c_array2, array_first_index((x,y)->x+y, c_array1, c_array2) from ${tableName} order by id;"
    qt_select "select c_array1, c_array2, array_first_index((x,y)->x * abs(y) > 10, c_array1, c_array2) from ${tableName} order by id;"
    
    sql "DROP TABLE IF EXISTS ${tableName}"
}
