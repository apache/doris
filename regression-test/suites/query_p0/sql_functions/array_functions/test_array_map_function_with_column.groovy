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

suite("test_array_map_function_with_column") {

    def tableName = "array_test_with_column"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` int(11) NULL COMMENT "",
          `k2` int(11) NULL COMMENT "",
          `c_array1` ARRAY<int(11)> NULL COMMENT "",
          `c_array2` ARRAY<int(11)> NULL COMMENT ""
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`,`k2`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """


    sql """INSERT INTO ${tableName} values
        (1, 2, [1,2,3,4], [null,2,3]),
        (2, 3, [6,7,null,9], [4,null,6]),
        (3, 4, NULL, [4, 5, 6]),
        (NULL, NULL, NULL, NULL);
    """

    qt_select_1  "select *,array_map(x->x+k1+k2 > k1*k2,c_array1) from ${tableName} order by k1;"

    sql "set batch_size = 1;"
    qt_select_2  "select *,array_map(x->x+k1+k2 > k1*k2,c_array1) from ${tableName} order by k1;"

    sql "set batch_size = 4;"
    qt_select_3  "select *,array_map(x->x+k1+k2 > k1*k2,c_array1) from ${tableName} order by k1;"

    sql "set batch_size = 6;"
    qt_select_4  "select *,array_map(x->x+k1+k2 > k1*k2,c_array1) from ${tableName} order by k1;"

    sql "set batch_size = 8;"
    qt_select_5  "select *,array_map(x->x+k1+k2 > k1*k2,c_array1) from ${tableName} order by k1;"

    sql "truncate table ${tableName};"

    sql """INSERT INTO ${tableName} values
        (4, 5, [6,7,null,9], [4,5,6,7]),
        (5, 6, [10,11,12,13], [8,9,null,11]),
        (6, 7, NULL, NULL),
        (NULL, NULL, NULL, NULL);
    """
    qt_select_6  "select *,array_map((x,y)->x+k1+k2 > y+k1*k2,c_array1,c_array2) from ${tableName} order by k1;"

    qt_select_7  "select *,array_map((x,y)->x+k1+k2 > y+k1*k2,c_array1,c_array2) from ${tableName} where array_count((x,y) -> k1*x>y+k2, c_array1, c_array2) > 1 order by k1;"

    sql "DROP TABLE IF EXISTS ${tableName}"
}
