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


/*
How to produce the bug:
when a column's _fixed_values exceeds the max_pushdown_conditions_per_column limit,
the column will not perform predicate pushdown, but if there are subsequent columns 
that need to be pushed down, the subsequent column pushdown will be misplaced in 
_scan_keys and it causes query results to be wrong
*/
suite("test_exceed_max_pushdown_conditions_per_column") {
    def tableName = "exceed_max_pushdown_conditions_per_column_limit_test_table";
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        create table if not exists ${tableName}(
            id1 int comment 'id1'
            ,id2 int comment 'id2'
        ) ENGINE=OLAP
        UNIQUE KEY(id1,id2)
        COMMENT '超过max_pushdown_conditions_per_column测试'
        DISTRIBUTED BY HASH(id1,id2) BUCKETS 1
        PROPERTIES("replication_num" = "1")
        ;
    """
    sql """
        insert into ${tableName}(id1,id2) values(1,2);
    """
    sql """
        set max_pushdown_conditions_per_column = 10;
    """
    qt_select1 """
        select * from ${tableName} where id1 in
        (1,2,3,4,5,6,7,8,9,10)
        and id2=2;
    """
    qt_select2 """
        select * from ${tableName} where id1 in
        (1,2,3,4,5,6,7,8,9,10,11)
        and id2=2;
    """
    sql "DROP TABLE IF EXISTS ${tableName};"
}

