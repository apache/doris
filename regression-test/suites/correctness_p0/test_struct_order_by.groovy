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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_struct_order_by") {
    sql """drop table if exists test_struct_order_by;"""
    sql """CREATE TABLE test_struct_order_by(
                       typ_id     BIGINT          NOT NULL COMMENT "ID",
                       name       VARCHAR(20)     NULL     COMMENT "名称",
                       arr        STRUCT<f1:INT,f2:array<int(10)>,f3:STRING> NULL     COMMENT "数组"
                   )
                   DUPLICATE KEY(typ_id)
                   DISTRIBUTED BY HASH(typ_id) BUCKETS 10
                   PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """insert into test_struct_order_by values(1,'name1',NULL), (6,'name2',{1, [1,2,3,4,5], 'name2'}), (7,'name3',{1, [1,2,3,4,5], 'name3'}), (2,'name3',{1, [1,2,3], 'name3'}), (3,'name3',{1, [null,2,3,4,5], 'name3'}), (4,'name3',{null, [1,2,3,4,5], 'name2'}), (6,'name2',{1, [1,2,3,4,5], 'name2'});"""
    qt_select1 """ select * from test_struct_order_by order by arr ASC LIMIT 10;  """
    qt_select2 """ select * from test_struct_order_by order by arr DESC LIMIT 10;  """
    qt_select3 """ select * from test_struct_order_by order by name,arr ASC LIMIT 10;  """
    qt_select4 """ select * from test_struct_order_by order by name,arr DESC LIMIT 10;  """
    qt_select5 """ select * from test_struct_order_by order by typ_id,arr ASC LIMIT 10;  """
    qt_select6 """ select * from test_struct_order_by order by typ_id,arr DESC LIMIT 10;  """

    qt_select7 """ select arr, count(typ_id) from test_struct_order_by group by arr order by arr;  """
    qt_select8 """ select arr,name, count(typ_id) from test_struct_order_by group by name,arr order by name,arr;  """
    qt_select9 """ select arr,name, count(typ_id) from test_struct_order_by group by arr,name order by name,arr;  """
    qt_select11 """ select arr,name, sum(typ_id) over(partition by arr,name order by arr,name rows between unbounded preceding and current row)  from test_struct_order_by order by arr """
    qt_select12 """ select arr,name, sum(typ_id) over(partition by name, arr order by name,arr rows between unbounded preceding and current row)  from test_struct_order_by order by arr """
    qt_select13 """ select arr, sum(typ_id) over(partition by arr order by arr rows between unbounded preceding and current row)  from test_struct_order_by order by arr """

    sql """ set force_sort_algorithm=topn; """
    qt_select1 """ select * from test_struct_order_by order by arr ASC LIMIT 10;  """
    qt_select2 """ select * from test_struct_order_by order by arr DESC LIMIT 10;  """
    qt_select3 """ select * from test_struct_order_by order by name,arr ASC LIMIT 10;  """
    qt_select4 """ select * from test_struct_order_by order by name,arr DESC LIMIT 10;  """
    qt_select5 """ select * from test_struct_order_by order by typ_id,arr ASC LIMIT 10;  """
    qt_select6 """ select * from test_struct_order_by order by typ_id,arr DESC LIMIT 10;  """
    sql """ set force_sort_algorithm=heap; """
    qt_select1 """ select * from test_struct_order_by order by arr ASC LIMIT 10;  """
    qt_select2 """ select * from test_struct_order_by order by arr DESC LIMIT 10;  """
    qt_select3 """ select * from test_struct_order_by order by name,arr ASC LIMIT 10;  """
    qt_select4 """ select * from test_struct_order_by order by name,arr DESC LIMIT 10;  """
    qt_select5 """ select * from test_struct_order_by order by typ_id,arr ASC LIMIT 10;  """
    qt_select6 """ select * from test_struct_order_by order by typ_id,arr DESC LIMIT 10;  """
}
