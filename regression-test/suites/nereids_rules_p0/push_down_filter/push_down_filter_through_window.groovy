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

suite("push_down_filter_through_window") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set ignore_shape_nodes='PhysicalDistribute'"
    sql "drop table if exists push_down_multi_column_predicate_through_window_t"
    multi_sql """
    CREATE TABLE push_down_multi_column_predicate_through_window_t (id INT, value1 INT, value2 VARCHAR(50))
    properties("replication_num"="1");
    INSERT INTO push_down_multi_column_predicate_through_window_t (id, value1, value2) VALUES(1, 10, 'A'),(2, 20, 'B'),(3, 30, 'C'),(4, 40, 'D');
    """
    qt_multi_column_predicate_push_down_window_shape """
    explain shape plan
    select * from (select row_number() over(partition by id,value1 order by value1) as num, id, value1 from push_down_multi_column_predicate_through_window_t ) t 
    where abs(id+value1)<4 and num<=2;
    """
    qt_multi_column_predicate_push_down_window """
    select * from (select row_number() over(partition by id,value1 order by value1) as num, id, value1 from push_down_multi_column_predicate_through_window_t ) t 
    where abs(id+value1)<30 and num<=2 order by id,value1,num;
    """
    qt_multi_column_or_predicate_push_down_window_shape """
    explain shape plan
    select * from (select id,value1, row_number() over(partition by id,value1 order by value1) rc from push_down_multi_column_predicate_through_window_t ) t where (id>1 or value1>2) and rc<2;
    """
    qt_multi_column_or_predicate_push_down_window """
    select * from (select id,value1, row_number() over(partition by id,value1 order by value1) rc from push_down_multi_column_predicate_through_window_t ) t where (id>1 or value1>2) and rc<2 order by 1,2 ;
    """
}