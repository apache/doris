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
suite("eliminate_group_by") {
    sql "drop table if exists test_unique2;"
    sql """create table test_unique2(a int not null, b int) unique key(a) distributed by hash(a) properties("replication_num"="1");"""
    sql "insert into test_unique2 values(1,2),(2,2),(3,4),(4,4);"
    qt_count_star "select a,count(*) from test_unique2 group by a order by 1,2;"
    qt_count_1 "select a,count(1) from test_unique2 group by a order by 1,2;"
    qt_avg "select a,avg(b) from test_unique2 group by a order by 1,2;"
    qt_expr "select a,max(a+1),avg(abs(a+100)),sum(a+b) from test_unique2 group by a order by 1,2,3,4;"
    qt_window "select a,avg(sum(b) over(partition by b order by a)) from test_unique2 group by a order by 1,2"
}