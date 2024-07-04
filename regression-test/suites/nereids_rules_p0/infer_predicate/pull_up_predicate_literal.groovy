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

suite("test_pull_up_predicate_literal") {
    sql """ DROP TABLE IF EXISTS test_pull_up_predicate_literal; """

    sql """
     CREATE TABLE `test_pull_up_predicate_literal` (
    `col1` varchar(50),
    `col2` varchar(50)
    ) 
    PROPERTIES
    (
    "replication_num"="1"
    );
 """
    sql "insert into test_pull_up_predicate_literal values('abc','def'),(null,'def'),('abc',null)"
    sql """
     DROP view if exists test_pull_up_predicate_literal_view;
 """

    sql """
     create view test_pull_up_predicate_literal_view
    (
    `col1` ,
        `col2`
    )
    AS
    select
    tmp.col1,tmp.col2
    from (
    select 'abc' as col1,'def' as col2
    ) tmp
    inner join test_pull_up_predicate_literal ds on tmp.col1 = ds.col1  and tmp.col2 = ds.col2;
 """


   qt_test_pull_up_literal """explain shape plan select * from test_pull_up_predicate_literal_view where col1='abc' and col2='def';"""



    sql """ DROP TABLE IF EXISTS test_pull_up_predicate_literal; """
    sql """ DROP view if exists test_pull_up_predicate_literal_view; """
}

