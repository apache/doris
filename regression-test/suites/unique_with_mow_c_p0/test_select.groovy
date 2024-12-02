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

suite("test_select") {
    sql """ DROP TABLE IF EXISTS test_select0 """
    sql """ DROP TABLE IF EXISTS test_select1 """

    sql """ 
        create table test_select0 (
            pk int,
            v1 char(255) null,
            v2 varchar(255) not null, 
            v3 varchar(1000) not null 
        ) engine=olap
        UNIQUE KEY(pk)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """ 
        create table test_select1 (
            pk int,
            v1 char(255) null,
            v2 varchar(255) not null, 
            v3 varchar(1000) not null 
        ) engine=olap
        UNIQUE KEY(pk)
        cluster by(v2, v3)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test_select0 values 
        (0,null,'a','1'),
        (1,'r','0','9999-12-31 23:59:59'),
        (2,'n','she','i'),
        (3,'but','can','2024-08-03 13:08:30');
    """

    sql """
        insert into test_select1 values 
        (0,null,'a','1'),
        (1,'r','0','9999-12-31 23:59:59'),
        (2,'n','she','i'),
        (3,'but','can','2024-08-03 13:08:30');
    """

    order_qt_sql0 """ 
        select v2  from test_select0 where v1 is not null  ORDER BY v2 LIMIT 2 ;
    """

    order_qt_sql1 """ 
        select v2  from test_select1 where v1 is not null  ORDER BY v2 LIMIT 2 ;
    """
}
