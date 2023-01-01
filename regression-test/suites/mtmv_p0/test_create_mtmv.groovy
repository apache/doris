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

suite("test_create_mtmv") {
    def dbName = "db_mtmv"
    def tableName="t_user"
    def tableNamePv="t_user_pv"
    def mvName="multi_mv"
    sql """
        admin set frontend config("enable_mtmv_scheduler_framework"="true");
        """
    sql "DROP DATABASE IF EXISTS ${dbName};"
    sql "create database ${dbName};"
    sql "use ${dbName};"

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
        event_day DATE,
        id bigint,
        username varchar(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
        "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName} values("2022-10-26",1,"clz"),("2022-10-28",2,"zhangsang"),("2022-10-29",3,"lisi");
    """
    sql """
        create table ${tableNamePv}(
        event_day DATE,
        id bigint,
        pv bigint
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
    "replication_num" = "1"
    );
    """

    sql """
        insert into ${tableNamePv} values("2022-10-26",1,200),("2022-10-28",2,200),("2022-10-28",3,300);
    """
    sql """
        CREATE MATERIALIZED VIEW  ${mvName}
        BUILD IMMEDIATE 
        REFRESH COMPLETE 
        start with "2022-10-27 19:35:00"
        next  60 second
        KEY(username)   
        DISTRIBUTED BY HASH (username)  buckets 1
        PROPERTIES ('replication_num' = '1') 
        AS 
        select ${tableName}.username, ${tableNamePv}.pv  from ${tableName}, ${tableNamePv} where ${tableName}.id=${tableNamePv}.id;
    """
    Thread.sleep(10000);
    def result= sql """ select * from   ${mvName}"""
    assertTrue(result.size()==3);
}

