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

suite("test_set_operation_adjust_nullable") {
    sql "set enable_fallback_to_original_planner=false"
    String realDb = context.config.getDbNameByFile(context.file)
    logger.info("realDb:${realDb}")
    sql """
        DROP TABLE IF EXISTS set_operation_t1
    """
    sql """
        DROP TABLE IF EXISTS set_operation_t2
    """

    sql """
        CREATE TABLE set_operation_t1(c1 varchar) DISTRIBUTED BY hash(c1) PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE set_operation_t2(c2 date) DISTRIBUTED BY hash(c2) PROPERTIES ("replication_num" = "1");
    """

    sql """
        insert into set_operation_t1 values('+06-00');
    """

    sql """
        insert into set_operation_t2 values('1990-11-11');
    """

    sql """
        SELECT c1, c1 FROM set_operation_t1 MINUS SELECT c2, c2 FROM set_operation_t2;
    """

    // do not use regulator child output nullable as init nullable info

    sql """
        DROP TABLE IF EXISTS set_operation_t1
    """
    sql """
        DROP TABLE IF EXISTS set_operation_t2
    """

    sql """
        create table set_operation_t1 (
            pk int,
            c1 char(25)  not null  ,
            c2 varchar(100)  null  ,
        )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """insert into set_operation_t1 values (1, '1', '1');"""

    sql """
        create table set_operation_t2 (
            c3 varchar(100)  not null  ,
            pk int
        )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """insert into set_operation_t2 values ('1', 1);"""

    sql """
        select
            c2,
            c1
        from
            set_operation_t1
        order by
            1,
            2 asc
        limit
            0
        union distinct
        select
            c3,
            c3
        from
            set_operation_t2
        except
        select
            'LDvlqYTfrq',
            'rVdUjeSaJW';
    """
}
