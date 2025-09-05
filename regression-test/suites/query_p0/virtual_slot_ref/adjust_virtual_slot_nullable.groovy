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

suite("adjust_virtual_slot_nullable") {
    def tbl1 = "tbl_adjust_virtual_slot_nullable_1"
    def tbl2 = "tbl_adjust_virtual_slot_nullable_2"
    multi_sql """
        SET ignore_shape_nodes='PhysicalDistribute';
        drop table if exists ${tbl1} force;
        drop table if exists ${tbl2} force;
        create table ${tbl1} (c_int int not null, c_date date not null) properties('replication_num' = '1');
        create table ${tbl2} (c_int int not null, c_date date not null) properties('replication_num' = '1');
        insert into ${tbl1} values
            (1, '2020-01-01'),
            (1, '2020-01-02'),
            (1, '2020-01-03'),
            (2, '2021-01-01'),
            (2, '2021-01-02'),
            (2, '2021-01-03');
        insert into ${tbl2} values
            (1, '2022-02-01'),
            (1, '2022-02-02'),
            (1, '2022-02-03');
    """

    def querySql = """
        SELECT t1.*, t2.*
        FROM
            ${tbl1} AS t1
        LEFT JOIN ${tbl2} AS t2
        ON  t1.c_int = t2.c_int
        WHERE
            NOT (
                    day(t2.c_date) IN (1, 3)
                AND
                    day(t2.c_date) IN (2, 3, 3)
                );
    """

    explainAndOrderResult 'left_join', querySql
    explain {
        sql querySql
        verbose true
        contains 'type=tinyint, nullable=false, isAutoIncrement=false, subColPath=null, virtualColumn=dayofmonth(c_date'
    }
}
