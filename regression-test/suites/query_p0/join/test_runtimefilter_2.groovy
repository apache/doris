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

 suite("test_runtimefilter_2", "query_p0") {
     sql "drop table if exists t_ods_tpisyncjpa4_2;"
     sql """ create table t_ods_tpisyncjpa4_2(INTERNAL_CODE varchar(50), USER_ID varchar(50), USER_NAME varchar(50), STATE_ID varchar(50)) distributed by hash(INTERNAL_CODE) properties('replication_num'='1'); """

     sql """ insert into t_ods_tpisyncjpa4_2 values('1', '2', '3', '1');"""

     sql "drop table if exists t_ods_tpisyncjpp1_2;"
     sql """ create table t_ods_tpisyncjpp1_2(INTERNAL_CODE varchar(50), USER_ID varchar(50), STATE_ID varchar(50), POST_ID varchar(50)) distributed by hash(INTERNAL_CODE) properties('replication_num'='1'); """

     sql """insert into t_ods_tpisyncjpp1_2 values('1', '2', '1', 'BSDSAE1018');"""

     sql """set runtime_filter_type='MIN_MAX';"""
     qt_select_1 """
            select     "aaa" FROM     t_ods_tpisyncjpa4_2 tpisyncjpa4     inner join (         SELECT             USER_ID,             MAX(INTERNAL_CODE) as INTERNAL_CODE         FROM             t_ods_tpisyncjpa4_2         WHERE             STATE_ID = '1'         GROUP BY             USER_ID     ) jpa4 on tpisyncjpa4.USER_ID = jpa4.USER_ID;
     """
 }