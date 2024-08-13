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
     sql """set runtime_filter_type='IN';"""
     qt_select_2 """
            select     "aaa" FROM     t_ods_tpisyncjpa4_2 tpisyncjpa4     inner join (         SELECT             USER_ID,             MAX(INTERNAL_CODE) as INTERNAL_CODE         FROM             t_ods_tpisyncjpa4_2         WHERE             STATE_ID = '1'         GROUP BY             USER_ID     ) jpa4 on tpisyncjpa4.USER_ID = jpa4.USER_ID;
     """
     qt_select_3 """
            select *, tpisyncjpp1.POST_ID=jpp1.POST_ID, tpisyncjpp1.INTERNAL_CODE=jpp1.INTERNAL_CODE from ( select tpisyncjpp1.POST_ID,tpisyncjpp1.INTERNAL_CODE as INTERNAL_CODE, tpisyncjpp1.STATE_ID, tpisyncjpp1.STATE_ID  ='1' from ( select tpisyncjpa4.* from t_ods_tpisyncjpa4_2  tpisyncjpa4 inner  join [broadcast]       (             SELECT                 USER_ID,                 MAX(INTERNAL_CODE)  as  INTERNAL_CODE             FROM                t_ods_tpisyncjpa4_2                              WHERE                 STATE_ID  =  '1'             GROUP  BY                 USER_ID         )jpa4 on  tpisyncjpa4.USER_ID=jpa4.USER_ID  and  tpisyncjpa4.INTERNAL_CODE=jpa4.INTERNAL_CODE where tpisyncjpa4.STATE_ID  ='1' ) tpisyncjpa4 inner join [broadcast] t_ods_tpisyncjpp1_2  tpisyncjpp1 where  tpisyncjpa4.USER_ID  =  tpisyncjpp1.USER_ID  AND  tpisyncjpp1.STATE_ID  ='1'   AND  tpisyncjpp1.POST_ID='BSDSAE1018' ) tpisyncjpp1 inner  join  [broadcast] (             SELECT                 POST_ID,                 MAX(INTERNAL_CODE)  as  INTERNAL_CODE             FROM                             t_ods_tpisyncjpp1_2             WHERE                 STATE_ID  =  '1'             GROUP  BY                 POST_ID )jpp1 on tpisyncjpp1.POST_ID=jpp1.POST_ID and tpisyncjpp1.INTERNAL_CODE=jpp1.INTERNAL_CODE;     
     """
     qt_select_4 """
        select DISTINCT         tpisyncjpa4.USER_ID as USER_ID,            tpisyncjpa4.USER_NAME as USER_NAME,       tpisyncjpp1.POST_ID AS "T4_POST_ID"   FROM t_ods_tpisyncjpa4_2 tpisyncjpa4 cross join [shuffle]       t_ods_tpisyncjpp1_2 tpisyncjpp1            inner join            (       SELECT         USER_ID,         MAX(INTERNAL_CODE) as INTERNAL_CODE       FROM        t_ods_tpisyncjpa4_2              WHERE         STATE_ID = '1'       GROUP BY         USER_ID     )jpa4         on tpisyncjpa4.USER_ID=jpa4.USER_ID and tpisyncjpa4.INTERNAL_CODE=jpa4.INTERNAL_CODE         inner join [shuffle]         (       SELECT         POST_ID,         MAX(INTERNAL_CODE) as INTERNAL_CODE       FROM             t_ods_tpisyncjpp1_2       WHERE         STATE_ID = '1'       GROUP BY         POST_ID     )jpp1         on tpisyncjpp1.POST_ID=jpp1.POST_ID and  tpisyncjpp1.INTERNAL_CODE=jpp1.INTERNAL_CODE         where tpisyncjpa4.USER_ID = tpisyncjpp1.USER_ID AND tpisyncjpp1.STATE_ID ='1' AND tpisyncjpa4.STATE_ID ='1'         AND tpisyncjpp1.POST_ID='BSDSAE1018';     
     """
    
 }