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

suite("outer_join_with_subquery", "query") {


    def tableName1 = "ods_park_cp_order_hand_lift_record"
    def tableName2 = "ods_park_cp_sys_enum"
    def tableName3 = "ods_park_cp_base_channel"
    def tableName4 = "dw_park_idm_org_map"
    def tableName5 = "util_date_type"
    def viewName = "util_date_type_now_view"

    sql """drop table if exists ${tableName1}"""
    sql """drop table if exists ${tableName2}"""
    sql """drop table if exists ${tableName3}"""
    sql """drop table if exists ${tableName4}"""
    sql """drop table if exists ${tableName5}"""
    sql """drop view if exists ${viewName}"""


    sql new File("""${context.file.parent}/ddl/${tableName1}.sql""").text
    sql new File("""${context.file.parent}/ddl/${tableName2}.sql""").text
    sql new File("""${context.file.parent}/ddl/${tableName3}.sql""").text
    sql new File("""${context.file.parent}/ddl/${tableName4}.sql""").text
    sql new File("""${context.file.parent}/ddl/${tableName5}.sql""").text
    sql new File("""${context.file.parent}/ddl/${viewName}.sql""").text


    qt_select """ with idm_org_table as (SELECT '9950019053' org_id)
                        , time_table as (
                                select * from ${viewName} where date_type = 'month'
                        )
                        select direction,plate,release_cause,enter_channel,lift_time,operater from (
                                select  if(a.direction='00','进场',if(a.direction='01','出场','')) direction,plate
                        ,if(b.cn_name is null,a.lift_cause,b.cn_name) release_cause
                        ,c.name enter_channel,a.lift_time,split_part(a.operater,'(',1) operater
                        ,row_number() over(order by lift_time desc ) r
                        from (
                                select direction,plate,lift_cause,lift_time,operater,channel_id
                                        from ${tableName1}
                                        where park_id in (SELECT park_id from ${tableName4} where org_id = (select org_id from idm_org_table))
                                        and lift_time >= date_sub(now(), INTERVAL 35 day)
                                        and lift_time>=(select start_time from time_table)
                                        and lift_time<=(select end_time from time_table)
                        ) a
                        left join (select code,cn_name from ${tableName2} where parent_id = 'cp_order_hand_lift_record_lift_cause') b
                        on a.lift_cause = b.code
                        left join ${tableName3} c on a.channel_id = c.pk_channel_id
                        where if('all'='all',1=1,'all'=c.name)
                        )  a
                        where r>0 and r<=20;
                """
}
