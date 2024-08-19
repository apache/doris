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

suite("test_hudi_timetravel", "p2,external,hudi,external_remote,external_remote_hudi") {

    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
    }

    String catalog_name = "test_hudi_timetravel"
    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            ${props}
        );
    """

    sql """switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """

    qt_q00 """select * from timetravel_cow order by id"""
    qt_q01 """select * from timetravel_cow FOR TIME AS OF "2024-07-24" order by id""" // no data
    qt_q02 """select * from timetravel_cow FOR TIME AS OF "20240724" order by id""" // no data
    qt_q01 """select * from timetravel_cow FOR TIME AS OF "2024-07-25" order by id"""
    qt_q02 """select * from timetravel_cow FOR TIME AS OF "20240725" order by id"""
    qt_q03 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "2024-07-24 19:58:43" order by id """  // no data
    qt_q04 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "20240724195843" order by id """ // no data
    qt_q05 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "2024-07-24 19:58:44" order by id """ // one
    qt_q06 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "20240724195844" order by id """ //one 
    qt_q07 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "2024-07-24 19:58:48" order by id """ // two
    qt_q08 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "20240724195848" order by id """ // two
    qt_q09 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "2024-07-24 19:58:49" order by id """ // three
    qt_q10 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "20240724195849" order by id """ // three
    qt_q11 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "2024-07-24 19:58:51" order by id """ // four
    qt_q12 """ select id, val1,val2,par1,par2 from timetravel_cow FOR TIME AS OF "20240724195851" order by id """ // four

    qt_q50 """select * from timetravel_mor order by id"""
    qt_q51 """select * from timetravel_mor FOR TIME AS OF "2024-07-24" order by id""" // no data
    qt_q52 """select * from timetravel_mor FOR TIME AS OF "20240724" order by id""" // no data
    qt_q51 """select * from timetravel_mor FOR TIME AS OF "2024-07-25" order by id"""
    qt_q52 """select * from timetravel_mor FOR TIME AS OF "20240725" order by id"""
    qt_q53 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "2024-07-24 19:58:53" order by id """  // no data
    qt_q54 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "20240724195853" order by id """ // no data
    qt_q55 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "2024-07-24 19:58:54" order by id """ // one
    qt_q56 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "20240724195854" order by id """ //one 
    qt_q57 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "2024-07-24 19:58:58" order by id """ // two
    qt_q58 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "20240724195858" order by id """ // two
    qt_q59 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "2024-07-24 19:58:59" order by id """ // three
    qt_q60 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "20240724195859" order by id """ // three
    qt_q61 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "2024-07-24 19:59:03" order by id """ // four
    qt_q62 """ select id, val1,val2,par1,par2 from timetravel_mor FOR TIME AS OF "20240724195903" order by id """ // four
}


/*

create table timetravel_cow (
    Id int,
    VAL1 string,
    val2 string,
    PAR1 string,
    par2 string
) using hudi
partitioned by (par1, par2)
TBLPROPERTIES (
  'type' = 'cow');

create table timetravel_mor (
    Id int,
    VAL1 string,
    val2 string,
    PAR1 string,
    par2 string
) using hudi
partitioned by (par1, par2)
TBLPROPERTIES (
  'primaryKey' = 'Id',
  'type' = 'mor');

insert into timetravel_cow values (1, 'a','b','para','para');
insert into timetravel_cow values (2, 'a','b','para','parb');
insert into timetravel_cow values (3, 'a','b','para','para');
insert into timetravel_cow values (4, 'a','b','para','parb');

insert into timetravel_mor values (1, 'a','b','para','para');
insert into timetravel_mor values (2, 'a','b','para','parb');
insert into timetravel_mor values (3, 'a','b','para','para');
insert into timetravel_mor values (4, 'a','b','para','parb');

*/
