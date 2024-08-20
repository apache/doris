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


suite("test_hive_rename_column_orc_parquet", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hivePrefix  ="hive3";
        setHivePrefix(hivePrefix)
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
    
        String catalog_name = "test_hive_schema_change2"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
        create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hadoop.username' = 'hadoop',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
        );
        """

        sql """ switch ${catalog_name} """
        sql """ use `default` """
        

        sql """ set hive_orc_use_column_names=true; """ 
        qt_hive1_orc_1_true """ select  * from  simulation_hive1_orc order by b; """
        qt_hive1_orc_2_true """ select  b,c,a from  simulation_hive1_orc order by b; """
        qt_hive1_orc_3_true """ select  c,a from  simulation_hive1_orc order by b; """
        qt_hive1_orc_4_true """ select  count(*) from  simulation_hive1_orc; """
        qt_hive1_orc_5_true """ select  count(a) from  simulation_hive1_orc; """
        qt_hive1_orc_6_true """ select  b from  simulation_hive1_orc order by b; """
        qt_hive1_orc_7_true """ select  b,count(*) from  simulation_hive1_orc group by b order by b; """
        qt_hive1_orc_8_true """ select  * from  simulation_hive1_orc where a +b  = 11 ; """
        qt_hive1_orc_9_true """ select  * from  simulation_hive1_orc where a +b  != 11 ; """
        qt_hive1_orc_10_true """ select  * from  simulation_hive1_orc where a +b  != 11  and c = "keep"; """
        qt_hive1_orc_11_true """ select  * from  simulation_hive1_orc where a +b  != 11  and c != "keepxxx"; """
        qt_hive1_orc_12_true """ select  c from  simulation_hive1_orc order by c; """


        sql """ set hive_orc_use_column_names=false; """ 
        qt_hive1_orc_1_false """ select  * from  simulation_hive1_orc order by b; """
        qt_hive1_orc_2_false """ select  b,c,a from  simulation_hive1_orc order by b; """
        qt_hive1_orc_3_false """ select  c,a from  simulation_hive1_orc order by b; """
        qt_hive1_orc_4_false """ select  count(*) from  simulation_hive1_orc; """
        qt_hive1_orc_5_false """ select  count(a) from  simulation_hive1_orc; """
        qt_hive1_orc_6_false """ select  b from  simulation_hive1_orc order by b; """
        qt_hive1_orc_7_false """ select  b,count(*) from  simulation_hive1_orc group by b order by b; """
        qt_hive1_orc_8_false """ select  * from  simulation_hive1_orc where a +b  = 11 ; """
        qt_hive1_orc_9_false """ select  * from  simulation_hive1_orc where a +b  != 11 ; """
        qt_hive1_orc_10_false """ select  * from  simulation_hive1_orc where a +b  != 11  and c = "keep"; """
        qt_hive1_orc_11_false """ select  * from  simulation_hive1_orc where a +b  != 11  and c != "keepxxx"; """
        qt_hive1_orc_12_false """ select  c from  simulation_hive1_orc order by c; """


        sql """ set hive_orc_use_column_names=true; """ 
        qt_rename_orc_1_true """ select  * from test_hive_rename_column_orc order by new_b,c """;
        qt_rename_orc_2_true """ select  new_b from test_hive_rename_column_orc order by new_b,c """;
        qt_rename_orc_3_true """ select new_b,count(*) from test_hive_rename_column_orc   group by new_b order by new_b """;
        qt_rename_orc_4_true """ select * from test_hive_rename_column_orc where new_a = 1 order by new_b,c  """;
        qt_rename_orc_5_true """ select * from test_hive_rename_column_orc where new_d is not null order by new_b,c  """
        qt_rename_orc_6_true """ select * from test_hive_rename_column_orc where new_d is null order by new_b,c; """
        qt_rename_orc_7_true """ select * from test_hive_rename_column_orc where new_b + new_a = 31 order by new_b,c; """
        qt_rename_orc_8_true """ select new_a  from test_hive_rename_column_orc where new_a = 1 order by new_b,c; """
        qt_rename_orc_9_true """ select new_b  from test_hive_rename_column_orc where new_b = 1 order by new_b; """
        qt_rename_orc_10_true """ select new_b,new_d  from test_hive_rename_column_orc where new_d +30*new_b=100  order by new_b,c; """
        qt_rename_orc_11_true """ select new_b,new_a  from test_hive_rename_column_orc order by new_b,c,new_a;   """
        qt_rename_orc_12_true """ select f,new_d,c,new_b,new_a from test_hive_rename_column_orc order by new_b,c; """
        qt_rename_orc_13_true """ select * from test_hive_rename_column_orc where new_b + new_a != 31 order by new_b,c; """




        sql """ set hive_orc_use_column_names=false; """ 
        qt_rename_orc_1_false """ select  * from test_hive_rename_column_orc order by new_b,c """;
        qt_rename_orc_2_false """ select  new_b from test_hive_rename_column_orc order by new_b,c """;
        qt_rename_orc_3_false """ select new_b,count(*) from test_hive_rename_column_orc   group by new_b order by new_b """;
        qt_rename_orc_4_false """ select * from test_hive_rename_column_orc where new_a = 1 order by new_b,c  """;
        qt_rename_orc_5_false """ select * from test_hive_rename_column_orc where new_d is not null order by new_b  """
        qt_rename_orc_6_false """ select * from test_hive_rename_column_orc where new_d is null order by new_b,c; """
        qt_rename_orc_7_false """ select * from test_hive_rename_column_orc where new_b + new_a = 31 order by new_b,c; """
        qt_rename_orc_8_false """ select new_a  from test_hive_rename_column_orc where new_a = 1 order by new_b,c; """
        qt_rename_orc_9_false """ select new_b  from test_hive_rename_column_orc where new_b = 1 order by new_b; """
        qt_rename_orc_10_false """ select new_b,new_d  from test_hive_rename_column_orc where new_d +30*new_b=100  order by new_b,c;  """
        qt_rename_orc_11_false """ select new_b,new_a  from test_hive_rename_column_orc order by new_b,c,new_a;   """
        qt_rename_orc_12_false """ select f,new_d,c,new_b,new_a from test_hive_rename_column_orc order by new_b,c; """
        qt_rename_orc_13_false """ select * from test_hive_rename_column_orc where new_b + new_a != 31 order by new_b,c; """


        sql """ set hive_parquet_use_column_names=true; """ 
        qt_rename_parquet_1_true """ select  * from test_hive_rename_column_parquet order by new_b,c """;
        qt_rename_parquet_2_true """ select  new_b from test_hive_rename_column_parquet order by new_b,c """;
        qt_rename_parquet_3_true """ select new_b,count(*) from test_hive_rename_column_parquet   group by new_b order by new_b """;
        qt_rename_parquet_4_true """ select * from test_hive_rename_column_parquet where new_a = 1 order by new_b,c  """;
        qt_rename_parquet_5_true """ select * from test_hive_rename_column_parquet where new_d is not null order by new_b,c  """
        qt_rename_parquet_6_true """ select * from test_hive_rename_column_parquet where new_d is null order by new_b,c; """
        qt_rename_parquet_7_true """ select * from test_hive_rename_column_parquet where new_b + new_a = 31 order by new_b,c; """
        qt_rename_parquet_8_true """ select new_a  from test_hive_rename_column_parquet where new_a = 1 order by new_b,c; """
        qt_rename_parquet_9_true """ select new_b  from test_hive_rename_column_parquet where new_b = 1 order by new_b; """
        qt_rename_parquet_10_true """ select new_b,new_d  from test_hive_rename_column_parquet where new_d +30*new_b=100 order by new_b,c; """
        qt_rename_parquet_11_true """ select new_b,new_a  from test_hive_rename_column_parquet order by new_b,c,new_a;   """
        qt_rename_parquet_12_true """ select f,new_d,c,new_b,new_a from test_hive_rename_column_parquet order by new_b,c; """
        qt_rename_parquet_13_true """ select * from test_hive_rename_column_parquet where new_b + new_a != 31 order by new_b,c; """




        sql """ set hive_parquet_use_column_names=false; """ 
        qt_rename_parquet_1_false """ select  * from test_hive_rename_column_parquet order by new_b,c """;
        qt_rename_parquet_2_false """ select  new_b from test_hive_rename_column_parquet order by new_b,c """;
        qt_rename_parquet_3_false """ select new_b,count(*) from test_hive_rename_column_parquet   group by new_b order by new_b """;
        qt_rename_parquet_4_false """ select * from test_hive_rename_column_parquet where new_a = 1 order by new_b,c  """;
        qt_rename_parquet_5_false """ select * from test_hive_rename_column_parquet where new_d is not null order by new_b,c  """
        qt_rename_parquet_6_false """ select * from test_hive_rename_column_parquet where new_d is null order by new_b,c; """
        qt_rename_parquet_7_false """ select * from test_hive_rename_column_parquet where new_b + new_a = 31 order by new_b,c; """
        qt_rename_parquet_8_false """ select new_a  from test_hive_rename_column_parquet where new_a = 1 order by new_b,c; """
        qt_rename_parquet_9_false """ select new_b  from test_hive_rename_column_parquet where new_b = 1 order by new_b; """
        qt_rename_parquet_10_false """ select new_b,new_d  from test_hive_rename_column_parquet where new_d +30*new_b=100  order by new_b,c; """
        qt_rename_parquet_11_false """ select new_b,new_a  from test_hive_rename_column_parquet order by new_b,c,new_a;   """
        qt_rename_parquet_12_false """ select f,new_d,c,new_b,new_a from test_hive_rename_column_parquet order by new_b,c; """
        qt_rename_parquet_13_false """ select * from test_hive_rename_column_parquet where new_b + new_a != 31 order by new_b,c; """

        



    }
}
/*
CREATE TABLE  simulation_hive1_orc(
    `_col0` boolean,
    `_col1` INT,
    `_col2` STRING 
)stored as orc;
insert into simulation_hive1_orc values(true,10,"hello world"),(false,20,"keep");
select  * from simulation_hive1_orc; 
alter table simulation_hive1_orc change column `_col0` a boolean;
alter table simulation_hive1_orc change column `_col1` b int;
alter table simulation_hive1_orc change column `_col2` c string;
select  * from simulation_hive1_orc;
show create table simulation_hive1_orc;


CREATE TABLE  test_hive_rename_column_orc(
    a boolean,
    b INT,
    c STRING
)stored as orc;
insert into  test_hive_rename_column_orc values (true,10,"hello world"),(false,20,"keep");
alter table test_hive_rename_column_orc change column  a  new_a boolean;
alter table test_hive_rename_column_orc change column  b new_b int;
insert into  test_hive_rename_column_orc values (true,30,"abcd"),(false,40,"new adcd");
select  * from test_hive_rename_column_orc;
alter table test_hive_rename_column_orc add columns(d  int,f string);
insert into  test_hive_rename_column_orc values (true,50,"xxx",60,"cols"),(false,60,"yyy",100,"yyyyyy");
alter table test_hive_rename_column_orc change column  d new_d int;
insert into  test_hive_rename_column_orc values (true,70,"hahaha",8888,"abcd"),(false,80,"cmake",9999,"efg");
select  * from test_hive_rename_column_orc;
show create table test_hive_rename_column_orc;



CREATE TABLE  test_hive_rename_column_parquet(
    a boolean,
    b INT,
    c STRING
)stored as parquet;
insert into  test_hive_rename_column_parquet values (true,10,"hello world"),(false,20,"keep");
alter table test_hive_rename_column_parquet change column  a  new_a boolean;
alter table test_hive_rename_column_parquet change column  b new_b int;
insert into  test_hive_rename_column_parquet values (true,30,"abcd"),(false,40,"new adcd");
select  * from test_hive_rename_column_parquet;
alter table test_hive_rename_column_parquet add columns(d  int,f string);
insert into  test_hive_rename_column_parquet values (true,50,"xxx",60,"cols"),(false,60,"yyy",100,"yyyyyy");
alter table test_hive_rename_column_parquet change column  d new_d int;
insert into  test_hive_rename_column_parquet values (true,70,"hahaha",8888,"abcd"),(false,80,"cmake",9999,"efg");
select  * from test_hive_rename_column_parquet;
show create table test_hive_rename_column_parquet;
*/