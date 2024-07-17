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

suite("iceberg_position_delete", "p2,external,iceberg,external_remote,external_remote_iceberg") {

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {

        String catalog_name = "test_external_iceberg_position_delete"
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHdfsPort = context.config.otherConfigs.get("extHdfsPort")
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hadoop',
                'warehouse' = 'hdfs://${extHiveHmsHost}:${extHdfsPort}/usr/hive/warehouse/hadoop_catalog'
            );
        """

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """ use multi_catalog;""" 

        qt_gen_data_1 """ select * from iceberg_position_gen_data where  name = 'xyzxxxxxx' and id != 9;""" 
        qt_gen_data_2 """ select * from iceberg_position_gen_data where id = 1; """
        qt_gen_data_3 """ select * from iceberg_position_gen_data where id = 5; """ 
        qt_gen_data_4 """ select * from iceberg_position_gen_data where id = 10; """ 
        qt_gen_data_5 """ select * from iceberg_position_gen_data where id = 15; """ 
        qt_gen_data_6 """ select * from iceberg_position_gen_data where id = 2 limit 3;""" 
        qt_gen_data_7 """ select id from iceberg_position_gen_data where id = 2 limit 3;""" 
        qt_gen_data_8 """ select id,count(name) from iceberg_position_gen_data where id != 1 group by id order by id ;"""
        qt_gen_data_9 """ select id from iceberg_position_gen_data where id = 1; """
        qt_gen_data_10 """ select name from iceberg_position_gen_data where id = 5; """ 
        qt_gen_data_11 """ select id from iceberg_position_gen_data where id = 10; """ 
        qt_gen_data_12 """ select name from iceberg_position_gen_data where id = 15;""" 
        qt_gen_data_13 """ select * from iceberg_position_gen_data where id = 15 and name = 'select xxxxxxxxx';""" 
        qt_gen_data_14 """ select * from iceberg_position_gen_data where id = 2 and name = 'select xxxxxxxxx' limit 3;""" 
        qt_gen_data_15 """ select * from iceberg_position_gen_data where id = 7 and name = '12345xxx' limit 3;""" 
        qt_gen_data_16 """ select * from iceberg_position_gen_data where  name = 'hello world' ;""" 
        qt_gen_data_17 """ select name from iceberg_position_gen_data where  name = 'hello world' ;""" 
        qt_gen_data_18 """ select id from iceberg_position_gen_data where  name = 'hello world' ;""" 
        qt_gen_data_19 """ select count(*) from iceberg_position_gen_data where  name != 'final entryxxxxxx' ;""" 
        qt_gen_data_20 """ select count(*) from iceberg_position_gen_data; """ 


        qt_orc_1 """ select * from iceberg_position_orc where  name = 'xyzxxxxxx' and id != 9;""" 
        qt_orc_2 """ select * from iceberg_position_orc where id = 1; """
        qt_orc_3 """ select * from iceberg_position_orc where id = 5; """ 
        qt_orc_4 """ select * from iceberg_position_orc where id = 10; """ 
        qt_orc_5 """ select * from iceberg_position_orc where id = 15; """ 
        qt_orc_6 """ select * from iceberg_position_orc where id = 2 limit 3;""" 
        qt_orc_7 """ select id from iceberg_position_orc where id = 2 limit 3;""" 
        qt_orc_8 """ select id,count(name) from iceberg_position_orc where id != 1 group by id order by id ;"""
        qt_orc_9 """ select id from iceberg_position_orc where id = 1; """
        qt_orc_10 """ select name from iceberg_position_orc where id = 5; """ 
        qt_orc_11 """ select id from iceberg_position_orc where id = 10; """ 
        qt_orc_12 """ select name from iceberg_position_orc where id = 15;""" 
        qt_orc_13 """ select * from iceberg_position_orc where id = 15 and name = 'select xxxxxxxxx';""" 
        qt_orc_14 """ select * from iceberg_position_orc where id = 2 and name = 'select xxxxxxxxx' limit 3;""" 
        qt_orc_15 """ select * from iceberg_position_orc where id = 7 and name = '12345xxx' limit 3;""" 
        qt_orc_16 """ select * from iceberg_position_orc where  name = 'hello world' ;""" 
        qt_orc_17 """ select name from iceberg_position_orc where  name = 'hello world' ;""" 
        qt_orc_18 """ select id from iceberg_position_orc where  name = 'hello world' ;""" 
        qt_orc_19 """ select count(*) from iceberg_position_orc where  name != 'final entryxxxxxx' ;""" 
        qt_orc_20 """ select count(*) from iceberg_position_orc; """ 

        qt_parquet_1 """ select * from iceberg_position_parquet where  name = 'xyzxxxxxx' and id != 9;""" 
        qt_parquet_2 """ select * from iceberg_position_parquet where id = 1; """
        qt_parquet_3 """ select * from iceberg_position_parquet where id = 5; """ 
        qt_parquet_4 """ select * from iceberg_position_parquet where id = 10; """ 
        qt_parquet_5 """ select * from iceberg_position_parquet where id = 15; """ 
        qt_parquet_6 """ select * from iceberg_position_parquet where id = 2 limit 3;""" 
        qt_parquet_7 """ select id from iceberg_position_parquet where id = 2 limit 3;""" 
        qt_parquet_8 """ select id,count(name) from iceberg_position_parquet where id != 1 group by id order by id ;"""
        qt_parquet_9 """ select id from iceberg_position_parquet where id = 1; """
        qt_parquet_10 """ select name from iceberg_position_parquet where id = 5; """ 
        qt_parquet_11 """ select id from iceberg_position_parquet where id = 10; """ 
        qt_parquet_12 """ select name from iceberg_position_parquet where id = 15;""" 
        qt_parquet_13 """ select * from iceberg_position_parquet where id = 15 and name = 'select xxxxxxxxx';""" 
        qt_parquet_14 """ select * from iceberg_position_parquet where id = 2 and name = 'select xxxxxxxxx' limit 3;""" 
        qt_parquet_15 """ select * from iceberg_position_parquet where id = 7 and name = '12345xxx' limit 3;""" 
        qt_parquet_16 """ select * from iceberg_position_parquet where  name = 'hello world' ;""" 
        qt_parquet_17 """ select name from iceberg_position_parquet where  name = 'hello world' ;""" 
        qt_parquet_18 """ select id from iceberg_position_parquet where  name = 'hello world' ;""" 
        qt_parquet_19 """ select count(*) from iceberg_position_parquet where  name != 'final entryxxxxxx' ;""" 
        qt_parquet_20 """ select count(*) from iceberg_position_parquet; """ 


        List<List<Object>> iceberg_position_orc = sql """ select * from iceberg_position_orc ;"""
        List<List<Object>> iceberg_position_parquet = sql """ select * from iceberg_position_parquet;"""
        List<List<Object>> iceberg_position_gen = sql """ select * from iceberg_position_gen_data;"""

        assertTrue(iceberg_position_orc.size() == iceberg_position_gen.size())
        assertTrue(iceberg_position_orc.size() == iceberg_position_parquet.size())
        assertTrue(iceberg_position_orc.size() == 5632)


        List<List<Object>> iceberg_position_orc_1 = sql  """select * from iceberg_position_orc where id != 1;"""
        List<List<Object>> iceberg_position_orc_2 = sql """select * from iceberg_position_orc where name != "hello word" ;"""
        List<List<Object>> iceberg_position_orc_3 = sql """select id from iceberg_position_orc where id != 1;"""
        List<List<Object>> iceberg_position_orc_4 = sql """select name from iceberg_position_orc where id != 1;"""
        List<List<Object>> iceberg_position_orc_5 = sql """select name from iceberg_position_orc where name != "hello word" ;"""
        List<List<Object>> iceberg_position_orc_6 = sql """select id from iceberg_position_orc where name != "hello word" ;"""
        List<List<Object>> iceberg_position_orc_7 = sql """select * from iceberg_position_orc where id != 1 and name != "33333";"""
        assertTrue(iceberg_position_orc_1.size() == 5632)
        assertTrue(iceberg_position_orc_2.size() == 5632)
        assertTrue(iceberg_position_orc_3.size() == 5632)
        assertTrue(iceberg_position_orc_4.size() == 5632)
        assertTrue(iceberg_position_orc_5.size() == 5632)
        assertTrue(iceberg_position_orc_6.size() == 5632)
        assertTrue(iceberg_position_orc_7.size() == 5632)


        List<List<Object>> iceberg_position_gen_1 = sql """select * from iceberg_position_gen_data where id != 1 and name != "hello";"""
        assertTrue(iceberg_position_gen_1.size() == 5632)

        List<List<Object>> iceberg_position_gen_2 = sql """select * from iceberg_position_gen_data where id != 2;"""
        assertTrue(iceberg_position_gen_2.size() == 5120)
        
        List<List<Object>> iceberg_position_gen_22 = sql """select * from iceberg_position_gen_data where id != 5;"""
        assertTrue(iceberg_position_gen_22.size() == 5632)

        List<List<Object>> iceberg_position_gen_3 = sql """select * from iceberg_position_gen_data where name != "hello word" ;"""
        assertTrue(iceberg_position_gen_3.size() == 5632)
        
        List<List<Object>> iceberg_position_gen_4 = sql """select id from iceberg_position_gen_data where id != 2;"""
        assertTrue(iceberg_position_gen_4.size() == 5120)
        
        List<List<Object>> iceberg_position_gen_44 = sql """select id from iceberg_position_gen_data where id != 5;"""
        assertTrue(iceberg_position_gen_44.size() == 5632)
        
        List<List<Object>> iceberg_position_gen_5 = sql """select name from iceberg_position_gen_data where id != 2;"""
        assertTrue(iceberg_position_gen_5.size() == 5120)
        
        List<List<Object>> iceberg_position_gen_55 = sql """select name from iceberg_position_gen_data where id != 5;"""
        assertTrue(iceberg_position_gen_55.size() == 5632)
        
        List<List<Object>> iceberg_position_gen_6 = sql """select name from iceberg_position_gen_data where name != "hello wordxx" ;"""
        assertTrue(iceberg_position_gen_6.size() == 5632)
        
        List<List<Object>> iceberg_position_gen_7 = sql """select id from iceberg_position_gen_data where name != "hello word" ;"""
        assertTrue(iceberg_position_gen_7.size() == 5632)

        sql """drop catalog ${catalog_name}"""
    }
}
/*


create table iceberg_position_gen_data(
    id int,
    name string
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);

INSERT INTO iceberg_position_gen_data VALUES
(1, "hello world"),
(2, "select xxxxxxxxx"),
(3, "example xxxx"),
(4, "more dataxxx"),
(5, "another examplexxx"),
(6, "testxxx"),
(7, "12345xxx"),
(8, "abcdefxxxx"),
(9, "xyzxxxxxx"),
(10, "inserted dataxxxxx"),
(11, "SQLxxxxx"),
(12, "tablexxxx"),
(13, "rowxxxx"),
(14, "data entryxxxx"),
(15, "final entryxxxxxx");
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;
insert into iceberg_position_gen_data select * from iceberg_position_gen_data;



create table iceberg_position_parquet(
    id int,
    name string
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);
create table iceberg_position_orc(
    id int,
    name string
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);

insert into iceberg_position_parquet select * from iceberg_position_gen_data; 
insert into iceberg_position_orc select * from iceberg_position_parquet;


delete from iceberg_position_gen_data where id = 1;
delete from iceberg_position_gen_data where id = 5;
delete from iceberg_position_gen_data where id = 10;
delete from iceberg_position_gen_data where id = 15;

delete from iceberg_position_parquet where id = 1;
delete from iceberg_position_parquet where id = 5;
delete from iceberg_position_parquet where id = 10;
delete from iceberg_position_parquet where id = 15;

delete from iceberg_position_orc where id = 1;
delete from iceberg_position_orc where id = 5;
delete from iceberg_position_orc where id = 10;
delete from iceberg_position_orc where id = 15;
*/


