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

suite("iceberg_complex_type", "p2,external,iceberg,external_remote,external_remote_iceberg") {

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {

        String catalog_name = "test_external_iceberg_complex_type"
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



        qt_parquet_v1_1  """ desc complex_parquet_v1 ;""" 
        qt_parquet_v1_2  """ select * from  complex_parquet_v1 order by id; """ 
        qt_parquet_v1_3  """ select count(*) from  complex_parquet_v1 ;"""
        qt_parquet_v1_4  """ select array_size(col2) from  complex_parquet_v1 where col2 is not null   order by id ; """ 
        qt_parquet_v1_5  """ select map_keys(col3) from  complex_parquet_v1  order by id; """ 
        qt_parquet_v1_6  """ select struct_element(col4, 1) from  complex_parquet_v1  where id >=7 order by id; """ 
        qt_parquet_v1_7  """ select id,count(col2) from  complex_parquet_v1  group by id order by id desc limit 2; """ 


        qt_parquet_v2_1  """ desc complex_parquet_v2 ;""" 
        qt_parquet_v2_2  """ select * from  complex_parquet_v2 order by id; """ 
        qt_parquet_v2_3  """ select count(*) from  complex_parquet_v2 ;"""
        qt_parquet_v2_4  """ select array_size(col2) from  complex_parquet_v2 where col2 is not null   order by id ; """ 
        qt_parquet_v2_5  """ select map_keys(col3) from  complex_parquet_v2  order by id; """ 
        qt_parquet_v2_6  """ select struct_element(col4, 1) from  complex_parquet_v2  where id >=7 order by id; """ 
        qt_parquet_v2_7  """ select id,count(col2) from  complex_parquet_v2  group by id order by id desc limit 2; """ 


        qt_orc_v1_1  """ desc complex_orc_v1 ;""" 
        qt_orc_v1_2  """ select * from  complex_orc_v1 order by id; """ 
        qt_orc_v1_3  """ select count(*) from  complex_orc_v1 ;"""
        qt_orc_v1_4  """ select array_size(col2) from  complex_orc_v1 where col2 is not null   order by id ; """ 
        qt_orc_v1_5  """ select map_keys(col3) from  complex_orc_v1  order by id; """ 
        qt_orc_v1_6  """ select struct_element(col4, 1) from  complex_orc_v1  where id >=7 order by id; """ 
        qt_orc_v1_7  """ select id,count(col2) from  complex_orc_v1  group by id order by id desc limit 2; """ 


        qt_orc_v2_1  """ desc complex_orc_v2 ;""" 
        qt_orc_v2_2  """ select * from  complex_orc_v2 order by id; """ 
        qt_orc_v2_3  """ select count(*) from  complex_orc_v2 ;"""
        qt_orc_v2_4  """ select array_size(col2) from  complex_orc_v2 where col2 is not null   order by id ; """ 
        qt_orc_v2_5  """ select map_keys(col3) from  complex_orc_v2  order by id; """ 
        qt_orc_v2_6  """ select struct_element(col4, 1) from  complex_orc_v2  where id >=7 order by id; """ 
        qt_orc_v2_7  """ select id,count(col2) from  complex_orc_v2  group by id order by id desc limit 2; """ 




    }
}

/*
schema :
    id                      int                                         
    col2                    array<array<array<array<array<int>>>>>                      
    col3                    map<array<float>,map<int,map<int,float>>>                           
    col4                    struct<x:array<int>,y:array<double>,z:map<boolean,string>>                          
    col5                    map<int,map<int,map<int,map<int,map<float,map<double,struct<x:int,y:array<double>>>>>>>>                            
    col6                    struct<xx:array<int>,yy:array<map<double,float>>,zz:struct<xxx:struct<xxxx:struct<xxxxx:decimal(13,2)>>>>

*/