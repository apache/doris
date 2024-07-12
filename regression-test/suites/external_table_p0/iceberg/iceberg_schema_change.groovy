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

suite("iceberg_schema_change", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    // TODO 找当时的人看下怎么构造的这个表
    return

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_schema_change"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """ use multi_catalog;""" 


        qt_parquet_v1_1  """ desc complex_parquet_v1_schema_change ;""" 
        qt_parquet_v1_2  """ select * from  complex_parquet_v1_schema_change order by id; """ 
        qt_parquet_v1_3  """ select count(*) from  complex_parquet_v1_schema_change ;""" 
        qt_parquet_v1_4  """ select id from  complex_parquet_v1_schema_change where col_add +1 = col_add2 order by id;""" 
        qt_parquet_v1_5  """ select col_add from  complex_parquet_v1_schema_change where col_add is not null   order by col_add ; """ 
        qt_parquet_v1_6  """ select id from  complex_parquet_v1_schema_change  where col_add + 5 = id order by id; """ 
        qt_parquet_v1_7  """ select rename_col7 from  complex_parquet_v1_schema_change order by id; """ 
        qt_parquet_v1_8  """ select col_add2 from  complex_parquet_v1_schema_change  where id >=7 order by id; """ 
        qt_parquet_v1_9  """ select id,count(col_add) from  complex_parquet_v1_schema_change  group by id order by id desc ; """ 
        qt_parquet_v1_10  """ select col_add from  complex_parquet_v1_schema_change where col_add -1 = col_add2 order by id; """ 



        qt_parquet_v2_1  """ desc complex_parquet_v2_schema_change ;""" 
        qt_parquet_v2_2  """ select * from  complex_parquet_v2_schema_change order by id; """ 
        qt_parquet_v2_3  """ select count(*) from  complex_parquet_v2_schema_change ;""" 
        qt_parquet_v2_4  """ select id from  complex_parquet_v2_schema_change where col_add +1 = col_add2 order by id;""" 
        qt_parquet_v2_5  """ select col_add from  complex_parquet_v2_schema_change where col_add is not null   order by col_add ; """ 
        qt_parquet_v2_6  """ select id from  complex_parquet_v2_schema_change  where col_add + 5 = id order by id; """ 
        qt_parquet_v2_7  """ select rename_col7 from  complex_parquet_v2_schema_change order by id; """ 
        qt_parquet_v2_8  """ select col_add2 from  complex_parquet_v2_schema_change  where id >=7 order by id; """ 
        qt_parquet_v2_9  """ select id,count(col_add) from  complex_parquet_v2_schema_change  group by id order by id desc ; """ 
        qt_parquet_v2_10  """ select col_add from  complex_parquet_v2_schema_change where col_add -1 = col_add2 order by id; """ 




        qt_orc_v1_1  """ desc complex_orc_v1_schema_change ;""" 
        qt_orc_v1_2  """ select * from  complex_orc_v1_schema_change order by id; """ 
        qt_orc_v1_3  """ select count(*) from  complex_orc_v1_schema_change ;""" 
        qt_orc_v1_4  """ select id from  complex_orc_v1_schema_change where col_add +1 = col_add2 order by id;""" 
        qt_orc_v1_5  """ select col_add from  complex_orc_v1_schema_change where col_add is not null   order by col_add ; """ 
        qt_orc_v1_6  """ select id from  complex_orc_v1_schema_change  where col_add + 5 = id order by id; """ 
        qt_orc_v1_7  """ select rename_col7 from  complex_orc_v1_schema_change order by id; """ 
        qt_orc_v1_8  """ select col_add2 from  complex_orc_v1_schema_change  where id >=7 order by id; """ 
        qt_orc_v1_9  """ select id,count(col_add) from  complex_orc_v1_schema_change  group by id order by id desc ; """ 
        qt_orc_v1_10  """ select col_add from  complex_orc_v1_schema_change where col_add -1 = col_add2 order by id; """ 

        

        qt_orc_v2_1  """ desc complex_orc_v2_schema_change ;""" 
        qt_orc_v2_2  """ select * from  complex_orc_v2_schema_change order by id; """ 
        qt_orc_v2_3  """ select count(*) from  complex_orc_v2_schema_change ;""" 
        qt_orc_v2_4  """ select id from  complex_orc_v2_schema_change where col_add +1 = col_add2 order by id;""" 
        qt_orc_v2_5  """ select col_add from  complex_orc_v2_schema_change where col_add is not null   order by col_add ; """ 
        qt_orc_v2_6  """ select id from  complex_orc_v2_schema_change  where col_add + 5 = id order by id; """ 
        qt_orc_v2_7  """ select rename_col7 from  complex_orc_v2_schema_change order by id; """ 
        qt_orc_v2_8  """ select col_add2 from  complex_orc_v2_schema_change  where id >=7 order by id; """ 
        qt_orc_v2_9  """ select id,count(col_add) from  complex_orc_v2_schema_change  group by id order by id desc ; """ 
        qt_orc_v2_10  """ select col_add from  complex_orc_v2_schema_change where col_add -1 = col_add2 order by id; """ 

}
/*
before schema: 
    id int,
    col1 array<int>,
    col2 array<float>,
    col3 array<decimal(12,4)>,
    col4 map<int,int>,
    col5 map<int,float>,
    col6 map<int,decimal(12,5)>,
    col7 struct<x:int,y:float,z:decimal(12,5)>,
    col8 int,
    col9 float,
    col10 decimal(12,5),
    col_del int


ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col1.element type bigint;
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col2.element type double;
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col3.element type decimal(20,4);
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col4.value type bigint;
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col5.value type double;
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col6.value type decimal(20,5);
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col7.x type bigint;
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col7.y type double;
ALTER TABLE complex_parquet_v2_schema_change CHANGE COLUMN col7.z type decimal(20,5);
alter table complex_parquet_v2_schema_change CHANGE COLUMN col8 col8 bigint;
alter table complex_parquet_v2_schema_change CHANGE COLUMN col9 col9 double;
alter table complex_parquet_v2_schema_change CHANGE COLUMN col10 col10 decimal(20,5);
alter table complex_parquet_v2_schema_change drop column col7.z;
alter table complex_parquet_v2_schema_change add column col7.add double;
alter table complex_parquet_v2_schema_change change column col7.add first;
alter table complex_parquet_v2_schema_change rename COLUMN col1 to rename_col1;
alter table complex_parquet_v2_schema_change rename COLUMN col2 to rename_col2;
alter table complex_parquet_v2_schema_change rename COLUMN col3 to rename_col3;
alter table complex_parquet_v2_schema_change rename COLUMN col4 to rename_col4;
alter table complex_parquet_v2_schema_change rename COLUMN col5 to rename_col5;
alter table complex_parquet_v2_schema_change rename COLUMN col6 to rename_col6;
alter table complex_parquet_v2_schema_change rename COLUMN col7 to rename_col7;
alter table complex_parquet_v2_schema_change rename COLUMN col8 to rename_col8;
alter table complex_parquet_v2_schema_change rename COLUMN col9 to rename_col9;
alter table complex_parquet_v2_schema_change rename COLUMN col10 to rename_col10;
alter table complex_parquet_v2_schema_change drop column col_del;
alter table complex_parquet_v2_schema_change CHANGE COLUMN rename_col8 first;
alter table complex_parquet_v2_schema_change CHANGE COLUMN rename_col9 after rename_col8;
alter table complex_parquet_v2_schema_change CHANGE COLUMN rename_col10 after rename_col9;
alter table complex_parquet_v2_schema_change add column col_add int;
alter table complex_parquet_v2_schema_change add column col_add2 int;

after schema:
    rename_col8             bigint                                      
    rename_col9             double                                      
    rename_col10            decimal(20,5)                               
    id                      int                                         
    rename_col1             array<bigint>                               
    rename_col2             array<double>                               
    rename_col3             array<decimal(20,4)>                        
    rename_col4             map<int,bigint>                             
    rename_col5             map<int,double>                             
    rename_col6             map<int,decimal(20,5)>                      
    rename_col7             struct<add:double,x:bigint,y:double>                        
    col_add                 int                                         
    col_add2                int                                         
                                
*/
