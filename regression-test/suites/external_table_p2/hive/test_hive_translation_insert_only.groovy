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

suite("test_hive_translation_insert_only", "p2,external,hive,external_remote,external_remote_hive") {

    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    //hudi hive use same catalog in p2.
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable test")
        return;
    }

    String props = context.config.otherConfigs.get("hudiEmrCatalog")    
    String hms_catalog_name = "test_hive_translation_insert_only"

    sql """drop catalog if exists ${hms_catalog_name};"""
    sql """
        CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
        PROPERTIES ( 
            ${props}
            ,'hive.version' = '3.1.3'
        );
    """

    logger.info("catalog " + hms_catalog_name + " created")
    sql """switch ${hms_catalog_name};"""
    logger.info("switched to catalog " + hms_catalog_name)
    sql """ use regression;"""

    qt_1 """ select * from text_insert_only order by id """ 
    qt_2 """ select * from parquet_insert_only_major order by id """ 
    qt_3 """ select * from orc_insert_only_minor order by id """ 

    sql """drop catalog ${hms_catalog_name};"""
}


/*
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
create table text_insert_only (id INT, value STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ('transactional' = 'true',
'transactional_properties'='insert_only');
insert into text_insert_only values (1, 'A');
insert into text_insert_only values (2, 'B');
insert into text_insert_only values (3, 'C');
Load data xxx    (4,D)
create table parquet_insert_only_major (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS parquet
TBLPROPERTIES ('transactional' = 'true',
'transactional_properties'='insert_only');
insert into parquet_insert_only_major values (1, 'A');
insert into parquet_insert_only_major values (2, 'B');
insert into parquet_insert_only_major values (3, 'C');
ALTER TABLE parquet_insert_only_major COMPACT 'major';
insert into parquet_insert_only_major values (4, 'D');
insert into parquet_insert_only_major values (5, 'E');
create table orc_insert_only_minor (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
stored as orc
TBLPROPERTIES ('transactional' = 'true',
'transactional_properties'='insert_only');
insert into orc_insert_only_minor values (1, 'A');
insert into orc_insert_only_minor values (2, 'B');
insert into orc_insert_only_minor values (3, 'C');
ALTER TABLE orc_insert_only_minor COMPACT 'minor';
insert into orc_insert_only_minor values (4, 'D');
insert into orc_insert_only_minor values (5, 'E');
*/