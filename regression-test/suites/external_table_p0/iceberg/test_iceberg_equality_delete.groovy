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

suite("test_iceberg_equality_delete", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_equality_delete"

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
        sql """ use test_db;""" 

        // one delete column
        qt_one_delete_column """select * from customer_flink_one order by id"""
        qt_one_delete_column_orc """select * from customer_flink_one_orc order by id"""
        qt_count1 """select count(*) from  customer_flink_one"""
        qt_count1_orc """select count(*) from  customer_flink_one_orc"""
        qt_max1 """select max(val) from customer_flink_one"""
        qt_max1_orc """select max(val) from customer_flink_one_orc"""

        // three delete columns
        qt_one_delete_column """select * from customer_flink_three order by id1"""
        qt_one_delete_column_orc """select * from customer_flink_three_orc order by id1"""
        qt_count3 """select count(*) from  customer_flink_three"""
        qt_count3_orc """select count(*) from  customer_flink_three_orc"""
        qt_max3 """select max(val) from customer_flink_three"""
        qt_max3_orc """select max(val) from customer_flink_three_orc"""

        sql """drop catalog ${catalog_name}"""
}

/*

---- flink sql:

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export S3_PATH_STYLE_ACCESS=true
export S3_ENDPOINT=http://172.21.0.101:19001

CREATE CATALOG iceberg_rest WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri'='http://172.21.0.101:18181',
    'warehouse'='s3a://warehouse/wh',
    's3.endpoint' = 'http://172.21.0.101:19001'
);

use iceberg_rest.test_db;

create table customer_flink_one (
    id int,
    val string,
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'format-version'='2',
    'write-format'='parquet',
    'upsert-enabled'='true'
);

insert into customer_flink_one values (1,'a');
insert into customer_flink_one values (2,'b');
insert into customer_flink_one values (1,'b');
insert into customer_flink_one values (2, 'a'),(2, 'c');


create table customer_flink_one_orc (
    id int,
    val string,
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'format-version'='2',
    'write-format'='orc',
    'upsert-enabled'='true'
);

insert into customer_flink_one_orc values (1,'a');
insert into customer_flink_one_orc values (2,'b');
insert into customer_flink_one_orc values (1,'b');
insert into customer_flink_one_orc values (2, 'a'),(2, 'c');


create table customer_flink_three (
    id1 int,
    id2 string,
    id3 int,
    val string,
    PRIMARY KEY(`id1`,`id2`,`id3`) NOT ENFORCED
) WITH (
    'format-version'='2',
    'write-format'='parquet',
    'upsert-enabled'='true'
);
insert into customer_flink_three values (1,'id2',1,'a');
insert into customer_flink_three values (1,'id2',1,'b');
insert into customer_flink_three values (2,'id2',1,'b');
insert into customer_flink_three values (2,'id2',1,'a'),(2,'id2',1,'c');

create table customer_flink_three_orc (
    id1 int,
    id2 string,
    id3 int,
    val string,
    PRIMARY KEY(`id1`,`id2`,`id3`) NOT ENFORCED
) WITH (
    'format-version'='2',
    'write-format'='orc',
    'upsert-enabled'='true'
);

insert into customer_flink_three_orc values (1,'id2',1,'a');
insert into customer_flink_three_orc values (1,'id2',1,'b');
insert into customer_flink_three_orc values (2,'id2',1,'b');
insert into customer_flink_three_orc values (2,'id2',1,'a'),(2,'id2',1,'c');

*/
