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

suite("iceberg_read_unitycatalog_table", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "iceberg_read_unitycatalog_table"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
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
    sql """ use ${catalog_name}.test_db """
    String tb = "unitycatalog_marksheet_uniform"

    qt_q1  """ select * from ${tb} order by c1 """ 
    qt_q2  """ select c1 from ${tb} where c1 > 6 order by c1 """ 
    qt_q3  """ select c2, c3 from ${tb} where c3 > 200 order by c1 """ 

}

/*

spark-sql:
    1. create table marksheet_uniform (c1 int, c2 string, c3 int);
    2. get parquet file from marksheet_uniform; (ref: https://docs.unitycatalog.io/usage/tables/uniform/)
    3. put parquet file to hdfs: hdfs dfs -put <parquet_file> hdfs://xxxxx
    4. CALL <catalog_name>.system.add_files(
            table => '<catalog_name>.unitycatalog_db.marksheet_uniform',
            source_table => '`parquet`.`hdfs://172.20.32.136:8020/user/doris/preinstalled_data/iceberg_hadoop_warehouse/unitycatalog_db/marksheet_uniform_data/part-00000-5af50cc4-3218-465b-a3a4-eb4fc709421d-c000.snappy.parquet`'
        );
*/