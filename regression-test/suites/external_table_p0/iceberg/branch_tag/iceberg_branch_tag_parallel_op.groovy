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

suite("iceberg_branch_tag_parallel_op", "p2,external,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_tag_parallel"
    String db_name = "test_parallel_op"
    String table_name = "test_branch_tag_operate"

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

    sql """drop database if exists ${catalog_name}.${db_name} force"""
    sql """create database ${catalog_name}.${db_name}"""
    sql """ use ${catalog_name}.${db_name} """
    sql """drop table if exists ${table_name}"""
    sql """create table ${table_name} (id int, name string)"""
    sql """insert into ${table_name} values (1, 'name_1')"""

    def futures = []
    for (int i = 0; i < 10; i++) {
        int idx = i
        futures.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            sql """alter table ${table_name} create branch branch_${idx} """
        })
    }
    def combineFuture = combineFutures(futures)
    combineFuture.get()
    for (int i = 0; i < 10; i++) {
        sql """use ${catalog_name}.${db_name}"""
        "qt_select_branch_${i}" """select * from ${table_name}@branch(branch_${i}) """
    }

    def futures1 = []
    for (int i = 0; i < 10; i++) {
        int idx = i
        futures1.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            sql """insert into ${table_name}@branch(branch_${idx}) values (${idx + 2}, 'name_${idx + 2}') """
        })
    }
    def combineFuture1 = combineFutures(futures1)
    combineFuture1.get()
    for (int i = 0; i < 10; i++) {
        sql """use ${catalog_name}.${db_name}"""
        "qt_select_branch_after_insert_${i}" """select * from ${table_name}@branch(branch_${i}) order by id"""
    }

    def futures2 = []
    for (int i = 0; i < 10; i++) {
        int idx = i
        futures2.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            sql """alter table ${table_name} drop branch branch_${idx} """
        })
    }
    def combineFuture2 = combineFutures(futures2)
    combineFuture2.get()


    def futures3 = []
    for (int i = 0; i < 10; i++) {
        int idx = i
        futures3.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            sql """alter table ${table_name} create tag tag_${idx} """
        })
    }
    def combineFuture3 = combineFutures(futures3)
    combineFuture3.get()
    for (int i = 0; i < 10; i++) {
        sql """use ${catalog_name}.${db_name}"""
        "qt_select_tag_${i}" """select * from ${table_name}@tag(tag_${i}) """
    }
    def futures4 = []
    for (int i = 0; i < 10; i++) {
        int idx = i
        futures4.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            sql """alter table ${table_name} drop tag tag_${idx} """
        })
    }
    def combineFuture4 = combineFutures(futures4)
    combineFuture4.get()

    def futures5 = []
    for (int i = 0; i < 10; i++) {
        futures5.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} create branch branch_same_name """
                qt_select_branch_same_name_1 """select * from ${table_name}@branch(branch_same_name) """
            } catch (Exception e) {
                logger.info("Caught exception: " + e.getMessage())
                assertTrue(e.getMessage().contains("Ref branch_same_name already exists"), "Exception message should contain 'Ref branch_same_name already exists'")
            }
        })
    }
    def combineFuture5 = combineFutures(futures5)
    combineFuture5.get()

    def futures6 = []
    for (int i = 0; i < 10; i++) {
        futures6.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} create tag tag_same_name """
                qt_select_tag_same_name_1 """select * from ${table_name}@tag(tag_same_name) """
            } catch (Exception e) {
                logger.info("Caught exception: " + e.getMessage())
                assertTrue(e.getMessage().contains("Ref tag_same_name already exists"), "Exception message should contain 'Ref tag_same_name already exists'")
            }
        })
    }
    def combineFuture6 = combineFutures(futures6)
    combineFuture6.get()

}

