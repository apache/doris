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

suite("iceberg_branch_insert_data", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_insert_data"

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

    sql """drop database if exists ${catalog_name}.iceberg_branch_insert_data_db force"""
    sql """create database ${catalog_name}.iceberg_branch_insert_data_db"""
    sql """ use ${catalog_name}.iceberg_branch_insert_data_db """

    String tmp_tb1 = catalog_name + "_tmp_tb1"
    String tmp_tb2 = catalog_name + "_tmp_tb2"
    // create an unpartitioned table
    sql """ drop table if exists ${tmp_tb1} """
    sql """ create table ${tmp_tb1} (id int, par string) """

    // create a partitioned table
    sql """ drop table if exists ${tmp_tb2} """
    sql """ create table ${tmp_tb2} (id int, par string) PARTITION BY LIST(par)() """

    sql """ insert into ${tmp_tb1} values (1,'a'),(2,'a'),(3,'b'),(4,'b') """;
    sql """ insert into ${tmp_tb2} values (1,'a'),(2,'a'),(3,'b'),(4,'b') """;

    sql """ alter table ${tmp_tb1} create tag t1 """
    sql """ alter table ${tmp_tb1} create branch b1 """
    sql """ alter table ${tmp_tb1} create branch b2 """

    sql """ alter table ${tmp_tb2} create tag t1 """
    sql """ alter table ${tmp_tb2} create branch b1 """
    sql """ alter table ${tmp_tb2} create branch b2 """

    // error with not exists branch
    test {
        sql """ insert into ${tmp_tb1}@branch(brrrr1) values (5,'a') """
        exception "brrrr1 is not founded"
    }
    test {
        sql """ insert into ${tmp_tb2}@branch(brrrr2) values (5,'a') """
        exception "brrrr2 is not founded"
    }

    // error with tag
    test {
        sql """ insert into ${tmp_tb1}@branch(t1) values (5,'a') """
        exception "t1 is a tag, not a branch"
    }
    test {
        sql """ insert into ${tmp_tb2}@branch(t1) values (5,'a') """
        exception "t1 is a tag, not a branch"
    }

    def execute = { table_name ->

        def query_all = {
            order_qt_main """ select * from ${table_name} order by id """
            order_qt_t1 """ select * from ${table_name}@tag(t1) order by id """
            order_qt_b1 """ select * from ${table_name}@branch(b1) order by id """
            order_qt_b2 """ select * from ${table_name}@branch(b2) order by id """
        }

        query_all()

        sql """ insert into ${table_name}@branch(b1) values (5,'a'),(6,'a') """
        // 1,2,3,4
        // 1,2,3,4
        // 1,2,3,4,5,6
        // 1,2,3,4
        query_all()

        sql """ insert into ${table_name} values (7,'c'),(8,'c') """
        // 1,2,3,4,7,8
        // 1,2,3,4
        // 1,2,3,4,5,6
        // 1,2,3,4
        query_all()

        sql """ insert overwrite table ${table_name} values (9,'a'),(10,'b') """
        // Non-partitioned table: 9,10 Partitioned table: 7,8,9,10
        // 1,2,3,4
        // 1,2,3,4,5,6
        // 1,2,3,4
        query_all()

        sql """ insert overwrite table ${table_name}@branch(b1) values (11,'a'), (12,'b') """
        // Non-partitioned table: 9,10 Partitioned table: 7,8,9,10
        // 1,2,3,4
        // 11,12
        // 1,2,3,4
        query_all()

        sql """ insert into ${table_name}@branch(b1) select * from ${table_name} """
        // Non-partitioned table: 9,10 Partitioned table: 7,8,9,10
        // 1,2,3,4
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // 1,2,3,4
        query_all()

        sql """ insert overwrite table ${table_name}@branch(b2) select * from ${table_name} """
        // Non-partitioned table: 9,10 Partitioned table: 7,8,9,10
        // 1,2,3,4
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // Non-partitioned table: 9,10 Partitioned table: 7,8,9,10
        query_all()

        sql """ insert overwrite table ${table_name}@branch(b2) select * from ${table_name}@branch(b1) """
        // Non-partitioned table: 9,10 Partitioned table: 7,8,9,10
        // 1,2,3,4
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        query_all()

        sql """ insert overwrite table ${table_name} select * from ${table_name}@branch(b1) """
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // 1,2,3,4
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        query_all()

        sql """ insert overwrite table ${table_name}@branch(b2) select * from ${table_name}@tag(t1) """
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // 1,2,3,4
        // Non-partitioned table: 9,10,11,12 Partitioned table: 7,8,9,10,11,12
        // Non-partitioned table: 1,2,3,4 Partitioned table: 1,2,3,4,7,8
        query_all()

        // insert with columns
        sql """ insert into ${table_name}@branch(b1)(par) values('part');"""
        query_all()
        sql """ insert overwrite table ${table_name}@branch(b1)(par) values('part2');"""
        query_all()
        
    }

    execute(tmp_tb1)
    execute(tmp_tb2)

    // test insert table which not support branch
    sql """switch internal"""
    sql """drop database if exists iceberg_branch_insert_data_internal_db"""
    sql """create database iceberg_branch_insert_data_internal_db"""
    sql """use iceberg_branch_insert_data_internal_db"""
    sql """create table iceberg_branch_insert_data_internal_tb (id int, par string) properties("replication_num" = "1")"""

    test {
        sql """insert into iceberg_branch_insert_data_internal_tb@branch(b1) values (1,'a'),(2,'a'),(3,'b'),(4,'b')"""
        exception "Only support insert data into iceberg table's branch"
    }

    test {
        sql """insert overwrite table iceberg_branch_insert_data_internal_tb@branch(b1) values (1,'a'),(2,'a'),(3,'b'),(4,'b')"""
        exception "Only support insert overwrite into iceberg table's branch"
    }
}


