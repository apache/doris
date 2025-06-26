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

suite("test_iceberg_case_sensibility", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
            String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get("hive2HmsPort")
            String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
            String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
            String warehouse = "${default_fs}/warehouse"

            // rest
            for (String case_type : ["0", "1", "2"]) {
                sql """drop catalog if exists test_iceberg_case_sensibility_rest"""
                sql """CREATE CATALOG test_iceberg_case_sensibility_rest PROPERTIES (
                        'type'='iceberg',
                        'iceberg.catalog.type'='rest',
                        'uri' = 'http://${externalEnvIp}:${rest_port}',
                        "s3.access_key" = "admin",
                        "s3.secret_key" = "password",
                        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                        "s3.region" = "us-east-1",
                        "only_test_lower_case_table_names" = "${case_type}"
                    );"""

                sql """ switch test_iceberg_case_sensibility_rest """
                sql """ use test_db"""
                sql """ drop table if exists iceberg_case_sensibility_TBL1"""
                sql """ drop table if exists iceberg_case_sensibility_tbl2"""
                sql """ create table iceberg_case_sensibility_TBL1(k1 int);"""
                sql """ create table iceberg_case_sensibility_tbl2(k1 int);"""
                sql """insert into iceberg_case_sensibility_TBL1 values(1)"""
                sql """insert into iceberg_case_sensibility_tbl2 values(1)"""

                if (case_type.equals("0")) {
                    // store in origin, query case sensitive
                    qt_sql """show tables like "iceberg_case_sensibility_TBL1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_TBL1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    test {
                        sql "select * from iceberg_case_sensibility_tbl1"
                        exception "does not exist in database"
                    }
                    test {
                        sql "insert into iceberg_case_sensibility_tbl1 values(1)"
                        exception "does not exist in database"
                    }
                } else if (case_type.equals("1")) {
                    // store in lower case, query case insensitive
                    qt_sql """show tables like "iceberg_case_sensibility_tbl1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_tbl1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_TBL2""";
                } else if (case_type.equals("2")) {
                    // store in origin, query case insensitive
                    qt_sql """show tables like "iceberg_case_sensibility_TBL1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_tbl1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_TBL2""";
                }

                // hadoop
                sql """drop catalog if exists test_iceberg_case_sensibility_hadoop"""
                sql """CREATE CATALOG test_iceberg_case_sensibility_hadoop PROPERTIES (
                        'type'='iceberg',
                        'iceberg.catalog.type'='hadoop',
                        'warehouse' = 's3a://warehouse/wh',
                        "s3.access_key" = "admin",
                        "s3.secret_key" = "password",
                        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                        "s3.region" = "us-east-1",
                        "only_test_lower_case_table_names" = "${case_type}"
                    );"""

                sql """ switch test_iceberg_case_sensibility_hadoop """
                sql """ drop database if exists iceberg_case_sensibility_hadoop_db force"""
                sql """ create database iceberg_case_sensibility_hadoop_db"""
                sql """ use iceberg_case_sensibility_hadoop_db"""
                sql """ drop table if exists iceberg_case_sensibility_TBL1"""
                sql """ drop table if exists iceberg_case_sensibility_tbl2"""
                sql """ create table iceberg_case_sensibility_TBL1(k1 int);"""
                sql """ create table iceberg_case_sensibility_tbl2(k1 int);"""
                sql """insert into iceberg_case_sensibility_TBL1 values(1)"""
                sql """insert into iceberg_case_sensibility_tbl2 values(1)"""

                if (case_type.equals("0")) {
                    // store in origin, query case sensitive
                    qt_sql """show tables like "iceberg_case_sensibility_TBL1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_TBL1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    test {
                        sql "select * from iceberg_case_sensibility_tbl1"
                        exception "does not exist in database"
                    }
                    test {
                        sql "insert into iceberg_case_sensibility_tbl1 values(1)"
                        exception "does not exist in database"
                    }
                } else if (case_type.equals("1")) {
                    // store in lower case, query case insensitive
                    qt_sql """show tables like "iceberg_case_sensibility_tbl1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_tbl1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_TBL2""";
                } else if (case_type.equals("2")) {
                    // store in origin, query case insensitive
                    qt_sql """show tables like "iceberg_case_sensibility_TBL1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_tbl1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_TBL2""";
                }

                // hms
                sql """drop catalog if exists test_iceberg_case_sensibility_hms"""
                sql """create catalog test_iceberg_case_sensibility_hms properties (
                    'type'='iceberg',
                    'iceberg.catalog.type'='hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                    'fs.defaultFS' = '${default_fs}',
                    'warehouse' = '${warehouse}',
                    "only_test_lower_case_table_names" = "${case_type}"
                );"""

                sql """ switch test_iceberg_case_sensibility_hms """
                sql """ drop database if exists iceberg_case_sensibility_hms_db force"""
                sql """ create database iceberg_case_sensibility_hms_db"""
                sql """ use iceberg_case_sensibility_hms_db"""
                sql """ drop table if exists iceberg_case_sensibility_tbl1"""
                sql """ drop table if exists iceberg_case_sensibility_tbl2"""
                sql """ create table iceberg_case_sensibility_TBL1(k1 int);"""
                sql """ create table iceberg_case_sensibility_tbl2(k1 int);"""
                sql """insert into iceberg_case_sensibility_tbl1 values(1)"""
                sql """insert into iceberg_case_sensibility_tbl2 values(1)"""

                if (case_type.equals("0")) {
                    // hms is case insensitive, so table name will be stored in lower, query case sensitive
                    qt_sql """show tables like "iceberg_case_sensibility_tbl1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    test {
                        sql "select * from iceberg_case_sensibility_TBL1"
                        exception "does not exist in database"
                    }
                    test {
                        sql "insert into iceberg_case_sensibility_TBL1 values(1)"
                        exception "does not exist in database"
                    }
                } else if (case_type.equals("1")) {
                    // store in lower case, query case insensitive
                    qt_sql """show tables like "iceberg_case_sensibility_tbl1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_TBL1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_TBL2""";
                } else if (case_type.equals("2")) {
                    // store in origin, query case insensitive
                    qt_sql """show tables like "iceberg_case_sensibility_TBL1""""
                    qt_sql """show tables like "iceberg_case_sensibility_tbl2""""
                    sql """insert into iceberg_case_sensibility_TBL1 values(2)"""
                    sql """insert into iceberg_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from iceberg_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from iceberg_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from iceberg_case_sensibility_TBL2""";
                }
            }

        } finally {
            // sql """set enable_external_table_batch_mode=true"""
        }
    }
}


