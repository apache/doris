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

suite("test_iceberg_rest_case_sensibility", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    try {
        for (String case_type : ["0", "1", "2"]) {
            sql """drop catalog if exists test_iceberg_case_sensibility_rest;"""
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
            sql """switch test_iceberg_case_sensibility_rest;"""
            // 0. clear env
            sql "drop database if exists iceberg_rest_case_db1 force";
            sql "drop database if exists iceberg_rest_case_db2 force";
            sql "drop database if exists ICEBERG_REST_CASE_DB2 force";

            // 1. create db
            sql """create database iceberg_rest_case_db1;"""
            sql """create database if not exists iceberg_rest_case_db1;"""
            sql """create database if not exists iceberg_rest_case_db2;"""
            sql """create database ICEBERG_REST_CASE_DB2;"""

            qt_sql1 """show databases like "%iceberg_rest_case_db1%";"""
            qt_sql3 """show databases like "%iceberg_rest_case_db2%";"""
            qt_sql2 """show databases like "%ICEBERG_REST_CASE_DB2%";"""
            test {
                sql """create database ICEBERG_REST_CASE_DB2;""" // conflict
                exception "database exists"
                exception "ICEBERG_REST_CASE_DB2"
            }
            test {
                sql """create database iceberg_rest_case_db2;""" // conflict
                exception "database exists"
                exception "iceberg_rest_case_db2"
            }
            // 2. drop database
            qt_sql4 """show databases like "%iceberg_rest_case_db1%";"""
            sql """drop database iceberg_rest_case_db1;"""
            qt_sql5 """show databases like "%iceberg_rest_case_db1%";""" // empty

            sql """drop database ICEBERG_REST_CASE_DB2;"""
            sql """drop database iceberg_rest_case_db2;"""
            test {
                sql """drop database iceberg_rest_case_db1"""
                exception "database doesn't exist"
                exception "iceberg_rest_case_db1"
            }
            test {
                sql """drop database iceberg_rest_case_db2"""
                exception "database doesn't exist"
                exception "iceberg_rest_case_db2"
            }
            sql """drop database if exists iceberg_rest_case_db2;""" 
            qt_sql6 """show databases like "%ICEBERG_REST_CASE_DB2%";""" // empty
            qt_sql7 """show databases like "%iceberg_rest_case_db2%";""" // empty

            // 3. recreate db to test create table
            sql """create database iceberg_rest_case_db1;"""
            sql """create database iceberg_rest_case_db2;"""
            sql """create database ICEBERG_REST_CASE_DB2;"""

            sql """use iceberg_rest_case_db1"""
            sql """use iceberg_rest_case_db2"""
            sql """use ICEBERG_REST_CASE_DB2"""
            
            test {
                sql """create table ICEBERG_REST_CASE_DB1.case_tbl21 (k1 int);"""
                exception "Failed to get database: 'ICEBERG_REST_CASE_DB1'"
            }
            test {
                sql """create table if not exists ICEBERG_REST_CASE_DB1.case_tbl21 (k1 int);"""
                exception "Failed to get database: 'ICEBERG_REST_CASE_DB1'"
            }
            sql """create table iceberg_rest_case_db1.case_tbl11 (k1 int);"""
            sql """create table iceberg_rest_case_db2.CASE_TBL22 (k1 int);"""
            if (case_type.equals("0")) {
                sql """create table iceberg_rest_case_db2.case_tbl22 (k1 int);"""
            } else {
                test {
                    sql """create table iceberg_rest_case_db2.case_tbl22 (k1 int);"""
                    exception "Table 'case_tbl22' already exists"
                }
            }
            sql """create table ICEBERG_REST_CASE_DB2.case_tbl21 (k1 int);""" 

            test {
                sql """create table iceberg_rest_case_db1.case_tbl11 (k1 int);"""
                exception "Table 'case_tbl11' already exists"
            }
            sql """create table if not exists iceberg_rest_case_db1.case_tbl11 (k1 int);"""
            if (case_type.equals("0")) {
                sql """create table if not exists iceberg_rest_case_db1.CASE_TBL11 (k1 int);"""
            } else {
                test {
                    sql """create table iceberg_rest_case_db1.CASE_TBL11 (k1 int);"""
                    exception "Table 'CASE_TBL11' already exists"
                }
            }
            qt_sqlx """show tables from iceberg_rest_case_db1 like "%CASE_TBL11%""""

            sql """create table iceberg_rest_case_db1.CASE_TBL12 (k1 int);"""
            sql """use iceberg_rest_case_db1;"""
            sql """create table case_tbl13 (k1 int);"""
            sql """create table CASE_TBL14 (k1 int);"""

            qt_sql8 """show tables like "%CASE_TBL14%""""
            qt_sql9 """show tables like "%case_tbl14%"""" // empty
            qt_sql10 """show tables like "%case_tbl13%""""

            test {
                sql """show tables from ICEBERG_REST_CASE_DB1 like "%case_tbl14%""""
                exception "Unknown database 'ICEBERG_REST_CASE_DB1'"
            }
            qt_sql11 """show tables from iceberg_rest_case_db2 like "%case_tbl14%"""" // empty
            qt_sql12 """show tables from iceberg_rest_case_db2 like "%case_tbl21%"""" // empty
            qt_sql12 """show tables from iceberg_rest_case_db2 like "%case_tbl22%""""

            order_qt_sql13 """select * from information_schema.tables where TABLE_SCHEMA="iceberg_rest_case_db2";"""
            order_qt_sql14 """select * from information_schema.columns where TABLE_SCHEMA="iceberg_rest_case_db2";"""
            order_qt_sql131 """select * from information_schema.tables where TABLE_SCHEMA="ICEBERG_REST_CASE_DB2";"""
            order_qt_sql141 """select * from information_schema.columns where TABLE_SCHEMA="ICEBERG_REST_CASE_DB2";"""

            // 4. insert
            /// full qualified name 
            test {
                sql """insert into ICEBERG_REST_CASE_DB1.CASE_TBL11 values(1);"""
                exception "Database [ICEBERG_REST_CASE_DB1] does not exist"
            }
            test {
                sql """insert into ICEBERG_REST_CASE_DB1.case_tbl11 values(1);"""
                exception "Database [ICEBERG_REST_CASE_DB1] does not exist."
            }

            if (case_type.equals("0")) {
                test {
                    sql """insert into iceberg_rest_case_db1.CASE_TBL13 values(1);"""
                    exception "Table [CASE_TBL13] does not exist in database [iceberg_rest_case_db1]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert into iceberg_rest_case_db1.CASE_TBL13 values(11);"""
                order_qt_sqly """select * from iceberg_rest_case_db1.CASE_TBL13"""
            }

            
            sql """insert into iceberg_rest_case_db2.case_tbl22 values(1);"""
            sql """insert into iceberg_rest_case_db2.CASE_TBL22 values(1);"""
            order_qt_sqlz1 """select * from iceberg_rest_case_db2.CASE_TBL22"""
            order_qt_sqlz2 """select * from iceberg_rest_case_db2.case_tbl22"""
            test {
                sql """select * from ICEBERG_REST_CASE_DB1.CASE_TBL11"""
                exception "Database [ICEBERG_REST_CASE_DB1] does not exist"
            }
            test {
                sql """select * from ICEBERG_REST_CASE_DB1.case_tbl13"""
                exception "Database [ICEBERG_REST_CASE_DB1] does not exist"
            }

            test {
                sql """insert overwrite table ICEBERG_REST_CASE_DB1.CASE_TBL11 values(2);"""
                exception "Database [ICEBERG_REST_CASE_DB1] does not exist"
            }
            test {
                sql """insert overwrite table ICEBERG_REST_CASE_DB1.case_tbl11 values(2); """
                exception "Database [ICEBERG_REST_CASE_DB1] does not exist"
            }
            
            sql """insert overwrite table iceberg_rest_case_db2.case_tbl22 values(20);"""
            sql """insert overwrite table iceberg_rest_case_db2.CASE_TBL22 values(21);"""
            // 0: 20, 1,2: 21
            order_qt_sql16 """select * from iceberg_rest_case_db2.case_tbl22;"""

            /// not full qualified
            sql """use iceberg_rest_case_db1;"""
            if (case_type.equals("0")) {
                test {
                    sql """insert into case_tbl12 values(3);"""
                    exception "Table [case_tbl12] does not exist in database [iceberg_rest_case_db1]"
                }
                sql """insert into CASE_TBL12 values(3);"""
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert into CASE_TBL12 values(3);"""
                sql """insert into case_tbl12 values(3);"""
            }

            if (case_type.equals("0")) {
                test {
                    sql """select * from case_tbl12"""
                    exception "Table [case_tbl12] does not exist in database [iceberg_rest_case_db1]"
                }
                order_qt_sql17 """select * from CASE_TBL12"""
            } else if (case_type.equals("1") || case_type.equals("2")) {
                order_qt_sql1511 """select * from CASE_TBL12"""
                order_qt_sql1512 """select * from case_tbl12"""
            }

            if (case_type.equals("0")) {
                test {
                    sql """insert overwrite table case_tbl12 values(4);"""
                    exception "Table [case_tbl12] does not exist in database [iceberg_rest_case_db1]"
                }
                sql """insert overwrite table CASE_TBL12 values(4);"""
                order_qt_sql18 """select * from CASE_TBL12;"""
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert overwrite table case_tbl12 values(4);"""
                sql """insert overwrite table CASE_TBL12 values(5);"""
                order_qt_sql18 """select * from case_tbl12;"""
                order_qt_sql18 """select * from CASE_TBL12;"""
            }

            // // 5. truncate(todo not support for iceberg now)

            // 6. drop table
            /// full qualified
            test {
                sql """drop table ICEBERG_REST_CASE_DB1.CASE_TBL11"""
                exception "Failed to get database: 'ICEBERG_REST_CASE_DB1' in catalog"
            }
            test {
                sql """drop table ICEBERG_REST_CASE_DB1.case_tbl13"""
                exception "Failed to get database: 'ICEBERG_REST_CASE_DB1'"
            }
            test {
                sql """drop table if exists ICEBERG_REST_CASE_DB1.case_tbl22;"""
                exception "Failed to get database: 'ICEBERG_REST_CASE_DB1'"
            }
            if (case_type.equals("0")) {
                sql """drop table iceberg_rest_case_db2.CASE_TBL22"""
                sql """drop table iceberg_rest_case_db2.case_tbl22"""
                test {
                    sql """drop table iceberg_rest_case_db2.CASE_TBL22"""
                    exception "Failed to get table: 'CASE_TBL22'"
                }
                test {
                    sql """drop table iceberg_rest_case_db2.case_tbl22"""
                    exception "Failed to get table: 'case_tbl22'"
                }
            } else {
                sql """drop table iceberg_rest_case_db2.CASE_TBL22"""
                test {
                    sql """drop table iceberg_rest_case_db2.case_tbl22"""
                    exception "Failed to get table: 'case_tbl22'"
                }
            }

            sql """drop table if exists iceberg_rest_case_db2.case_tbl22"""
                
            test {
                sql """select * from iceberg_rest_case_db2.case_tbl22;"""
                exception "Table [case_tbl22] does not exist in database [iceberg_rest_case_db2]"
            }
            sql """create table iceberg_rest_case_db2.case_tbl22 (k1 int);""" // recreate
            qt_sql_show """show tables from iceberg_rest_case_db2 like "%case_tbl22%""""
            
            sql """insert into iceberg_rest_case_db2.case_tbl22 values(5);"""
            order_qt_sql21 """select * from iceberg_rest_case_db2.case_tbl22;"""

            /// not full qualified
            sql """use iceberg_rest_case_db1;"""
            if (case_type.equals("0")) {
                test {
                    sql """drop table case_tbl12;"""
                    exception "Failed to get table: 'case_tbl12' in database: iceberg_rest_case_db1"
                }
                sql """drop table CASE_TBL12;"""
            } else {
                sql """drop table CASE_TBL11;"""
                test {
                    sql """drop table case_tbl11;"""
                    exception "Failed to get table: 'case_tbl11' in database: iceberg_rest_case_db1"
                }
            }

            test {
                sql """select * from iceberg_rest_case_db2.case_tbl12;"""
                exception "Table [case_tbl12] does not exist in database [iceberg_rest_case_db2]"
            }

            // 7. re create and insert
            sql """create table iceberg_rest_case_db2.case_tbl12 (k1 int);""" 
            sql """insert into  iceberg_rest_case_db2.case_tbl12 values(6);"""
            order_qt_sql22 """select * from iceberg_rest_case_db2.case_tbl12;"""
            sql """insert overwrite table  iceberg_rest_case_db2.case_tbl12 values(7);"""
            order_qt_sql222 """select * from iceberg_rest_case_db2.case_tbl12;"""

            // 8. drop db force
            sql """insert into iceberg_rest_case_db1.case_tbl13 values(8)"""
            sql """insert into iceberg_rest_case_db1.CASE_TBL14 values(9)"""
            order_qt_sql23 """select * from iceberg_rest_case_db1.case_tbl13;"""
            order_qt_sql24 """select * from iceberg_rest_case_db1.CASE_TBL14;"""

            // use tvf to check data under dir
            order_qt_sql25 """select * from s3(
                "uri" = "s3a://warehouse/wh/iceberg_rest_case_db1/CASE_TBL14/data/*",
                "format" = "parquet",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}"
                );
                """
            sql """drop database iceberg_rest_case_db1 force;"""
            order_qt_sql26 """select * from s3(
                "uri" = "s3a://warehouse/wh/iceberg_rest_case_db1/CASE_TBL14/data/*",
                "format" = "parquet",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}"
                );
                """ // empty
        }
    } finally {
    }
}
