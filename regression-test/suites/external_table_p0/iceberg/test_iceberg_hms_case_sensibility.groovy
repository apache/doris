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

suite("test_iceberg_hms_case_sensibility", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get("hive2HmsPort")
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
    String warehouse = "${default_fs}/warehouse"

    try {
        for (String case_type : ["0", "1", "2"]) {
            sql """drop catalog if exists test_iceberg_case_sensibility_hms;"""
            sql """create catalog test_iceberg_case_sensibility_hms properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = '${default_fs}',
                'warehouse' = '${warehouse}',
                "only_test_lower_case_table_names" = "${case_type}"
            );"""
            sql """switch test_iceberg_case_sensibility_hms;"""
            // 0. clear env
            sql "drop database if exists iceberg_hms_case_db1 force";
            sql "drop database if exists iceberg_hms_case_db2 force";

            // 1. create db
            sql """create database iceberg_hms_case_db1;"""
            test {
                sql """create database ICEBERG_HMS_CASE_DB1;""" // conflict
                exception "Can't create database 'ICEBERG_HMS_CASE_DB1'; database exists"
            }
            sql """create database ICEBERG_HMS_CASE_DB2;"""
            sql """create database if not exists ICEBERG_HMS_CASE_DB1;"""
            sql """create database if not exists iceberg_hms_case_db1;"""
            sql """create database if not exists iceberg_hms_case_db2;"""

            qt_sql1 """show databases like "%iceberg_hms_case_db1%";"""
            qt_sql2 """show databases like "%ICEBERG_HMS_CASE_DB1%";""" // empty
            qt_sql3 """show databases like "%iceberg_hms_case_db2%";"""
            test {
                sql """create database ICEBERG_HMS_CASE_DB2;""" // conflict
                exception "database exists"
                exception "ICEBERG_HMS_CASE_DB2"
            }
            test {
                sql """create database iceberg_hms_case_db2;""" // conflict
                exception "database exists"
                exception "iceberg_hms_case_db2"
            }
            // 2. drop database
            test {
                sql """drop database ICEBERG_HMS_CASE_DB1"""
                exception "database doesn't exist"
                exception "ICEBERG_HMS_CASE_DB1"
            }
            sql """drop database if exists ICEBERG_HMS_CASE_DB1;""" 
            qt_sql4 """show databases like "%iceberg_hms_case_db1%";""" // still exists
            sql """drop database iceberg_hms_case_db1;"""
            qt_sql5 """show databases like "%iceberg_hms_case_db1%";""" // empty

            test {
                sql """drop database ICEBERG_HMS_CASE_DB2;"""
                exception "database doesn't exist"
                exception "ICEBERG_HMS_CASE_DB2"
            }
            sql """drop database iceberg_hms_case_db2;"""
            test {
                sql """drop database iceberg_hms_case_db1"""
                exception "database doesn't exist"
                exception "iceberg_hms_case_db1"
            }
            test {
                sql """drop database iceberg_hms_case_db2"""
                exception "database doesn't exist"
                exception "iceberg_hms_case_db2"
            }
            sql """drop database if exists iceberg_hms_case_db2;""" 
            qt_sql6 """show databases like "%iceberg_hms_case_db1%";""" // empty
            qt_sql7 """show databases like "%iceberg_hms_case_db2%";""" // empty

            // 3. recreate db to test create table
            sql """create database iceberg_hms_case_db1;"""
            sql """create database ICEBERG_HMS_CASE_DB2;"""

            test {
                sql """use ICEBERG_HMS_CASE_DB2"""
                exception "Unknown database 'ICEBERG_HMS_CASE_DB2'"
            }
            
            test {
                sql """create table ICEBERG_HMS_CASE_DB2.case_tbl21 (k1 int);"""
                exception "Failed to get database: 'ICEBERG_HMS_CASE_DB2'"
            }
            test {
                sql """create table if not exists ICEBERG_HMS_CASE_DB2.case_tbl21 (k1 int);"""
                exception "Failed to get database: 'ICEBERG_HMS_CASE_DB2'"
            }
            sql """create table iceberg_hms_case_db2.case_tbl21 (k1 int);""" 
            sql """create table iceberg_hms_case_db2.CASE_TBL22 (k1 int);"""
            sql """create table iceberg_hms_case_db1.case_tbl11 (k1 int);"""

            test {
                sql """create table iceberg_hms_case_db1.case_tbl11 (k1 int);"""
                exception "Table 'case_tbl11' already exists"
            }
            sql """create table if not exists iceberg_hms_case_db1.case_tbl11 (k1 int);"""
            sql """create table if not exists iceberg_hms_case_db1.CASE_TBL11 (k1 int);"""

            sql """create table iceberg_hms_case_db1.CASE_TBL12 (k1 int);"""
            sql """use iceberg_hms_case_db1;"""
            sql """create table case_tbl13 (k1 int);"""
            sql """create table CASE_TBL14 (k1 int);"""

            qt_sql8 """show tables like "%CASE_TBL14%"""" // empty
            qt_sql9 """show tables like "%case_tbl14%""""
            qt_sql10 """show tables like "%case_tbl13%""""

            test {
                sql """show tables from ICEBERG_HMS_CASE_DB2 like "%case_tbl14%""""
                exception "Unknown database 'ICEBERG_HMS_CASE_DB2'"
            }
            qt_sql11 """show tables from iceberg_hms_case_db2 like "%case_tbl14%"""" // empty
            qt_sql12 """show tables from iceberg_hms_case_db2 like "%case_tbl21%""""

            order_qt_sql13 """select * from information_schema.tables where TABLE_SCHEMA="iceberg_hms_case_db1";"""
            order_qt_sql14 """select * from information_schema.columns where TABLE_SCHEMA="iceberg_hms_case_db1";"""

            // 4. insert
            /// full qualified name 
            test {
                sql """insert into ICEBERG_HMS_CASE_DB2.CASE_TBL22 values(1);"""
                exception "Database [ICEBERG_HMS_CASE_DB2] does not exist"
            }
            test {
                sql """insert into ICEBERG_HMS_CASE_DB2.case_tbl22 values(1);"""
                exception "Database [ICEBERG_HMS_CASE_DB2] does not exist."
            }

            if (case_type.equals("0")) {
                test {
                    sql """insert into iceberg_hms_case_db2.CASE_TBL22 values(1);"""
                    exception "Table [CASE_TBL22] does not exist in database [iceberg_hms_case_db2]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert into iceberg_hms_case_db2.CASE_TBL22 values(11);"""
            }

            sql """insert into iceberg_hms_case_db2.case_tbl22 values(1);"""
            test {
                sql """select * from ICEBERG_HMS_CASE_DB2.CASE_TBL22"""
                exception "Database [ICEBERG_HMS_CASE_DB2] does not exist"
            }
            test {
                sql """select * from ICEBERG_HMS_CASE_DB2.case_tbl22"""
                exception "Database [ICEBERG_HMS_CASE_DB2] does not exist"
            }

            if (case_type.equals("0")) {
                test {
                    sql """select * from iceberg_hms_case_db2.CASE_TBL22"""
                    exception "Table [CASE_TBL22] does not exist in database [iceberg_hms_case_db2]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                order_qt_sql141 """select * from iceberg_hms_case_db2.CASE_TBL22"""
            }
            order_qt_sql15 """select * from iceberg_hms_case_db2.case_tbl22"""

            test {
                sql """insert overwrite table ICEBERG_HMS_CASE_DB2.CASE_TBL22 values(2);"""
                exception "Database [ICEBERG_HMS_CASE_DB2] does not exist"
            }
            test {
                sql """insert overwrite table ICEBERG_HMS_CASE_DB2.case_tbl22 values(2); """
                exception "Database [ICEBERG_HMS_CASE_DB2] does not exist"
            }
            if (case_type.equals("0")) {
                test {
                    sql """insert overwrite table iceberg_hms_case_db2.CASE_TBL22 values(2);"""
                    exception "Table [CASE_TBL22] does not exist in database [iceberg_hms_case_db2]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert overwrite table iceberg_hms_case_db2.CASE_TBL22 values(2);"""
            }

            sql """insert overwrite table iceberg_hms_case_db2.case_tbl22 values(2);"""
            order_qt_sql16 """select * from iceberg_hms_case_db2.case_tbl22;"""

            /// not full qualified
            sql """use iceberg_hms_case_db1;"""
            if (case_type.equals("0")) {
                test {
                    sql """insert into CASE_TBL12 values(3);"""
                    exception "Table [CASE_TBL12] does not exist in database [iceberg_hms_case_db1]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert into CASE_TBL12 values(3);"""
            }

            sql """insert into case_tbl12 values(3);"""
            if (case_type.equals("0")) {
                test {
                    sql """select * from CASE_TBL12"""
                    exception "Table [CASE_TBL12] does not exist in database [iceberg_hms_case_db1]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                order_qt_sql151 """select * from CASE_TBL12"""
            }
            order_qt_sql17 """select * from case_tbl12"""

            if (case_type.equals("0")) {
                test {
                    sql """insert overwrite table CASE_TBL12 values(4);"""
                    exception "Table [CASE_TBL12] does not exist in database [iceberg_hms_case_db1]"
                }
            } else if (case_type.equals("1") || case_type.equals("2")) {
                sql """insert overwrite table CASE_TBL12 values(4);"""
            }
            sql """insert overwrite table case_tbl12 values(4);"""
            order_qt_sql18 """select * from case_tbl12;"""

            // // 5. truncate(todo not support for iceberg now)
            // /// full qualified
            // test {
            //     sql """truncate table ICEBERG_HMS_CASE_DB2.CASE_TBL22"""
            //     exception "Unknown database 'ICEBERG_HMS_CASE_DB2'"
            // }
            // test {
            //     sql """truncate table ICEBERG_HMS_CASE_DB2.case_tbl22"""
            //     exception "Unknown database 'ICEBERG_HMS_CASE_DB2'"
            // }
            // if (case_type.equals("0")) {
            //     test {
            //         sql """truncate table iceberg_hms_case_db2.CASE_TBL22"""
            //         exception "Unknown table 'CASE_TBL22'"
            //     }
            // } else {
            //     sql """truncate table iceberg_hms_case_db2.CASE_TBL22"""
            // }
            // sql """truncate table iceberg_hms_case_db2.case_tbl22 ;"""
            // qt_sql19 """select * from iceberg_hms_case_db2.case_tbl22;""" // empty
            // /// not full qualified
            // sql """use iceberg_hms_case_db1;"""
            // if (case_type.equals("0")) {
            //     test {
            //         sql """truncate table CASE_TBL12;"""
            //         exception "Unknown table 'CASE_TBL12'"
            //     }
            // } else {
            //     sql """truncate table CASE_TBL12;"""
            // }
            // sql """truncate table case_tbl12;"""
            // qt_sql20 """select * from case_tbl12;""" // empty

            // 6. drop table
            /// full qualified
            test {
                sql """drop table ICEBERG_HMS_CASE_DB2.CASE_TBL22"""
                exception "Failed to get database: 'ICEBERG_HMS_CASE_DB2' in catalog"
            }
            test {
                sql """drop table ICEBERG_HMS_CASE_DB2.case_tbl22"""
                exception "Failed to get database: 'ICEBERG_HMS_CASE_DB2'"
            }
            test {
                sql """drop table if exists ICEBERG_HMS_CASE_DB2.case_tbl22;"""
                exception "Failed to get database: 'ICEBERG_HMS_CASE_DB2'"
            }
            if (case_type.equals("0")) {
                test {
                    sql """drop table iceberg_hms_case_db2.CASE_TBL22"""
                    exception "Failed to get table: 'CASE_TBL22'"
                }
                sql """drop table iceberg_hms_case_db2.case_tbl22"""
            } else {
                sql """drop table iceberg_hms_case_db2.CASE_TBL22"""
            }

            test {
                sql """drop table iceberg_hms_case_db2.case_tbl22"""
                exception "Failed to get table: 'case_tbl22'"
            }
            sql """drop table if exists iceberg_hms_case_db2.case_tbl22"""
                
            test {
                sql """select * from iceberg_hms_case_db2.case_tbl22;"""
                exception "Table [case_tbl22] does not exist in database [iceberg_hms_case_db2]"
            }
            sql """create table iceberg_hms_case_db2.case_tbl22 (k1 int);""" // recreate
            sql """insert into iceberg_hms_case_db2.case_tbl22 values(5);"""
            order_qt_sql21 """select * from iceberg_hms_case_db2.case_tbl22;"""

            /// not full qualified
            sql """use iceberg_hms_case_db1;"""
            if (case_type.equals("0")) {
                test {
                    sql """drop table CASE_TBL12;"""
                    exception "Failed to get table: 'CASE_TBL12' in database: iceberg_hms_case_db1"
                }
                sql """drop table case_tbl12;"""
            } else {
                sql """drop table CASE_TBL12;"""
            }

            test {
                sql """select * from iceberg_hms_case_db2.case_tbl12;"""
                exception "Table [case_tbl12] does not exist in database [iceberg_hms_case_db2]"
            }

            // 7. re create and insert
            sql """create table iceberg_hms_case_db2.case_tbl12 (k1 int);""" 
            sql """insert into  iceberg_hms_case_db2.case_tbl12 values(6);"""
            order_qt_sql22 """select * from iceberg_hms_case_db2.case_tbl12;"""
            sql """insert overwrite table  iceberg_hms_case_db2.case_tbl12 values(7);"""
            order_qt_sql222 """select * from iceberg_hms_case_db2.case_tbl12;"""

            // 8. drop db force
            sql """insert into iceberg_hms_case_db1.case_tbl13 values(8)"""
            sql """insert into iceberg_hms_case_db1.case_tbl14 values(9)"""
            order_qt_sql23 """select * from iceberg_hms_case_db1.case_tbl13;"""
            order_qt_sql24 """select * from iceberg_hms_case_db1.case_tbl14;"""

            // use tvf to check data under dir
            order_qt_sql25 """select * from hdfs(
                "uri" = "hdfs://${externalEnvIp}:${hdfs_port}/user/hive/warehouse/iceberg_hms_case_db1.db/case_tbl14/*",
                "format" = "orc"
                );
                """
            sql """drop database iceberg_hms_case_db1 force;"""
            order_qt_sql26 """select * from hdfs(
                "uri" = "hdfs://${externalEnvIp}:${hdfs_port}/user/hive/warehouse/iceberg_hms_case_db1.db/case_tbl14/*",
                "format" = "orc"
                );
                """ // empty
        }
    } finally {
    }
}
