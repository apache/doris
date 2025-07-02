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

suite("test_hive_case_sensibility", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {

        try {

         for (String hivePrefix : ["hive2", "hive3"]) {

           String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
           String catalog_name = "${hivePrefix}_test_case_sensibility"
           String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            for (String case_type : ["0", "1", "2"]) {

                sql """drop catalog if exists ${catalog_name};"""
                sql """
                      create catalog if not exists ${catalog_name} properties (
                        'type'='hms',
                         'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                         'only_test_lower_case_table_names' = '${case_type}'
                      );
                    """
                 sql """switch ${catalog_name};"""

                 sql """ drop database if exists hive_case_sensibility_db force"""
                 sql """ create database hive_case_sensibility_db"""
                 sql """ use hive_case_sensibility_db"""

                 sql """ drop table if exists hive_case_sensibility_tbl1"""
                 sql """ drop table if exists hive_case_sensibility_tbl2"""
                 sql """ create table hive_case_sensibility_TBL1(k1 int);"""
                 sql """ create table hive_case_sensibility_tbl2(k1 int);"""
                 sql """insert into hive_case_sensibility_tbl1 values(1)"""
                 sql """insert into hive_case_sensibility_tbl2 values(1)"""

                 if (case_type.equals("0")) {
                   // hms is case insensitive, so table name will be stored in lower, query case sensitive
                    qt_sql """show tables like "hive_case_sensibility_tbl1";"""
                    qt_sql """show tables like "hive_case_sensibility_tbl2";"""
                    order_qt_sql11 """select * from hive_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from hive_case_sensibility_tbl2""";
                    test {
                           sql "select * from hive_case_sensibility_TBL1"
                           exception "does not exist in database"
                         }
                    test {
                           sql "insert into hive_case_sensibility_TBL1 values(1)"
                           exception "does not exist in database"
                          }
                 } else if (case_type.equals("1")) {
                    // store in lower case, query case insensitive
                    qt_sql """show tables like "hive_case_sensibility_tbl1";"""
                    qt_sql """show tables like "hive_case_sensibility_tbl2";"""
                    sql """insert into hive_case_sensibility_TBL1 values(2)"""
                    sql """insert into hive_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from hive_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from hive_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from hive_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from hive_case_sensibility_TBL2""";
                } else if (case_type.equals("2")) {
                    // store in origin, query case insensitive
                    qt_sql """show tables like "hive_case_sensibility_TBL1";"""
                    qt_sql """show tables like "hive_case_sensibility_tbl2";"""
                    sql """insert into hive_case_sensibility_TBL1 values(2)"""
                    sql """insert into hive_case_sensibility_tbl2 values(2)"""
                    order_qt_sql11 """select * from hive_case_sensibility_TBL1""";
                    order_qt_sql12 """select * from hive_case_sensibility_tbl2""";
                    order_qt_sql11 """select * from hive_case_sensibility_tbl1""";
                    order_qt_sql12 """select * from hive_case_sensibility_TBL2""";
                }
            }
         }
        } finally {
            // sql """set enable_external_table_batch_mode=true"""
        }
    }
}
