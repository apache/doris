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

suite("test_iceberg_dlf_rest_catalog", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String catalog = "test_iceberg_dlf_rest_catalog"
    String prop= context.config.otherConfigs.get("icebergDlfRestCatalog")

    sql """drop catalog if exists ${catalog};"""
    sql """
        create catalog if not exists ${catalog} properties (
            ${prop}
        );
    """

    sql """ use ${catalog}.test_iceberg_db"""

    qt_c1 """ select * from test_iceberg_table order by k1 """
    qt_c2 """  select file_format from test_iceberg_db.test_iceberg_table\$files;"""

    def uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    sql """drop database if exists dlf_iceberg_db_${uuid} force"""
    sql """create database dlf_iceberg_db_${uuid}"""
    sql """use dlf_iceberg_db_${uuid}"""
    sql """create table dlf_iceberg_test_tbl (k1 int, k2 string);"""
    sql """insert into dlf_iceberg_test_tbl values(1, 'abc'),(2, 'def');"""
    qt_c3 """select * from dlf_iceberg_test_tbl order by k1"""
    qt_c4 """select file_format from dlf_iceberg_test_tbl\$files"""
    sql """drop table dlf_iceberg_test_tbl"""
    qt_c5 """show tables"""
}

