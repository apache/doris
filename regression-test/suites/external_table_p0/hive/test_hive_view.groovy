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

suite("test_hive_view", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String catalog_name = "test_${hivePrefix}_view"
        String ex_db_name = "`default`"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

        sql """drop catalog if exists ${catalog_name} """

        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hadoop.username' = 'hive'
        );"""

        sql """use ${catalog_name}.${ex_db_name}"""
        qt_desc1 """desc test_view1"""
        qt_desc2 """desc test_view2"""
        qt_desc3 """desc test_view3"""
        qt_desc4 """desc test_view4"""
        // create view test_view1 as select * from sale_table;
        order_qt_sql1 """ select * from test_view1 """
        // create view test_view2 as select * from default.sale_table;
        order_qt_sql2 """ select dates from test_view2 """
        // create view test_view3 as select * from sale_table where bill_code="bill_code1";
        order_qt_sql3 """ select count(*) from test_view3 """
        // create view test_view4 as select parquet_zstd_all_types.t_int, parquet_zstd_all_types.t_varchar from parquet_zstd_all_types join multi_catalog.parquet_all_types on parquet_zstd_all_types.t_varchar = parquet_all_types.t_varchar order by t_int limit 10;
        order_qt_sql4 """ select * from test_view4 """
        order_qt_sql5 """ select test_view4.t_int from test_view4 join multi_catalog.parquet_all_types on test_view4.t_varchar = parquet_all_types.t_varchar order by test_view4.t_int limit 10; """

        // check unsupported view
        sql """set enable_fallback_to_original_planner=false;"""
        test {
            sql """select * from unsupported_view;"""
            exception """Unknown column 'bill_code' in '_u2' in SORT clause"""
        }
        // pr #43530
        qt_desc5 """desc unsupported_view;"""
    }
}

