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

suite("test_iceberg_view_query_p0", "p0,external,iceberg,external_docker,external_docker_iceberg") {

    String enableIcebergTest = context.config.otherConfigs.get("enableIcebergTest")
    // if (enableIcebergTest == null || !enableIcebergTest.equalsIgnoreCase("true")) {
    // This is suit can not be run currently because only hive catalog can support view.
    // but we can only create view in rest catalog
    // will be fixed later
    if (true) {
        logger.info("disable iceberg test.")
        return
    }

    try {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        //create iceberg hms catalog
        String iceberg_catalog_name = "test_iceberg_view_query_p0"
        sql """drop catalog if exists ${iceberg_catalog_name}"""
        sql """create catalog if not exists ${iceberg_catalog_name} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
            'use_meta_cache' = 'true'
        );"""

        //using database and close planner fallback
        sql """use `${iceberg_catalog_name}`.`test_db`"""
        sql """set enable_fallback_to_original_planner=false;"""

        // run all suites
        def q01 = {
            order_qt_q01 """select * from v_with_partitioned_table order by col1"""
            order_qt_q02 """select * from v_with_unpartitioned_table order by col1"""
            order_qt_q03 """select * from v_with_partitioned_column order by col5"""
        }

        def q02 = {
            order_qt_q01 """select count(*) from v_with_partitioned_table"""
            order_qt_q02 """select count(*) from v_with_unpartitioned_table"""
            order_qt_q03 """select count(*) from v_with_partitioned_column"""
        }

        def q03 = {
            order_qt_q01 """select col1,col2,col3,col4 from v_with_partitioned_table order by col1"""
            order_qt_q02 """select col5 from v_with_partitioned_table order by col5"""
        }

        def q04 = {
            order_qt_q01 """describe v_with_partitioned_table"""
            order_qt_q02 """describe v_with_unpartitioned_table"""
            order_qt_q03 """describe v_with_partitioned_column"""
        }

        q01()
        q02()
        q03()
        q04()

        def result1 = sql """explain verbose select * from v_with_partitioned_table"""
        assertTrue(result1.contains('t_partitioned_table'))
        def result2 = sql """explain verbose select * from v_with_unpartitioned_table"""
        assertTrue(result2.contains('v_with_unpartitioned_table'))
        def result3 = sql """explain verbose select * from v_with_partitioned_column"""
        assertTrue(result3.contains('t_partitioned_table'))

        def result4 = sql """explain verbose select count(*) from v_with_partitioned_table"""
        assertTrue(result4.contains('t_partitioned_table'))
        def result5 = sql """explain verbose select count(*) from v_with_unpartitioned_table"""
        assertTrue(result5.contains('t_unpartitioned_table'))
        def result6 = sql """explain verbose select count(*) from v_with_partitioned_column"""
        assertTrue(result6.contains('t_partitioned_table'))

        def result7 = sql """explain verbose select col1,col2,col3,col4 from v_with_partitioned_table"""
        assertTrue(result7.contains('t_partitioned_table'))
        def result8 = sql """explain verbose select col5 from v_with_partitioned_table"""
        assertTrue(result8.contains('t_partitioned_table'))

        def result9 = sql """show create table v_with_partitioned_table"""
        assertTrue(result9.contains('v_with_partitioned_table'))
        def result10 = sql """show create table v_with_unpartitioned_table"""
        assertTrue(result10.contains('v_with_unpartitioned_table'))
        def result11 = sql """show create table v_with_partitioned_column"""
        assertTrue(result11.contains('v_with_partitioned_column'))

        def result12 = sql """show create view v_with_partitioned_table"""
        assertTrue(result12.contains('v_with_partitioned_table'))
        def result13 = sql """show create view v_with_unpartitioned_table"""
        assertTrue(result13.contains('v_with_unpartitioned_table'))
        def result14 = sql """show create view v_with_partitioned_column"""
        assertTrue(result14.contains('v_with_partitioned_column'))

        try {
            sql """select * from v_with_partitioned_table FOR TIME AS OF '2025-06-11 20:17:01' order by col1 limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select * from v_with_partitioned_table FOR VERSION AS OF 5497706844625725452 order by col1 limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_partitioned_table FOR TIME AS OF '2025-06-11 20:17:01'"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_partitioned_table FOR VERSION AS OF 5497706844625725452"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }

        try {
            sql """select * from v_with_unpartitioned_table FOR TIME AS OF '2025-06-11 20:17:01' order by col1 limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select * from v_with_unpartitioned_table FOR VERSION AS OF 5497706844625725452 order by col1 limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_unpartitioned_table FOR TIME AS OF '2025-06-11 20:17:01'"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_unpartitioned_table FOR VERSION AS OF 5497706844625725452"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }

        try {
            sql """select * from v_with_partitioned_column FOR TIME AS OF '2025-06-11 20:17:01' order by col5 limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select * from v_with_partitioned_column FOR VERSION AS OF 5497706844625725452 order by col5 limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_partitioned_column FOR TIME AS OF '2025-06-11 20:17:01'"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_partitioned_column FOR VERSION AS OF 5497706844625725452"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }

        try {
            sql """select * from v_with_joint_table FOR TIME AS OF '2025-06-11 20:17:01' order by sale_date limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select * from v_with_joint_table FOR VERSION AS OF 5497706844625725452 order by sale_date limit 10"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_joint_table FOR TIME AS OF '2025-06-11 20:17:01'"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }
        try {
            sql """select count(*) from v_with_joint_table FOR VERSION AS OF 5497706844625725452"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("iceberg view not supported with snapshot time/version travel"), e.getMessage())
        }

        sql """drop view v_with_partitioned_table"""
        sql """drop view v_with_unpartitioned_table"""
        sql """drop view v_with_partitioned_column"""

        sql """drop catalog if exists ${iceberg_catalog_name}"""
    } finally {
    }
}
