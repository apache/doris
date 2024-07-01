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

suite("test_external_catalog_hive", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_external_catalog_hive"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """set enable_fallback_to_original_planner=false"""
        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );
        """

        sql """switch ${catalog_name};"""

        sql """use test;"""

        def res = sql """select count(*) from test.hive_test limit 10;"""
        logger.info("recoding select: " + res.toString())

        sql """switch internal"""

        def res1 = sql """show databases;"""
        logger.info("recoding select: " + res1.toString())

        sql """switch ${catalog_name};"""
        // test small table(text format)
        def q01 = {
            qt_q01 """ select name, count(1) as c from student group by name order by name desc;"""
            qt_q02 """ select lo_orderkey, count(1) as c from lineorder group by lo_orderkey order by lo_orderkey asc;"""
            qt_q03 """ select * from test1 order by col_1;"""
            qt_q04 """ select * from string_table order by p_partkey desc;"""
            qt_q05 """ select * from account_fund order by batchno;"""
            qt_q06 """ select * from sale_table order by bill_code limit 01;"""
            qt_q07 """ select count(card_cnt) from hive01;"""
            qt_q08 """ select * from test2 order by id;"""
            qt_q09 """ select * from test_hive_doris order by id;"""
        }
        sql """ use `default`; """
        q01()
        
        // Too big for p0 test, comment it.
        //test for big table(parquet format)
        // def q02 = {
        //     qt_q10 """ select c_address from customer where c_custkey = 1 and c_name = 'Customer#000000001'; """
        //     qt_q11 """ select l_quantity from lineitem where l_orderkey = 599614241 and l_partkey = 59018738 and l_suppkey = 1518744 limit 2 """
        //     qt_q12 """ select count(1) from nation """
        //     qt_q13 """ select count(1) from orders """
        //     qt_q14 """ select p_name from part where p_partkey = 4438130 order by p_name limit 1; """
        //     qt_q15 """ select ps_supplycost from partsupp where ps_partkey = 199588198 and ps_suppkey = 9588199 and ps_availqty = 2949 """
        //     qt_q16 """ select * from region order by r_regionkey limit 3 """
        //     qt_q17 """ select s_address from supplier where s_suppkey = 2823947 limit 3"""
        // }
        // sql """ use tpch_1000_parquet; """
        // q02()

        // Too big for p0 test, comment it.
        //test for big table(orc format)
        // def q03 = {
        //     qt_q18 """ select c_address from customer where c_custkey = 1 and c_name = 'Customer#000000001'; """
        //     qt_q19 """ select l_quantity from lineitem where l_orderkey = 599614241 and l_partkey = 59018738 and l_suppkey = 1518744 limit 2 """
        //     qt_q20 """ select count(1) from nation """
        //     qt_q21 """ select count(1) from orders """
        //     qt_q22 """ select p_name from part where p_partkey = 4438130 order by p_name limit 1; """
        //     qt_q23 """ select ps_supplycost from partsupp where ps_partkey = 199588198 and ps_suppkey = 9588199 and ps_availqty = 2949 """
        //     qt_q24 """ select * from region order by r_regionkey limit 3 """
        //     qt_q25 """ select s_address from supplier where s_suppkey = 2823947 limit 3"""
        // }
        // sql """ use tpch_1000_orc; """
        // q03()

        // Too big for p0 test, comment it.
        // test #21598
        //qt_pr21598 """select count(*) from( (SELECT r_regionkey AS key1, r_name AS name, pday AS pday FROM (SELECT r_regionkey, r_name, replace(r_comment, ' ', 'aaaa') AS pday FROM ${catalog_name}.tpch_1000_parquet.region) t2))x;"""

        // TODO(kaka11chen): Need to upload table to oss, comment it temporarily.
        // test not_single_slot_filter_conjuncts with dict filter issue
        // qt_not_single_slot_filter_conjuncts_orc """ select * from multi_catalog.lineitem_string_date_orc where l_commitdate < l_receiptdate and l_receiptdate = '1995-01-01'  order by l_orderkey, l_partkey, l_suppkey, l_linenumber limit 10; """
        // qt_not_single_slot_filter_conjuncts_parquet """ select * from multi_catalog.lineitem_string_date_orc where l_commitdate < l_receiptdate and l_receiptdate = '1995-01-01'  order by l_orderkey, l_partkey, l_suppkey, l_linenumber limit 10; """

        // TODO(kaka11chen): Need to upload table to oss, comment it temporarily.
        // test null expr with dict filter issue
        //qt_null_expr_dict_filter_orc """ select count(*), count(distinct user_no) from multi_catalog.dict_fitler_test_orc WHERE `partitions` in ('2023-08-21') and actual_intf_type  =  'type1' and (REUSE_FLAG<> 'y' or REUSE_FLAG is null); """
        //qt_null_expr_dict_filter_parquet """ select count(*), count(distinct user_no) from multi_catalog.dict_fitler_test_parquet WHERE `partitions` in ('2023-08-21') and actual_intf_type  =  'type1' and (REUSE_FLAG<> 'y' or REUSE_FLAG is null); """

        // test par fields in file
        qt_par_fields_in_file_orc1 """ select * from multi_catalog.par_fields_in_file_orc where year = 2023 and month = 8 order by id; """
        qt_par_fields_in_file_parquet1 """ select * from multi_catalog.par_fields_in_file_parquet where year = 2023 and month = 8 order by id; """
        qt_par_fields_in_file_orc2 """ select * from multi_catalog.par_fields_in_file_orc where year = 2023 order by id; """
        qt_par_fields_in_file_parquet2 """ select * from multi_catalog.par_fields_in_file_parquet where year = 2023 order by id; """
        qt_par_fields_in_file_orc3 """ select * from multi_catalog.par_fields_in_file_orc where month = 8 order by id; """
        qt_par_fields_in_file_parquet3 """ select * from multi_catalog.par_fields_in_file_parquet where month = 8 order by id; """
        qt_par_fields_in_file_orc4 """ select * from multi_catalog.par_fields_in_file_orc where month = 8 and year >= 2022 order by id; """
        qt_par_fields_in_file_parquet4 """ select * from multi_catalog.par_fields_in_file_parquet where month = 8 and year >= 2022 order by id; """
        qt_par_fields_in_file_orc5 """ select * from multi_catalog.par_fields_in_file_orc where month = 8 and year = 2022 order by id; """
        qt_par_fields_in_file_parquet5 """ select * from multi_catalog.par_fields_in_file_parquet where month = 8 and year = 2022 order by id; """

        // timestamp with isAdjustedToUTC=true
        qt_parquet_adjusted_utc """select * from multi_catalog.timestamp_with_time_zone order by date_col;"""

        // TODO(kaka11chen): hive docker env throws "Cannot find class 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'",  comment it temporarily.
        // test unsupported input format query
        //try {
        //    sql """ select * from multi_catalog.unsupported_input_format_empty; """
        //} catch (Exception e) {
        //    assertTrue(e.getMessage().contains("Unsupported hive input format: com.hadoop.mapred.DeprecatedLzoTextInputFormat"))
        //}

        // test remember last used database after switch / rename catalog
        sql """switch ${catalog_name};"""

        sql """use test;"""

        def res2 = sql """select count(*) from hive_test limit 10;"""
        logger.info("recoding select: " + res2.toString())

        sql """switch internal;"""

        sql """drop catalog if exists hms"""
        sql """alter catalog ${catalog_name} rename hms;"""

        sql """switch hms;"""

        def res3 = sql """select count(*) from test.hive_test limit 10;"""
        logger.info("recoding select: " + res3.toString())

        sql """alter catalog hms rename ${catalog_name};"""

        // test wrong access controller
        test {
            def tmp_name = "${catalog_name}" + "_wrong"
            sql "drop catalog if exists ${tmp_name}"
            sql """
                create catalog if not exists ${tmp_name} properties (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                    'access_controller.properties.ranger.service.name' = 'hive_wrong',
                    'access_controller.class' = 'org.apache.doris.catalog.authorizer.ranger.hive.RangerHiveAccessControllerFactory'
                );
            """
            exception "Failed to init access controller: bound must be positive"
        }
    }
}
