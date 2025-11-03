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

suite("test_hive_orc", "all_types,p0,external,hive,external_docker,external_docker_hive") {
    // Ensure that all types are parsed correctly
    def select_top50 = {
        qt_select_top50 """select * from orc_all_types order by int_col desc limit 50;"""
    }

    // Ensure that the null map of all types are parsed correctly
    def count_all = {
        qt_count_all """
        select p1_col, p2_col,
        count(tinyint_col),
        count(smallint_col),
        count(int_col),
        count(bigint_col),
        count(boolean_col),
        count(float_col),
        count(double_col),
        count(string_col),
        count(binary_col),
        count(timestamp_col),
        count(decimal_col),
        count(char_col),
        count(varchar_col),
        count(date_col),
        sum(size(list_double_col)),
        sum(size(list_string_col))
        from orc_all_types group by p1_col, p2_col
        order by p1_col, p2_col;
        """
    }

    // Ensure that the SearchArgument works well: LG
    def search_lg_int = {
        qt_search_lg_int """select count(int_col) from orc_all_types where int_col > 999613702;"""
    }

    // Ensure that the SearchArgument works well: IN
    def search_in_int = {
        qt_search_in_int """select count(int_col) from orc_all_types where int_col in (999742610, 999613702);"""
    }

    // Ensure that the SearchArgument works well: MIX
    def search_mix = {
        qt_search_mix """select int_col, decimal_col, date_col from orc_all_types where int_col > 995328433 and decimal_col > 988850.7929 and date_col > date '2018-08-25';"""
    }

    // only partition column selected
    def only_partition_col = {
        qt_only_partition_col """select count(p1_col), count(p2_col) from orc_all_types;"""
    }

    // decimals
    def decimals = {
        qt_decimals1 """select * from orc_decimal_table order by id;"""
        qt_decimals2 """select * from orc_decimal_table where id = 3 order by id;"""
        qt_decimals3 """select * from orc_decimal_table where id < 3 order by id;"""
        qt_decimals4 """select * from orc_decimal_table where id > 3 order by id;"""
    }

    // string col dict plain encoding mixed in different stripes
    def string_col_dict_plain_mixed = {
       qt_string_col_dict_plain_mixed1 """select count(col2) from string_col_dict_plain_mixed_orc where col4 = 'Additional data' and col1 like '%Test%' and col3 like '%2%';"""
       qt_string_col_dict_plain_mixed2 """select count(col2) from string_col_dict_plain_mixed_orc where col4 = 'Additional data' and col3 like '%2%';"""
       qt_string_col_dict_plain_mixed3 """select count(col2) from string_col_dict_plain_mixed_orc where col1 like '%Test%';"""
    }

    def predicate_pushdown = {
        qt_predicate_pushdown1 """ select count(o_orderkey) from tpch1_orc.orders where o_orderkey is not null and (o_orderkey < 100 or o_orderkey > 5999900 or o_orderkey in (1000000, 2000000, 3000000)); """
        qt_predicate_pushdown2 """ select count(o_orderkey) from tpch1_orc.orders where o_orderkey is null or (o_orderkey between 100 and 1000 and o_orderkey not in (200, 300, 400)); """
        qt_predicate_pushdown3 """ select count(o_orderkey) from tpch1_orc.orders where o_orderkey is not null and (o_orderkey < 100 or o_orderkey > 5999900 or o_orderkey = 3000000); """
        qt_predicate_pushdown4 """ select count(o_orderkey) from tpch1_orc.orders where o_orderkey is null or (o_orderkey between 1000000 and 1200000 and o_orderkey != 1100000); """
        qt_predicate_pushdown5 """ SELECT count(o_orderkey) FROM tpch1_orc.orders WHERE (o_orderdate >= '1994-01-01' AND o_orderdate <= '1994-12-31') AND (o_orderpriority = '5-LOW' OR o_orderpriority = '3-MEDIUM') AND o_totalprice > 2000;"""
        qt_predicate_pushdown6 """ SELECT count(o_orderkey) FROM tpch1_orc.orders WHERE o_orderstatus <> 'F' AND o_custkey < 54321; """
        qt_predicate_pushdown7 """ SELECT count(o_orderkey) FROM tpch1_orc.orders WHERE o_comment LIKE '%delayed%' OR o_orderpriority = '1-URGENT'; """
        qt_predicate_pushdown8 """ SELECT count(o_orderkey) FROM tpch1_orc.orders WHERE o_orderkey IN (1000000, 2000000, 3000000) OR o_clerk = 'Clerk#000000470'; """

        qt_predicate_pushdown_in1 """ select count(*)  from orc_all_types where boolean_col in (null); """
        qt_predicate_pushdown_in2 """ select count(*)  from orc_all_types where boolean_col in (null, 0); """
        qt_predicate_pushdown_in3 """ select count(*)  from orc_all_types where boolean_col in (null, 1); """

        def test_col_is_null = { String col ->
            "qt_orc_all_types_${col}_is_null" """ select count(*)  from orc_all_types where ${col} is null; """
        }
        test_col_is_null("tinyint_col")
        test_col_is_null("smallint_col")
        test_col_is_null("int_col")
        test_col_is_null("bigint_col")
        test_col_is_null("boolean_col")
        test_col_is_null("float_col")
        test_col_is_null("double_col")
        test_col_is_null("string_col")
        test_col_is_null("binary_col")
        test_col_is_null("timestamp_col")
        test_col_is_null("decimal_col")
        test_col_is_null("char_col")
        test_col_is_null("varchar_col")
        test_col_is_null("date_col")
    }

    def test_topn = {
        def test_col_topn = { String col -> 
            "qt_orc_all_types_${col}_topn_asc"  """ select  * from  orc_all_types  where  string_col is not null order by ${col},string_col asc limit 10; """
            "qt_orc_all_types_${col}_topn_desc"  """ select * from  orc_all_types  where  string_col is not null order by ${col},string_col desc limit 10; """
        }

        test_col_topn("tinyint_col")
        test_col_topn("smallint_col")
        test_col_topn("int_col")
        test_col_topn("bigint_col")
        test_col_topn("boolean_col")
        test_col_topn("float_col")
        test_col_topn("double_col")
        test_col_topn("string_col")
        test_col_topn("binary_col")
        test_col_topn("timestamp_col")
        test_col_topn("decimal_col")
        test_col_topn("char_col")
        test_col_topn("varchar_col")
        test_col_topn("date_col")
        test_col_topn("p1_col")
        test_col_topn("p2_col")
    }
    def test_topn_abs = {
        def test_col_topn = { String col -> 
            "qt_orc_all_types_${col}_topn_abs_asc"  """ select  * from  orc_all_types  where  string_col is not null order by abs(${col}),string_col asc limit 10; """
            "qt_orc_all_types_${col}_topn_abs_desc"  """ select * from  orc_all_types  where  string_col is not null order by abs(${col}),string_col desc limit 10; """
        }

        test_col_topn("tinyint_col")
        test_col_topn("smallint_col")
        test_col_topn("int_col")
        test_col_topn("bigint_col")
        test_col_topn("boolean_col")
        test_col_topn("float_col")
        test_col_topn("double_col")
        test_col_topn("string_col")
        test_col_topn("binary_col")
        test_col_topn("timestamp_col")
        test_col_topn("decimal_col")
        test_col_topn("char_col")
        test_col_topn("varchar_col")
        test_col_topn("date_col")
        test_col_topn("p1_col")
        test_col_topn("p2_col")
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_orc"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            select_top50()
            count_all()
            search_lg_int()
            search_in_int()
            search_mix()
            only_partition_col()
            decimals()
            string_col_dict_plain_mixed()
            predicate_pushdown()    
            test_topn()
            test_topn_abs()
            
            sql """drop catalog if exists ${catalog_name}"""

            // test old create-catalog syntax for compatibility
            sql """
                create catalog if not exists ${catalog_name} properties (
                    "type"="hms",
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
                );
            """
            sql """use `${catalog_name}`.`default`"""
            select_top50()
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

