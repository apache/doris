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

suite("test_hive_query_cache", "p0,external,hive,external_docker,external_docker_hive") {

    def q01 = {
        qt_q24 """ select name, count(1) as c from student group by name order by c desc;"""
        qt_q25 """ select lo_orderkey, count(1) as c from lineorder group by lo_orderkey order by c desc;"""
        qt_q26 """ select * from test1 order by col_1;"""
        qt_q27 """ select * from string_table order by p_partkey desc;"""
        qt_q28 """ select * from account_fund order by batchno;"""
        qt_q29 """ select * from sale_table order by bill_code limit 01;"""
        order_qt_q30 """ select count(card_cnt) from hive01;"""
        qt_q31 """ select * from test2 order by id;"""
        qt_q32 """ select * from test_hive_doris order by id;"""

        qt_q33 """ select dt, k1, * from table_with_vertical_line order by dt desc, k1 desc limit 10;"""
        order_qt_q34 """ select dt, k2 from table_with_vertical_line order by k2 desc limit 10;"""
        qt_q35 """ select dt, k2 from table_with_vertical_line where dt='2022-11-24' order by k2 desc limit 10;"""
        qt_q36 """ select k2, k5 from table_with_vertical_line where dt='2022-11-25' order by k2 desc limit 10;"""
        order_qt_q37 """ select count(*) from table_with_vertical_line;"""
        qt_q38 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25') order by k2 desc limit 10;"""
        qt_q39 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25', '2022-11-24') order by k2 desc limit 10;"""
        qt_q40 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-25') order by k2 desc limit 10;"""
        qt_q41 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') and dt in ('2022-11-24') order by k2 desc limit 10;"""
        qt_q42 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-24') order by k2 desc limit 10;"""

        qt_q43 """ select dt, k1, * from table_with_x01 order by dt desc, k1 desc limit 10;"""
        qt_q44 """ select dt, k2 from table_with_x01 order by k2 desc limit 10;"""
        qt_q45 """ select dt, k2 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        qt_q46 """ select k2, k5 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        order_qt_q47 """ select count(*) from table_with_x01;"""
        qt_q48 """ select k2, k5 from table_with_x01 where dt in ('2022-11-25') order by k2 desc limit 10;"""
        qt_q49 """ select k2, k5 from table_with_x01 where dt in ('2022-11-10', '2022-11-10') order by k2 desc limit 10;"""
        qt_q50 """ select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
        qt_q51 """ select col_2 from test1;"""
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        String hdfs_port = context.config.otherConfigs.get("hdfs_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String catalog_name = "hive_test_query_cache"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """switch ${catalog_name}"""

        sql """set enable_fallback_to_original_planner=false"""

        def tpch_1sf_q09 = """
            select
                nation,
                o_year,
                sum(amount) as sum_profit
            from
                (
                    select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                    from
                            lineitem join[shuffle] orders on o_orderkey = l_orderkey
                            join[shuffle] partsupp on ps_suppkey = l_suppkey and ps_partkey = l_partkey
                            join[shuffle] part on p_partkey = l_partkey and p_name like '%green%'
                            join supplier on s_suppkey = l_suppkey
                            join nation on s_nationkey = n_nationkey
                ) as profit
            group by
                nation,
                o_year
            order by
                nation,
                o_year desc;
        """

        // // test sql cache
        sql """admin set frontend config("cache_last_version_interval_second" = "1");"""
        sql """use `tpch1_parquet`"""
        qt_tpch_1sf_q09 "${tpch_1sf_q09}"
        sql "${tpch_1sf_q09}"

        test {
            sql "${tpch_1sf_q09}"
            time 10000
        }

        // test sql cache with empty result
        try {
            sql """set enable_sql_cache=true;"""
            sql """set test_query_cache_hit="none";"""
            sql """select * from lineitem where l_suppkey="abc";""" // non exist l_suppkey;
            sql """select * from lineitem where l_suppkey="abc";"""
        } catch (java.sql.SQLException t) {
            print t.getMessage()
            assertTrue(1 == 2)
        }

        // test more sql cache
        sql """use `default`"""
        sql """set enable_sql_cache=true;"""
        sql """set test_query_cache_hit="none";"""
        // 1. first query, because we need to init the schema of table_with_x01 to update the table's update time
        // then sleep 2 seconds to wait longer than Config.cache_last_version_interval_second,
        // so that when doing the second query, we can fill the cache on BE
        qt_sql1 """select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
        sleep(2000);
        // 2. second query is for filling the cache on BE
        qt_sql2 """select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
        // 3. third query, to test cache hit.
        sql """set test_query_cache_hit="sql";"""
        qt_sql3 """select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""

        // test not hit
        try {
            sql """set enable_sql_cache=true;"""
            sql """set test_query_cache_hit="sql";"""
            def r = UUID.randomUUID().toString();
            // using a random sql
            sql """select dt, "${r}" from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
            assertTrue(1 == 2)
        } catch (Exception t) {
            print t.getMessage()
            assertTrue(t.getMessage().contains("but the query cache is not hit"));
        }
    }
}
