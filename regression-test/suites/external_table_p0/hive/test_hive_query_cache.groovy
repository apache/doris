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

    def assertHasCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            contains("PhysicalSqlCache")
        }
    }

    def assertNoCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            notContains("PhysicalSqlCache")
        }
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String catalog_name = "${hivePrefix}_test_query_cache"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """switch ${catalog_name}"""

        sql """set enable_fallback_to_original_planner=false"""
        sql """set enable_sql_cache=false;"""

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
            time 20000
        }

        // test sql cache with empty result
        try {
            sql """set enable_sql_cache=true;"""
            sql """select * from lineitem where l_suppkey="abc";""" // non exist l_suppkey;
            sql """select * from lineitem where l_suppkey="abc";"""
        } catch (java.sql.SQLException t) {
            print t.getMessage()
            assertTrue(1 == 2)
        }

        // version 3.1 does not support hive sql cache with planner.
        // so the following cases check nothing, just make sure everything works normally when set enable_sql_cache=true;
        // test more sql cache
        sql """use `default`"""
        sql """set enable_sql_cache=true;"""
        // 1. first query, because we need to init the schema of table_with_x01 to update the table's update time
        // then sleep 2 seconds to wait longer than Config.cache_last_version_interval_second,
        // so that when doing the second query, we can fill the cache on BE
        qt_sql1 """select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
        sleep(2000);
        // 2. second query is for filling the cache on BE
        qt_sql2 """select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
        // 3. third query, to test cache hit.
        qt_sql3 """select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""

        // test not hit
        sql """set enable_sql_cache=true;"""
        def r = UUID.randomUUID().toString();
        // using a random sql
        sql """select dt, "${r}" from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
    }
}
