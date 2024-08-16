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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

import java.util.stream.Collectors

suite("sql_cache") {
    // TODO: regression-test does not support check query profile,
    // so this suite does not check whether cache is used, :)
    def tableName = "test_sql_cache"
    sql  "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '0')"

    def variables = sql "show variables"
    def variableString = variables.stream()
            .map { it.toString() }
            .collect(Collectors.joining("\n"))
    logger.info("Variables:\n${variableString}")

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` date NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202205 VALUES [('2022-05-01'), ('2022-06-01')),
            PARTITION p202206 VALUES [('2022-06-01'), ('2022-07-01')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """

    sql "sync"

    sql """ INSERT INTO ${tableName} VALUES 
                    ("2022-05-27",0),
                    ("2022-05-28",0),
                    ("2022-05-29",0),
                    ("2022-05-30",0),
                    ("2022-06-01",0),
                    ("2022-06-02",0)
        """

    qt_sql_cache1 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-06-30' 
                    group by
                        k1 
                    order by
                        k1;
                """
    
    sql "set enable_sql_cache=true "

    qt_sql_cache2 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-06-30' 
                    group by
                        k1 
                    order by
                        k1;
                """
    qt_sql_cache3 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-06-30' 
                    group by
                        k1 
                    order by
                        k1;
                """

    qt_sql_cache4 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-05-28'
                    group by
                        k1 
                    order by
                        k1
                    union all
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-05-28'
                    group by
                        k1 
                    order by
                        k1;
                """
    
    qt_sql_cache5 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-05-28'
                    group by
                        k1 
                    order by
                        k1
                    union all
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-05-28'
                    group by
                        k1 
                    order by
                        k1;
                """

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    qt_sql_cache6 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-06-30' 
                    group by
                        k1 
                    order by
                        k1;
                """
    qt_sql_cache7 """
                    select
                        k1,
                        sum(k2) as total_pv 
                    from
                        ${tableName} 
                    where
                        k1 between '2022-05-28' and '2022-06-30' 
                    group by
                        k1 
                    order by
                        k1;
                """

    sql 'set default_order_by_limit = 2'
    sql 'set sql_select_limit = 1'

    profile("sql_cache8") {
        run {
            qt_sql_cache8 """
                -- sql_cache8
                select
                    k1,
                    sum(k2) as total_pv 
                from
                    ${tableName} 
                where
                    k1 between '2022-05-28' and '2022-06-30' 
                group by
                    k1 
                order by
                    k1;
            """
        }

        check { profileString, exception ->
            if (!exception.is(null)) {
                logger.error("Profile failed, profile result:\n${profileString}", exception)
                throw exception
            }
        }
    }

    profile("sql_cache9") {
        run {
            qt_sql_cache9 """
                -- sql_cache9
                select
                    k1,
                    sum(k2) as total_pv 
                from
                    ${tableName} 
                where
                    k1 between '2022-05-28' and '2022-06-30' 
                group by
                    k1 
                order by
                    k1;
            """
        }

        check { profileString, exception ->
            if (!exception.is(null)) {
                logger.error("Profile failed, profile result:\n${profileString}", exception)
                throw exception
            }
        }
    }

    sql  "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '10')"

    // explain plan with sql cache
    connect {
        sql "set enable_sql_cache=true"
        sql "select 100"
        sql "explain plan select 100"
    }
}
