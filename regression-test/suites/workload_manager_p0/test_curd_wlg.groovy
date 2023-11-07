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

suite("test_crud_wlg") {
    def table_name = "wlg_test_table"

    sql "drop table if exists ${table_name}"

    sql """
        CREATE TABLE IF NOT EXISTS `${table_name}` (
          `siteid` int(11) NOT NULL COMMENT "",
          `citycode` int(11) NOT NULL COMMENT "",
          `userid` int(11) NOT NULL COMMENT "",
          `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """

    sql """insert into ${table_name} values
        (9,10,11,12),
        (1,2,3,4)
    """

    sql "ADMIN SET FRONTEND CONFIG ('enable_workload_group' = 'true');"

    sql "create workload group if not exists normal " +
            "properties ( " +
            "    'cpu_share'='10', " +
            "    'memory_limit'='50%', " +
            "    'enable_memory_overcommit'='true' " +
            ");"

    // reset normal group property
    sql "alter workload group normal properties ( 'cpu_share'='10' );"
    sql "alter workload group normal properties ( 'memory_limit'='50%' );"
    sql "alter workload group normal properties ( 'enable_memory_overcommit'='true' );"
    sql "alter workload group normal properties ( 'max_concurrency'='2147483647' );"
    sql "alter workload group normal properties ( 'max_queue_size'='0' );"
    sql "alter workload group normal properties ( 'queue_timeout'='0' );"

    sql "set workload_group=normal;"

    // test cpu_share
    qt_cpu_share """ select count(1) from ${table_name} """

    sql "alter workload group normal properties ( 'cpu_share'='20' );"

    qt_cpu_share_2 """ select count(1) from ${table_name} """

    try {
        sql "alter workload group normal properties ( 'cpu_share'='-2' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("requires a positive integer"));
    }

    sql "drop workload group if exists test_group;"

    // test create group
    sql "create workload group if not exists test_group " +
            "properties ( " +
            "    'cpu_share'='10', " +
            "    'memory_limit'='10%', " +
            "    'enable_memory_overcommit'='true' " +
            ");"
    sql "set workload_group=test_group;"

    qt_show_1 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit from workload_groups() order by name;"

    // test memory_limit
    try {
        sql "alter workload group test_group properties ( 'memory_limit'='100%' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("cannot be greater than"));
    }
    sql "alter workload group test_group properties ( 'memory_limit'='11%' );"
    qt_mem_limit_1 """ select count(1) from ${table_name} """
    qt_mem_limit_2 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit from workload_groups() order by name;"

    // test enable_memory_overcommit
    try {
        sql "alter workload group test_group properties ( 'enable_memory_overcommit'='1' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("must be true or false"));
    }
    sql "alter workload group test_group properties ( 'enable_memory_overcommit'='true' );"
    qt_mem_overcommit_1 """ select count(1) from ${table_name} """
    sql "alter workload group test_group properties ( 'enable_memory_overcommit'='false' );"
    qt_mem_overcommit_2 """ select count(1) from ${table_name} """
    qt_mem_overcommit_3 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit from workload_groups() order by name;"

    // test cpu_hard_limit
    try {
        sql "alter workload group test_group properties ( 'cpu_hard_limit'='101%' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("can not be greater than 100%"));
    }

    try {
        sql "alter workload group test_group properties ( 'cpu_hard_limit'='99%' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("can not be greater than 100%"));
    }

    sql "alter workload group test_group properties ( 'cpu_hard_limit'='20%' );"
    qt_cpu_hard_limit_1 """ select count(1) from ${table_name} """
    qt_cpu_hard_limit_2 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit from workload_groups() order by name;"

    // test query queue
    try {
        sql "alter workload group test_group properties ( 'max_concurrency'='-1' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("requires a positive integer"));
    }

    try {
        sql "alter workload group test_group properties ( 'max_queue_size'='-1' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("requires a positive integer"));
    }

    try {
        sql "alter workload group test_group properties ( 'queue_timeout'='-1' );"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("requires a positive integer"));
    }
    sql "alter workload group test_group properties ( 'max_concurrency'='0' );"
    sql "alter workload group test_group properties ( 'max_queue_size'='0' );"
    sql "alter workload group test_group properties ( 'queue_timeout'='0' );"
    try {
        sql "select count(1) from ${table_name}"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("queue failed"));
    }

    sql "alter workload group test_group properties ( 'max_concurrency'='100' );"
    qt_queue_1 """ select count(1) from ${table_name} """
    qt_show_queue "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit from workload_groups() order by name;"

    // test create group failed
    // failed for cpu_share
    try {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='-1', " +
                "    'memory_limit'='1%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("requires a positive integer"));
    }
    // failed for mem_limit
    try {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='200%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("cannot be greater than"))
    }

    try {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='99%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("cannot be greater than"))
    }


    // failed for mem_overcommit
    try {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='3%', " +
                "    'enable_memory_overcommit'='1', " +
                " 'cpu_hard_limit'='1%' " +
                ");"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("must be true or false"));
    }

    // failed for cpu_hard_limit
    try {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='3%', " +
                "    'enable_memory_overcommit'='true', " +
                " 'cpu_hard_limit'='120%' " +
                ");"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("can not be greater than"));
    }

    try {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='3%', " +
                "    'enable_memory_overcommit'='true', " +
                " 'cpu_hard_limit'='99%' " +
                ");"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("can not be greater than"));
    }

    // test show workload groups
    qt_select_tvf_1 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit from workload_groups() order by name;"

    qt_select_tvf_2 "select name, waiting_query_num,running_query_num,cpu_hard_limit from workload_groups() where name='test_group' order by name;"

    // test auth
    sql """drop user if exists test_wlg_user"""
    sql "CREATE USER 'test_wlg_user'@'%' IDENTIFIED BY '12345';"
    sql """grant SELECT_PRIV on *.*.* to test_wlg_user;"""
    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
            sql """ select 1; """
    }

    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        try {
            sql """ select 1; """
        } catch (Exception e) {
            e.getMessage().contains("Access denied")
        }
    }

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_group' TO 'test_wlg_user'@'%';"

    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        sql """ select 1; """
    }

}
