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
    def table_name2 = "wlg_test_table2"
    def table_name3 = "wlg_test_table3"

    sql "drop table if exists ${table_name}"
    sql "drop table if exists ${table_name2}"
    sql "drop table if exists ${table_name3}"

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

    sql """
        CREATE TABLE IF NOT EXISTS `${table_name2}` (
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

    // reset normal group property
    sql "alter workload group normal properties ( 'cpu_share'='1024' );"
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

    test {
        sql "alter workload group normal properties ( 'cpu_share'='-2' );"

        exception "requires a positive integer"
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

    qt_show_1 "select name,Item,Value from workload_groups() where name in ('normal','test_group') order by name,Item;"

    // test drop workload group
    sql "create workload group if not exists test_drop_wg properties ('cpu_share'='10', 'memory_limit'='10%')"
    qt_show_del_wg_1 "select name,Item,Value from workload_groups() where name in ('normal','test_group','test_drop_wg') order by name,Item;"
    sql "drop workload group test_drop_wg"
    qt_show_del_wg_2 "select name,Item,Value from workload_groups() where name in ('normal','test_group','test_drop_wg') order by name,Item;"

    // test memory_limit
    test {
        sql "alter workload group test_group properties ( 'memory_limit'='100%' );"

        exception "cannot be greater than"
    }

    sql "alter workload group test_group properties ( 'memory_limit'='11%' );"
    qt_mem_limit_1 """ select count(1) from ${table_name} """
    qt_mem_limit_2 "select name,Item,Value from workload_groups() where name in ('normal','test_group') order by name,Item;"

    // test enable_memory_overcommit
    test {
        sql "alter workload group test_group properties ( 'enable_memory_overcommit'='1' );"

        exception "must be true or false"
    }

    sql "alter workload group test_group properties ( 'enable_memory_overcommit'='true' );"
    qt_mem_overcommit_1 """ select count(1) from ${table_name} """
    sql "alter workload group test_group properties ( 'enable_memory_overcommit'='false' );"
    qt_mem_overcommit_2 """ select count(1) from ${table_name} """
    qt_mem_overcommit_3 "select name,Item,Value from workload_groups() where name in ('normal','test_group') order by name,Item;"

    // test query queue
    test {
        sql "alter workload group test_group properties ( 'max_concurrency'='-1' );"

        exception "requires a positive integer"
    }

    test {
        sql "alter workload group test_group properties ( 'max_queue_size'='-1' );"

        exception "requires a positive integer"
    }

    test {
        sql "alter workload group test_group properties ( 'queue_timeout'='-1' );"

        exception "requires a positive integer"
    }

    sql "alter workload group test_group properties ( 'max_concurrency'='100' );"
    qt_queue_1 """ select count(1) from ${table_name} """
    qt_show_queue "select name,Item,Value from workload_groups() where name in ('normal','test_group') order by name,Item;"

    // test create group failed
    // failed for cpu_share
    test {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='-1', " +
                "    'memory_limit'='1%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"

        exception "requires a positive integer"
    }

    // failed for mem_limit
    test {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='200%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"

        exception "cannot be greater than"
    }

    test {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='99%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"

        exception "cannot be greater than"
    }


    // failed for mem_overcommit
    test {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='3%', " +
                "    'enable_memory_overcommit'='1' " +
                ");"

        exception "must be true or false"
    }

    // test show workload groups
    qt_select_tvf_1 "select name,Item,Value from workload_groups() where name in ('normal','test_group') order by name,Item;"

    // test auth
    sql """drop user if exists test_wlg_user"""
    sql "CREATE USER 'test_wlg_user'@'%' IDENTIFIED BY '12345';"
    sql """grant SELECT_PRIV on *.*.* to test_wlg_user;"""
    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
            sql """ select count(1) from information_schema.tables; """
    }

    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        test {
            sql """ select count(1) from information_schema.tables; """
            exception "Access denied"
        }
    }

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_group' TO 'test_wlg_user'@'%';"

    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        sql """ select count(1) from information_schema.tables; """
    }

    // test query queue limit
    sql "set workload_group=test_group;"
    sql "alter workload group test_group properties ( 'max_concurrency'='0' );"
    sql "alter workload group test_group properties ( 'max_queue_size'='0' );"
    Thread.sleep(10000)
    test {
        sql "select /*+SET_VAR(workload_group=test_group)*/ * from ${table_name};"

        exception "query waiting queue is full"
    }

    // test insert into select will go to queue
    test {
        sql "insert into ${table_name2} select /*+SET_VAR(workload_group=test_group)*/ * from ${table_name};"

        exception "query waiting queue is full"
    }

    // test create table as select will go to queue
    test {
        sql "create table ${table_name3} PROPERTIES('replication_num' = '1') as select /*+SET_VAR(workload_group=test_group)*/ * from ${table_name};"

        exception "query waiting queue is full"
    }

    sql "alter workload group test_group properties ( 'max_queue_size'='1' );"
    sql "alter workload group test_group properties ( 'queue_timeout'='500' );"
    Thread.sleep(10000)
    test {
        sql "select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ * from ${table_name};"

        exception "query wait timeout"
    }
}
