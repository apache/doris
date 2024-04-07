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

    sql "drop workload group if exists tag1_wg1;"
    sql "drop workload group if exists tag1_wg2;"
    sql "drop workload group if exists tag2_wg1;"
    sql "drop workload group if exists tag1_wg3;"
    sql "drop workload group if exists tag1_mem_wg1;"
    sql "drop workload group if exists tag1_mem_wg2;"
    sql "drop workload group if exists tag1_mem_wg3;"
    sql "drop workload group if exists bypass_group;"

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
    sql "ADMIN SET FRONTEND CONFIG ('enable_alter_queue_prop_sync' = 'true');"
    sql "ADMIN SET FRONTEND CONFIG ('query_queue_update_interval_ms' = '100');"

    sql "create workload group if not exists normal " +
            "properties ( " +
            "    'cpu_share'='1024', " +
            "    'memory_limit'='50%', " +
            "    'enable_memory_overcommit'='true' " +
            ");"

    // reset normal group property
    sql "alter workload group normal properties ( 'cpu_share'='1024' );"
    sql "alter workload group normal properties ( 'memory_limit'='50%' );"
    sql "alter workload group normal properties ( 'enable_memory_overcommit'='true' );"
    sql "alter workload group normal properties ( 'max_concurrency'='2147483647' );"
    sql "alter workload group normal properties ( 'max_queue_size'='0' );"
    sql "alter workload group normal properties ( 'queue_timeout'='0' );"
    sql "alter workload group normal properties ( 'cpu_hard_limit'='1%' );"
    sql "alter workload group normal properties ( 'scan_thread_num'='-1' );"

    sql "set workload_group=normal;"

    // test cpu_share
    qt_cpu_share """ select count(1) from ${table_name} """

    sql "alter workload group normal properties ( 'scan_thread_num'='16' );"
    sql "alter workload group normal properties ( 'cpu_share'='20' );"

    qt_cpu_share_2 """ select count(1) from ${table_name} """

    test {
        sql "alter workload group normal properties ( 'cpu_share'='-2' );"

        exception "requires a positive integer"
    }

    test {
        sql "alter workload group normal properties ( 'scan_thread_num'='0' );"

        exception "scan_thread_num must be a positive integer or -1"
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

    qt_show_1 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num,tag from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test memory_limit
    test {
        sql "alter workload group test_group properties ( 'memory_limit'='100%' );"

        exception "cannot be greater than"
    }

    sql "alter workload group test_group properties ( 'memory_limit'='11%' );"
    qt_mem_limit_1 """ select count(1) from ${table_name} """
    qt_mem_limit_2 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test enable_memory_overcommit
    test {
        sql "alter workload group test_group properties ( 'enable_memory_overcommit'='1' );"

        exception "must be true or false"
    }

    sql "alter workload group test_group properties ( 'enable_memory_overcommit'='true' );"
    qt_mem_overcommit_1 """ select count(1) from ${table_name} """
    sql "alter workload group test_group properties ( 'enable_memory_overcommit'='false' );"
    qt_mem_overcommit_2 """ select count(1) from ${table_name} """
    qt_mem_overcommit_3 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test cpu_hard_limit
    test {
        sql "alter workload group test_group properties ( 'cpu_hard_limit'='101%' );"

        exception "must be a positive integer"
    }

    sql "alter workload group test_group properties ( 'cpu_hard_limit'='99%' );"

    test {
        sql "alter workload group normal properties ( 'cpu_hard_limit'='2%' );"

        exception "can not be greater than 100%"
    }

    sql "alter workload group test_group properties ( 'cpu_hard_limit'='20%' );"
    qt_cpu_hard_limit_1 """ select count(1) from ${table_name} """
    qt_cpu_hard_limit_2 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

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
    qt_show_queue "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

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
                "    'enable_memory_overcommit'='1', " +
                " 'cpu_hard_limit'='1%' " +
                ");"

        exception "must be true or false"
    }

    // failed for cpu_hard_limit
    test {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='3%', " +
                "    'enable_memory_overcommit'='true', " +
                " 'cpu_hard_limit'='120%' " +
                ");"

        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists test_group2 " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='3%', " +
                "    'enable_memory_overcommit'='true', " +
                " 'cpu_hard_limit'='99%' " +
                ");"

        exception "can not be greater than 100%"
    }

    // test show workload groups
    qt_select_tvf_1 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test auth
    sql """drop user if exists test_wlg_user"""
    sql "CREATE USER 'test_wlg_user'@'%' IDENTIFIED BY '12345';"
    sql """grant SELECT_PRIV on *.*.* to test_wlg_user;"""
    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
            sql """ select count(1) from information_schema.backend_active_tasks; """
    }

    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        test {
            sql """ select count(1) from information_schema.backend_active_tasks; """
            exception "Access denied"
        }
    }

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_group' TO 'test_wlg_user'@'%';"

    connect(user = 'test_wlg_user', password = '12345', url = context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        sql """ select count(1) from information_schema.backend_active_tasks; """
    }

    // test query queue limit
    sql "set workload_group=test_group;"
    sql "alter workload group test_group properties ( 'max_concurrency'='0' );"
    sql "alter workload group test_group properties ( 'max_queue_size'='0' );"
    Thread.sleep(10000)
    test {
        sql "select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ * from ${table_name};"

        exception "query waiting queue is full"
    }

    // test insert into select will go to queue
    test {
        sql "insert into ${table_name2} select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ * from ${table_name};"

        exception "query waiting queue is full"
    }

    // test create table as select will go to queue
    test {
        sql "create table ${table_name3} PROPERTIES('replication_num' = '1') as select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ * from ${table_name};"

        exception "query waiting queue is full"
    }

    sql "alter workload group test_group properties ( 'max_queue_size'='1' );"
    sql "alter workload group test_group properties ( 'queue_timeout'='500' );"
    Thread.sleep(10000)
    test {
        sql "select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ * from ${table_name};"

        exception "query wait timeout"
    }

    // test query queue running query/waiting num
    sql "drop workload group if exists test_query_num_wg;"
    sql "create workload group if not exists test_query_num_wg properties ('max_concurrency'='1');"
    sql "set workload_group=test_query_num_wg;"

    sql """insert into ${table_name} values
        (9,10,11,12),
        (1,2,3,4)
    """

    sql """insert into ${table_name} values
        (9,10,11,12),
        (1,2,3,4)
    """

    sql """insert into ${table_name} values
        (9,10,11,12),
        (1,2,3,4)
    """

    sql "select count(1) from ${table_name};"

    def ret = sql "show workload groups;"
    assertTrue(ret.size() != 0)
    boolean is_checked = false;
    for (int i = 0; i < ret.size(); i++) {
        def row = ret[i]
        if (row[1] == 'test_query_num_wg') {
            int running_query_num = Integer.parseInt(row[row.size() - 2])
            int wait_query_num = Integer.parseInt(row[row.size() - 1])
            assertTrue(running_query_num == 0)
            assertTrue(wait_query_num == 0)
            is_checked = true;
        }
    }
    assertTrue(is_checked)

    sql "drop workload group test_query_num_wg;"

    sql "set workload_group=normal;"
    sql "alter workload group test_group properties ( 'max_concurrency'='10' );"
    Thread.sleep(10000)
    sql "select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ * from ${table_name};"

    // test workload spill property
    // 1 create group
    test {
        sql "create workload group if not exists spill_group_test_failed properties (  'spill_threshold_low_watermark'='90%');"
        exception "should bigger than spill_threshold_low_watermark"
    }
    sql "create workload group if not exists spill_group_test properties (  'spill_threshold_low_watermark'='10%','spill_threshold_high_watermark'='10%');"
    qt_show_spill_1 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num,spill_threshold_low_watermark,spill_threshold_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"

    test {
        sql "create workload group if not exists spill_group_test properties (  'spill_threshold_low_watermark'='20%','spill_threshold_high_watermark'='10%');"
        exception "should bigger than spill_threshold_low_watermark"
    }

    // 2 alter low
    sql "alter workload group spill_group_test properties ( 'spill_threshold_low_watermark'='-1' );"
    qt_show_spill_1 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num,spill_threshold_low_watermark,spill_threshold_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"

    sql "alter workload group spill_group_test properties ( 'spill_threshold_low_watermark'='5%' );"
    qt_show_spill_2 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num,spill_threshold_low_watermark,spill_threshold_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"

    test {
        sql "alter workload group spill_group_test properties ( 'spill_threshold_low_watermark'='20%' );"
        exception "should bigger than spill_threshold_low_watermark"
    }

    test {
        sql "alter workload group spill_group_test properties ( 'spill_threshold_low_watermark'='0%' );"
        exception "must be a positive integer"
    }

    test {
        sql "alter workload group spill_group_test properties ( 'spill_threshold_low_watermark'='101%' );"
        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists spill_group_test2 properties (  'spill_threshold_low_watermark'='0%')"
        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists spill_group_test2 properties (  'spill_threshold_low_watermark'='101%')"
        exception "must be a positive integer"
    }

    // 3 alter high
    sql "alter workload group spill_group_test properties ( 'spill_threshold_high_watermark'='40%' );"
    qt_show_spill_3 "select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num,spill_threshold_low_watermark,spill_threshold_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"
    test {
        sql "alter workload group spill_group_test properties ( 'spill_threshold_high_watermark'='1%' );"
        exception "should bigger than spill_threshold_low_watermark"
    }

    test {
        sql "alter workload group spill_group_test properties ( 'spill_threshold_high_watermark'='0%' );"
        exception "must be a positive integer"
    }

    test {
        sql "alter workload group spill_group_test properties ( 'spill_threshold_high_watermark'='101%' );"
        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists spill_group_test2 properties (  'spill_threshold_high_watermark'='0%')"
        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists spill_group_test2 properties (  'spill_threshold_high_watermark'='101%')"
        exception "must be a positive integer"
    }

    sql "drop workload group test_group;"
    sql "drop workload group spill_group_test;"


    // test workload group's tag property, cpu_hard_limit
    test {
        sql "create workload group if not exists tag1_wg1 properties (  'cpu_hard_limit'='101%', 'tag'='tag1')"
        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists tag1_wg1 properties (  'cpu_hard_limit'='-2%', 'tag'='tag1')"
        exception "must be a positive integer"
    }

    test {
        sql "create workload group if not exists tag1_wg1 properties (  'cpu_hard_limit'='-1%', 'tag'='tag1')"
        exception "must be a positive integer"
    }

    sql "create workload group if not exists tag1_wg1 properties (  'cpu_hard_limit'='10%', 'tag'='tag1');"

    test {
        sql "create workload group if not exists tag1_wg2 properties (  'cpu_hard_limit'='91%', 'tag'='tag1');"
        exception "can not be greater than 100%"
    }

    sql "create workload group if not exists tag1_wg2 properties (  'cpu_hard_limit'='10%', 'tag'='tag1');"

    sql "create workload group if not exists tag2_wg1 properties (  'cpu_hard_limit'='91%', 'tag'='tag2');"

    test {
        sql "alter workload group tag2_wg1 properties ( 'tag'='tag1' );"
        exception "can not be greater than 100% "
    }

    sql "alter workload group tag2_wg1 properties ( 'cpu_hard_limit'='10%' );"
    sql "alter workload group tag2_wg1 properties ( 'tag'='tag1' );"

    test {
        sql "create workload group if not exists tag1_wg3 properties (  'cpu_hard_limit'='80%', 'tag'='tag1');"
        exception "can not be greater than 100% "
    }

    sql "drop workload group tag2_wg1;"
    sql "create workload group if not exists tag1_wg3 properties (  'cpu_hard_limit'='80%', 'tag'='tag1');"

    // test workload group's tag property, memory_limit
    sql "create workload group if not exists tag1_mem_wg1 properties (  'memory_limit'='50%', 'tag'='mem_tag1');"

    test {
        sql "create workload group if not exists tag1_mem_wg2 properties (  'memory_limit'='60%', 'tag'='mem_tag1');"
        exception "cannot be greater than 100.0%"
    }

    sql "create workload group if not exists tag1_mem_wg2 properties ('memory_limit'='49%', 'tag'='mem_tag1');"

    sql "create workload group if not exists tag1_mem_wg3 properties (  'memory_limit'='2%');"

    test {
        sql "alter workload group tag1_mem_wg3 properties ( 'tag'='mem_tag1' );"
        exception "cannot be greater than 100.0%"
    }

    sql "alter workload group tag1_mem_wg3 properties ( 'memory_limit'='1%' );"

    sql "alter workload group tag1_mem_wg3 properties ( 'tag'='mem_tag1' );"

    qt_show_wg_tag "select name,MEMORY_LIMIT,CPU_HARD_LIMIT,TAG from information_schema.workload_groups where name in('tag1_wg1','tag1_wg2','tag2_wg1','tag1_wg3','tag1_mem_wg1','tag1_mem_wg2','tag1_mem_wg3') order by tag,name;"

    // test bypass
    sql "create workload group if not exists bypass_group properties (  'max_concurrency'='0','max_queue_size'='0','queue_timeout'='0');"
    sql "set workload_group=bypass_group;"
    test {
        sql "select count(1) from information_schema.active_queries;"
        exception "query waiting queue is full"
    }

    sql "set bypass_workload_group = true;"
    sql "select count(1) from information_schema.active_queries;"

    sql "drop workload group tag1_wg1;"
    sql "drop workload group tag1_wg2;"
    sql "drop workload group if exists tag2_wg1;"
    sql "drop workload group tag1_wg3;"
    sql "drop workload group tag1_mem_wg1;"
    sql "drop workload group tag1_mem_wg2;"
    sql "drop workload group tag1_mem_wg3;"
    sql "drop workload group bypass_group;"

}
