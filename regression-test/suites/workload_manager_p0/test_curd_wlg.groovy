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

def getMetrics = { ip, port ->
        def dst = 'http://' + ip + ':' + port
        def conn = new URL(dst + "/metrics").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

suite("test_crud_wlg") {
    def table_name = "wlg_test_table"
    def table_name2 = "wlg_test_table2"
    def table_name3 = "wlg_test_table3"

    sql "drop table if exists ${table_name}"
    sql "drop table if exists ${table_name2}"
    sql "drop table if exists ${table_name3}"

    def forComputeGroupStr = "";

    String computeGroupName = "default"

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        forComputeGroupStr = " for  $validCluster "
        computeGroupName = validCluster
    }

    sql "drop workload group if exists bypass_group $forComputeGroupStr;"

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
    sql "ADMIN SET FRONTEND CONFIG ('query_queue_update_interval_ms' = '100');"

    // reset normal group property
    sql "alter workload group normal $forComputeGroupStr properties ( 'min_cpu_percent'='20%' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'max_memory_percent'='50%' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'max_concurrency'='2147483647' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'max_queue_size'='0' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'queue_timeout'='0' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'max_cpu_percent'='30%' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'scan_thread_num'='-1' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'scan_thread_num'='-1' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'remote_read_bytes_per_second'='-1' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'read_bytes_per_second'='-1' );"

    sql "set workload_group=normal;"

    // test min_cpu_percent
    qt_min_cpu_percent """ select count(1) from ${table_name} """

    sql "alter workload group normal $forComputeGroupStr properties ( 'scan_thread_num'='16' );"
    sql "alter workload group normal $forComputeGroupStr properties ( 'min_cpu_percent'='20' );"

    qt_min_cpu_percent_2 """ select count(1) from ${table_name} """

    test {
        sql "alter workload group normal $forComputeGroupStr properties ( 'min_cpu_percent'='-2' );"

        exception "The allowed min_cpu_percent value has to be in [0,100]"
    }

    test {
        sql "alter workload group normal $forComputeGroupStr properties ( 'scan_thread_num'='0' );"

        exception "The allowed scan_thread_num value is -1 or a positive integer"
    }

    sql "drop workload group if exists test_group $forComputeGroupStr;"

    // test create group
    sql "create workload group if not exists test_group $forComputeGroupStr " +
            "properties ( " +
            "    'min_cpu_percent'='10', " +
            "    'max_memory_percent'='10%' " +
            ");"
    sql "set workload_group=test_group;"

    qt_show_1 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,read_bytes_per_second,remote_read_bytes_per_second from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    def query_cg_result  = sql "select distinct compute_group from information_schema.workload_groups where name in ('normal','test_group')"
    String cg_name = query_cg_result[0][0]
    if (!computeGroupName.equals(cg_name)) {
        logger.info("expected:" + computeGroupName + ", real: $query_cg_result, " + cg_name)
        assertTrue(false)
    }

    // test drop workload group
    sql "create workload group if not exists test_drop_wg $forComputeGroupStr properties ('min_cpu_percent'='10')"
    qt_show_del_wg_1 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group','test_drop_wg') order by name;"
    sql "drop workload group test_drop_wg $forComputeGroupStr"
    qt_show_del_wg_2 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group','test_drop_wg') order by name;"


    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_memory_percent'='11%' );"
    qt_mem_limit_1 """ select count(1) from ${table_name} """
    qt_mem_limit_2 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"


    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_cpu_percent'='99%' );"

    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_cpu_percent'='20%' );"
    qt_max_cpu_percent_1 """ select count(1) from ${table_name} """
    qt_max_cpu_percent_2 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test query queue
    test {
        sql "alter workload group test_group $forComputeGroupStr properties ( 'max_concurrency'='-1' );"

        exception "The allowed max_concurrency value is an integer greater than or equal to 0"
    }

    test {
        sql "alter workload group test_group $forComputeGroupStr properties ( 'max_queue_size'='-1' );"

        exception "The allowed max_queue_size value is an integer greater than or equal to 0"
    }

    test {
        sql "alter workload group test_group $forComputeGroupStr properties ( 'queue_timeout'='-1' );"

        exception "The allowed queue_timeout value is an integer greater than or equal to 0"
    }

    test {
        sql "alter workload group test_group $forComputeGroupStr properties('read_bytes_per_second'='0')"
        exception "The allowed read_bytes_per_second value should be -1 or an positive integer"
    }

    test {
        sql "alter workload group test_group $forComputeGroupStr properties('read_bytes_per_second'='-2')"
        exception "The allowed read_bytes_per_second value should be -1 or an positive integer"
    }

    test {
        sql "alter workload group test_group $forComputeGroupStr properties('remote_read_bytes_per_second'='0')"
        exception "The allowed remote_read_bytes_per_second value should be -1 or an positive integer"
    }

    test {
        sql "alter workload group test_group $forComputeGroupStr properties('remote_read_bytes_per_second'='-2')"
        exception "The allowed remote_read_bytes_per_second value should be -1 or an positive integer"
    }

    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_concurrency'='100' );"
    sql "alter workload group test_group $forComputeGroupStr properties('remote_read_bytes_per_second'='104857600')"
    sql "alter workload group test_group $forComputeGroupStr properties('read_bytes_per_second'='104857600')"
    qt_queue_1 """ select count(1) from ${table_name} """
    qt_show_queue "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,read_bytes_per_second,remote_read_bytes_per_second from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test create group failed
    // failed for min_cpu_percent
    test {
        sql "create workload group if not exists test_group2 $forComputeGroupStr " +
                "properties ( " +
                "    'min_cpu_percent'='-2', " +
                "    'max_memory_percent'='1%' " +
                ");"

        exception "The allowed min_cpu_percent value has to be in [0,100]"
    }

    // test show workload groups
    qt_select_tvf_1 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num from information_schema.workload_groups where name in ('normal','test_group') order by name;"

    // test auth
    sql """drop user if exists test_wlg_user"""
    sql "CREATE USER 'test_wlg_user'@'%' IDENTIFIED BY '12345';"
    sql """grant SELECT_PRIV on *.*.* to test_wlg_user;"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO test_wlg_user""";
    }

    connect('test_wlg_user', '12345', context.config.jdbcUrl) {
            sql """ select count(1) from information_schema.backend_active_tasks; """
    }

    connect('test_wlg_user', '12345', context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        test {
            sql """ select count(1) from information_schema.backend_active_tasks; """
            exception "Access denied"
        }
    }


    sql "drop workload group if exists grant_test_wg $forComputeGroupStr;"
    test {
        sql " GRANT USAGE_PRIV ON WORKLOAD GROUP grant_test_wg TO 'test_wlg_user'@'%';"
        exception "Can not find workload group"
    }

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_group' TO 'test_wlg_user'@'%';"

    connect('test_wlg_user', '12345', context.config.jdbcUrl) {
        sql """ set workload_group = test_group; """
        sql """ select count(1) from information_schema.backend_active_tasks; """
    }

    // test query queue limit
    sql "set workload_group=test_group;"
    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_concurrency'='0' );"
    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_queue_size'='0' );"
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

    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_queue_size'='1' );"
    sql "alter workload group test_group $forComputeGroupStr properties ( 'queue_timeout'='500' );"
    Thread.sleep(10000)
    test {
        sql "select /*+SET_VAR(parallel_pipeline_task_num=1)*/ * from ${table_name};"

        exception "query queue timeout"
    }

    // test query queue running query/waiting num
    sql "drop workload group if exists test_query_num_wg $forComputeGroupStr;"
    sql "create workload group if not exists test_query_num_wg $forComputeGroupStr properties ('max_concurrency'='1');"
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

    sql "drop workload group test_query_num_wg $forComputeGroupStr;"

    sql "set workload_group=normal;"
    sql "alter workload group test_group $forComputeGroupStr properties ( 'max_concurrency'='10' );"
    Thread.sleep(10000)
    sql "select /*+SET_VAR(parallel_pipeline_task_num=1)*/ * from ${table_name};"

    // test workload spill property
    // 1 create group
    test {
        sql "create workload group if not exists spill_group_test_failed $forComputeGroupStr properties (  'memory_low_watermark'='96%');"
        exception "should bigger than memory_low_watermark"
    }
    sql "create workload group if not exists spill_group_test $forComputeGroupStr properties (  'memory_low_watermark'='10%','memory_high_watermark'='10%');"
    qt_show_spill_1 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,memory_low_watermark,memory_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"

    test {
        sql "create workload group if not exists spill_group_test $forComputeGroupStr properties (  'memory_low_watermark'='20%','memory_high_watermark'='10%');"
        exception "should bigger than memory_low_watermark"
    }

    // 2 alter low
    sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_low_watermark'='5%' );"
    qt_show_spill_2 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,memory_low_watermark,memory_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"

    test {
        sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_low_watermark'='20%' );"
        exception "should bigger than memory_low_watermark"
    }

    test {
        sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_low_watermark'='0%' );"
        exception "value is an integer value between 1 and 100"
    }

    test {
        sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_low_watermark'='101%' );"
        exception "value is an integer value between 1 and 100"
    }

    test {
        sql "create workload group if not exists spill_group_test2 $forComputeGroupStr properties (  'memory_low_watermark'='0%')"
        exception "value is an integer value between 1 and 100"
    }

    test {
        sql "create workload group if not exists spill_group_test2 $forComputeGroupStr properties (  'memory_low_watermark'='101%')"
        exception "value is an integer value between 1 and 100"
    }

    // 3 alter high
    sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_high_watermark'='40%' );"
    qt_show_spill_3 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,memory_low_watermark,memory_high_watermark from information_schema.workload_groups where name in ('spill_group_test');"
    test {
        sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_high_watermark'='1%' );"
        exception "should bigger than memory_low_watermark"
    }

    test {
        sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_high_watermark'='0%' );"
        exception "value is an integer value between 1 and 100"
    }

    test {
        sql "alter workload group spill_group_test $forComputeGroupStr properties ( 'memory_high_watermark'='101%' );"
        exception "value is an integer value between 1 and 100"
    }

    test {
        sql "create workload group if not exists spill_group_test2 $forComputeGroupStr properties (  'memory_high_watermark'='0%')"
        exception "value is an integer value between 1 and 100"
    }

    test {
        sql "create workload group if not exists spill_group_test2 $forComputeGroupStr properties (  'memory_high_watermark'='101%')"
        exception "value is an integer value between 1 and 100"
    }

    sql "drop workload group test_group $forComputeGroupStr;"
    sql "drop workload group spill_group_test $forComputeGroupStr;"


    // test bypass
    sql "create workload group if not exists bypass_group $forComputeGroupStr properties (  'max_concurrency'='0','max_queue_size'='0','queue_timeout'='0');"
    sql "set workload_group=bypass_group;"
    test {
        sql "select count(1) from ${table_name};"
        exception "query waiting queue is full"
    }

    sql "set bypass_workload_group = true;"
    sql "select count(1) from information_schema.active_queries;"

    // test set remote scan pool
    sql "drop workload group if exists test_remote_scan_wg $forComputeGroupStr;"
    test {
        sql "create workload group test_remote_scan_wg $forComputeGroupStr properties('min_remote_scan_thread_num'='123');"
        exception "must be specified simultaneously"
    }

    test {
        sql "create workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='123');"
        exception "must be specified simultaneously"
    }

    test {
        sql "create workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='10', 'min_remote_scan_thread_num'='123');"
        exception "must bigger or equal "
    }

    sql "create workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='20', 'min_remote_scan_thread_num'='10');"
    qt_select_remote_scan_num "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='21')"
    qt_select_remote_scan_num_2 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    test {
        sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='5')"
        exception "must bigger or equal"
    }

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('min_remote_scan_thread_num'='2')"
    qt_select_remote_scan_num_3 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    test {
        sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('min_remote_scan_thread_num'='30')"
        exception "must bigger or equal"
    }

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='40', 'min_remote_scan_thread_num'='20')"
    qt_select_remote_scan_num_4 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='10', 'min_remote_scan_thread_num'='5')"
    qt_select_remote_scan_num_5 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='3', 'min_remote_scan_thread_num'='3')"
    qt_select_remote_scan_num_6 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    sql "drop workload group test_remote_scan_wg $forComputeGroupStr;"
    sql "create workload group test_remote_scan_wg $forComputeGroupStr properties('min_cpu_percent'='10');"
    test {
        sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('min_remote_scan_thread_num'='30')"
        exception "must be specified simultaneously"
    }

    test {
        sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='30')"
        exception "must be specified simultaneously"
    }

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='10', 'min_remote_scan_thread_num'='5')"
    qt_select_remote_scan_num_7 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"

    sql "alter workload group test_remote_scan_wg $forComputeGroupStr properties('max_remote_scan_thread_num'='-1', 'min_remote_scan_thread_num'='-1')"
    qt_select_remote_scan_num_8 "select MAX_REMOTE_SCAN_THREAD_NUM,MIN_REMOTE_SCAN_THREAD_NUM from information_schema.workload_groups where name='test_remote_scan_wg';"
    sql "drop workload group test_remote_scan_wg $forComputeGroupStr"

    sql "drop workload group bypass_group $forComputeGroupStr;"

    // test workload group privilege table
    sql "set workload_group=normal;"
    sql "drop user if exists test_wg_priv_user1"
    sql "drop user if exists test_wg_priv_user2"
    sql "drop role if exists test_wg_priv_role1"
    sql "drop workload group if exists test_wg_priv_g1 $forComputeGroupStr;"
    // 1 test grant user
    sql "create workload group test_wg_priv_g1 $forComputeGroupStr properties('min_cpu_percent'='10')"

    sql "create user test_wg_priv_user1"
    qt_select_wgp_1 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_wg_priv_g1' TO test_wg_priv_user1;"
    qt_select_wgp_2 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    sql "revoke USAGE_PRIV ON WORKLOAD GROUP 'test_wg_priv_g1' from test_wg_priv_user1;"
    qt_select_wgp_3 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_wg_priv_g1' TO test_wg_priv_user1;"
    qt_select_wgp_4 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "



    // 2 test grant role
    sql "create role test_wg_priv_role1;"
    qt_select_wgp_5 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_wg_priv_g1' TO role 'test_wg_priv_role1';"
    qt_select_wgp_6 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    sql "revoke USAGE_PRIV ON WORKLOAD GROUP 'test_wg_priv_g1' from role 'test_wg_priv_role1';"
    qt_select_wgp_7 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'test_wg_priv_g1' TO role 'test_wg_priv_role1';"
    qt_select_wgp_8 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    // 3 test grant %
    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP '%' TO test_wg_priv_user1; "
    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP '%' TO role 'test_wg_priv_role1'; "
    qt_select_wgp_9 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "
    sql "revoke USAGE_PRIV ON WORKLOAD GROUP '%' from test_wg_priv_user1; "
    sql "revoke USAGE_PRIV ON WORKLOAD GROUP '%' from role 'test_wg_priv_role1'; "
    qt_select_wgp_10 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "

    //4 test row filter
    sql "create user test_wg_priv_user2"
    sql "grant SELECT_PRIV on *.*.* to test_wg_priv_user2"
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO test_wg_priv_user2""";
    }
    connect('test_wg_priv_user2', '', context.config.jdbcUrl) {
        qt_select_wgp_11 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "
    }

    sql "drop user test_wg_priv_user1"
    sql "drop user test_wg_priv_user2"
    sql "drop role test_wg_priv_role1"
    qt_select_wgp_12 "select GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from information_schema.workload_group_privileges where grantee like '%test_wg_priv%' order by GRANTEE,WORKLOAD_GROUP_NAME,PRIVILEGE_TYPE,IS_GRANTABLE; "
    sql "drop workload group test_wg_priv_g1 $forComputeGroupStr"

    // test default value
    sql "drop workload group if exists default_val_wg $forComputeGroupStr"
    sql "create workload group default_val_wg $forComputeGroupStr properties('max_concurrency'='10');"
    qt_select_default_val_wg_1 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,max_remote_scan_thread_num,min_remote_scan_thread_num,memory_low_watermark,memory_high_watermark,read_bytes_per_second,remote_read_bytes_per_second from information_schema.workload_groups where name = 'default_val_wg'"

    sql """
            alter workload group default_val_wg $forComputeGroupStr properties(
                'min_cpu_percent'='1%',
                'max_memory_percent'='100%',
                'max_concurrency'='100',
                'max_queue_size'='1',
                'queue_timeout'='123',
                'max_cpu_percent'='10%',
                'scan_thread_num'='1',
                'max_remote_scan_thread_num'='12',
                'min_remote_scan_thread_num'='10',
                'read_bytes_per_second'='123',
                'remote_read_bytes_per_second'='10');
    """

    qt_select_default_val_wg_2 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,max_remote_scan_thread_num,min_remote_scan_thread_num,memory_low_watermark,memory_high_watermark,read_bytes_per_second,remote_read_bytes_per_second from information_schema.workload_groups where name = 'default_val_wg'"

    sql """
       alter workload group default_val_wg $forComputeGroupStr properties(
        'min_cpu_percent'='0',
        'max_memory_percent'='100%',
        'max_concurrency'='2147483647',
        'max_queue_size'='0',
        'queue_timeout'='0',
        'max_cpu_percent'='100%',
        'scan_thread_num'='-1',
        'max_remote_scan_thread_num'='-1',
        'min_remote_scan_thread_num'='-1',
        'read_bytes_per_second'='-1',
        'remote_read_bytes_per_second'='-1'
        );
    """

    qt_select_default_val_wg_3 "select name,min_cpu_percent,max_memory_percent,max_concurrency,max_queue_size,queue_timeout,max_cpu_percent,scan_thread_num,max_remote_scan_thread_num,min_remote_scan_thread_num,memory_low_watermark,memory_high_watermark,read_bytes_per_second,remote_read_bytes_per_second from information_schema.workload_groups where name = 'default_val_wg'"

    sql "drop workload group if exists default_val_wg $forComputeGroupStr"
}
