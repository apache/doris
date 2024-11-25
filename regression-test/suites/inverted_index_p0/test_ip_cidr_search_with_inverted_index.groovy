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

suite("test_ip_cidr_search_with_inverted_index", "nonConcurrent"){
    // prepare test table
    sql "DROP TABLE IF EXISTS tc_ip_cidr_search_with_inverted_index"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS `tc_ip_cidr_search_with_inverted_index` (
        `id` INT NULL,
          `ipv4` IPV4 NULL,
          `ipv6` IPV6 NULL,
          `cidr` TEXT NULL,
          INDEX v4_index (`ipv4`) USING INVERTED,
          INDEX v6_index (`ipv6`) USING INVERTED
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """
    sql """ set enable_profile = true;"""

    sql """ insert into tc_ip_cidr_search_with_inverted_index values(1, '59.50.185.152', '2a02:e980:83:5b09:ecb8:c669:b336:650e', '127.0.0.0/8'),(3, '119.36.22.147', '2001:4888:1f:e891:161:26::', '127.0.0.0/8'),(2, '42.117.228.166', '2001:16a0:2:200a::2', null); """
    sql """ insert into tc_ip_cidr_search_with_inverted_index values(4, '.', '2001:1b70:a1:610::b102:2', null); """
    sql """ insert into tc_ip_cidr_search_with_inverted_index values(5, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffffg', null) """

    qt_sql """ select count() from tc_ip_cidr_search_with_inverted_index"""
    // without inverted index query
    sql """ set enable_common_expr_pushdown = false; """
    sql """ set enable_inverted_index_query=false; """
    // select ipv6 in ipv4 cidr
    qt_sql_without_ii_0 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '255.255.255.255/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '255.255.255.255/12') order by id; """
    // select ipv6 in ipv6 cidr
    qt_sql_without_ii_1 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') order by id; """
    qt_sql_without_ii_2 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '::ffff:192.168.0.4/128') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '::ffff:192.168.0.4/128') order by id; """
    qt_sql_without_ii_3 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '2001:16a0:2:200a::2/64') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '2001:16a0:2:200a::2/64') order by id; """

    // select ipv4 in ipv6 cidr
    qt_sql_without_ii_4 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') order by id; """
    // select ipv4 in ipv4 cidr
    qt_sql_without_ii_5 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '255.255.255.255/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '255.255.255.255/12') order by id; """
    qt_sql_without_ii_6 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, '127.0.0.0/8') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '127.0.0.0/8') order by id; """
    qt_sql_without_ii_7 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, '192.168.100.0/24') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '192.168.100.0/24') order by id; """

    // select in null cidr
    qt_sql_without_ii_8 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, null) from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, null) order by id; """
    qt_sql_without_ii_9 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, null) from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, null) order by id; """

    // with inverted index query
    // If we use common expr pass to inverted index , we should set enable_common_expr_pushdown = true
    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_inverted_index_query=true; """
    sql """ set inverted_index_skip_threshold = 0; """ // set skip threshold to 0

    // select ipv6 in ipv4 cidr
    qt_sql_with_ii_0 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '255.255.255.255/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '255.255.255.255/12') order by id; """
    // select ipv6 in ipv6 cidr
    qt_sql_with_ii_1 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') order by id; """
    qt_sql_with_ii_2 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '::ffff:192.168.0.4/128') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '::ffff:192.168.0.4/128') order by id; """
    qt_sql_with_ii_3 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '2001:16a0:2:200a::2/64') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '2001:16a0:2:200a::2/64') order by id; """

    // select ipv4 in ipv6 cidr
    qt_sql_with_ii_4 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') order by id; """
    // select ipv4 in ipv4 cidr
    qt_sql_with_ii_5 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, '255.255.255.255/12') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '255.255.255.255/12') order by id; """
    qt_sql_with_ii_6 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, '127.0.0.0/8') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '127.0.0.0/8') order by id; """
    qt_sql_with_ii_7 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, '192.168.100.0/24') from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '192.168.100.0/24') order by id; """

    // select in null cidr
    qt_sql_with_ii_8 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv4, null) from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, null) order by id; """
    qt_sql_with_ii_9 """  select id, ipv4, ipv6, is_ip_address_in_range(ipv6, null) from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, null) order by id; """



    def create_sql = {
        List<String> list = new ArrayList<>()
        // select ipv6 in ipv4 cidr
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '255.255.255.255/12') order by id;")
        // select ipv6 in ipv6 cidr
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') order by id;")
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '::ffff:192.168.0.4/128') order by id;")
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, '2001:16a0:2:200a::2/64') order by id;")
        // select ipv4 in ipv6 cidr
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/12') order by id;")
        // select ipv4 in ipv4 cidr
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '255.255.255.255/12') order by id;")
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '127.0.0.0/8') order by id;")
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, '192.168.100.0/24') order by id;")
        // select in null cidr
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv4, null) order by id;")
        list.add("select id, ipv4, ipv6 from tc_ip_cidr_search_with_inverted_index where is_ip_address_in_range(ipv6, null) order by id;")
        return list;
    }

    def checkpoints_name = "ip.inverted_index_filtered"
    def execute_sql = { sqlList ->
        def i = 0
        for (sqlStr in sqlList) {
            try {
                log.info("execute sql: i")
                GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [req_id: i])
                order_qt_sql """ ${sqlStr} """
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
            }
            ++i
        }
    }

    execute_sql.call(create_sql.call())

}