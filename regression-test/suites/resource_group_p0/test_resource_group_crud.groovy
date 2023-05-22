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

// note(wb) all mem_limit test should be in one suite
suite("test_resource_group_crud") {
    // alter normal group for later test
    sql "ALTER RESOURCE GROUP normal PROPERTIES ('memory_limit'='10%');"
    
    rg_name = "test_resource_group0"
    sql """
        drop resource group if exists ${rg_name};
    """

    sql "create resource group ${rg_name} properties('cpu_share'='10', 'memory_limit'='20%');"

    qt_select "select name, item, value from resource_groups() where name='${rg_name}' order by 1,2,3;"
    // test show statement
    String[][] resource_groups = sql """ show resource groups """
    boolean cpu_ret = false
    boolean mem_ret = false
    for (String[] rg in resource_groups) {
        String rgName = rg[1];
        String item = rg[2];
        String val = rg[3]
        if (rgName.equals("test_resource_group0") && item.equals("cpu_share")
            && val.equals("10")) {
            cpu_ret = true;
        }
        if (rgName.equals("test_resource_group0") && item.equals("memory_limit")
                && val.equals("20%")) {
            mem_ret = true;
        }
    }
    assertTrue(cpu_ret);
    assertTrue(mem_ret);

    rg_tab_name = "test_rg_tab1"

    sql """
        drop table if exists ${rg_tab_name};
    """

    sql """
        CREATE TABLE `${rg_tab_name}` (
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

    sql """insert into ${rg_tab_name} values
        (9,10,11,12),
        (10,11,12,13),
        (1,2,3,4)
    """

    sql """set experimental_enable_pipeline_engine = true;"""
    sql """set resource_group='${rg_name}';"""

    qt_select """
            select count(1) from ${rg_tab_name};
    """
    
    sql """
       ALTER RESOURCE GROUP `${rg_name}`  PROPERTIES ("cpu_share"="1", 'memory_limit'='25%'); 
    """

    qt_select "select name, item, value from resource_groups() where name='${rg_name}' order by 1,2,3;"

    qt_select """
            select count(1) from ${rg_tab_name};
    """

    sql """
        drop resource group `${rg_name}`;
    """

    sql """set resource_group='normal';"""
    qt_select "select name, item, value from resource_groups() where name='${rg_name}' order by 1,2,3;"

    sql """
        drop table ${rg_tab_name};
    """
}
