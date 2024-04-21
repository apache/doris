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
import groovy.json.JsonSlurper
import org.codehaus.groovy.runtime.IOGroovyMethods
import java.time.LocalDate;

suite("cold_heat_dynamic_partition_by_hdfs") {
    def fetchBeHttp = { check_func, meta_url ->
        def i = meta_url.indexOf("/api")
        String endPoint = meta_url.substring(0, i)
        String metaUri = meta_url.substring(i)
        i = endPoint.lastIndexOf('/')
        endPoint = endPoint.substring(i + 1)
        httpTest {
            endpoint endPoint
            uri metaUri
            op "get"
            check check_func
        }
    }
    // data_sizes is one arrayList<Long>, t is tablet
    def fetchDataSize = { data_sizes, t ->
        def tabletId = t[0]
        String meta_url = t[17]
        def clos = {  respCode, body ->
            logger.info("test ttl expired resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def obj = new JsonSlurper().parseText(out)
            data_sizes[0] = obj.local_data_size
            data_sizes[1] = obj.remote_data_size
        }
        fetchBeHttp(clos, meta_url.replace("header", "data_size"))
    }
    // used as passing out parameter to fetchDataSize
    List<Long> sizes = [-1, -1]
    def tableName = "tbl2"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    def check_storage_policy_exist = { name->
        def polices = sql"""
        show storage policy;
        """
        for (p in polices) {
            if (name == p[0]) {
                return true;
            }
        }
        return false;
    }

    def resource_name = "test_dynamic_partition_resource"
    def policy_name= "test_dynamic_partition_policy"

    if (check_storage_policy_exist(policy_name)) {
        sql """
            DROP STORAGE POLICY ${policy_name}
        """
    }

    def has_resouce = sql """
        SHOW RESOURCES WHERE NAME = "${resource_name}";
    """
    if (has_resouce.size() > 0) {
        sql """
            DROP RESOURCE IF EXISTS ${resource_name}
        """
    }

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource_name}"
        PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="${getHdfsFs()}",
            "hadoop.username"="${getHdfsUser()}",
            "hadoop.password"="${getHdfsPasswd()}",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
    """

    sql """
        CREATE STORAGE POLICY IF NOT EXISTS ${policy_name}
        PROPERTIES(
            "storage_resource" = "${resource_name}",
            "cooldown_ttl" = "300"
        )
    """

    // test one replica
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
            `k1` int,
            `k2` date
            )
            PARTITION BY RANGE(k2)()
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES
            (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.replication_num" = "1",
            "dynamic_partition.start" = "-3",
            "storage_policy" = "${policy_name}",
            "replication_num" = "1"
            );
        """
    LocalDate currentDate = LocalDate.now();
    LocalDate currentDatePlusOne = currentDate.plusDays(1);
    LocalDate currentDatePlusTwo = currentDate.plusDays(2);
    LocalDate currentDatePlusThree = currentDate.plusDays(3);
    sql """
        insert into ${tableName} values(1, "${currentDate.toString()}");
    """
    sql """
        insert into ${tableName} values(1, "${currentDatePlusOne.toString()}");
    """
    sql """
        insert into ${tableName} values(1, "${currentDatePlusTwo.toString()}");
    """
    sql """
        insert into ${tableName} values(1, "${currentDatePlusThree.toString()}");
    """

    // show tablets from table, 获取第一个tablet的 LocalDataSize1
    def tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty")
    assertTrue(tablets.size() > 0)
    fetchDataSize(sizes, tablets[0])
    def LocalDataSize1 = sizes[0]
    def RemoteDataSize1 = sizes[1]
    log.info( "test local size {} not zero, remote size {}", LocalDataSize1, RemoteDataSize1)
    assertTrue(LocalDataSize1 != 0)
    log.info( "test remote size is zero")
    assertEquals(RemoteDataSize1, 0)
    def originLocalDataSize1 = LocalDataSize1;

    // 等待10min，show tablets from table, 预期not_use_storage_policy_tablet_list 的 RemoteDataSize 为0，LocalDataSize不为0
    sleep(600000)


    tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty")
    fetchDataSize(sizes, tablets[0])
    while (sizes[1] == 0) {
        log.info( "test remote size is zero, sleep 10s")
        sleep(10000)
        tablets = sql """
        SHOW TABLETS FROM ${tableName}
        """
        fetchDataSize(sizes, tablets[0])
    }
    assertTrue(tablets.size() > 0)
    LocalDataSize1 = sizes[0]
    RemoteDataSize1 = sizes[1]
    Long sleepTimes = 0;
    while (RemoteDataSize1 != originLocalDataSize1 && sleepTimes < 60) {
        log.info( "test remote size is same with origin size, sleep 10s")
        sleep(10000)
        tablets = sql """
        SHOW TABLETS FROM
        """
        fetchDataSize(sizes, tablets[0])
        LocalDataSize1 = sizes[0]
        RemoteDataSize1 = sizes[1]
        sleepTimes += 1
    }
    log.info( "test local size is  zero")
    assertEquals(LocalDataSize1, 0)
    log.info( "test remote size not zero")
    assertEquals(RemoteDataSize1, originLocalDataSize1)

    // 12列是storage policy
    def partitions = sql "show partitions from ${tableName}"
    for (par in partitions) {
        assertTrue(par[12] == "${policy_name}")
    }

    try_sql """
    drop storage policy tmp_policy;
    """

    try_sql """
    drop resource tmp_resource;
    """

    sql """
        CREATE RESOURCE IF NOT EXISTS "tmp_resource"
        PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="${getHdfsFs()}",
            "hadoop.username"="${getHdfsUser()}",
            "hadoop.password"="${getHdfsPasswd()}",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
    """

    try_sql """
    create storage policy tmp_policy
    PROPERTIES( "storage_resource" = "tmp_resource", "cooldown_ttl" = "300");
    """

    // can not set to one policy with different resource
    try {
        sql """alter table ${tableName} set ("storage_policy" = "tmp_policy");"""
    } catch (java.sql.SQLException t) {
        assertTrue(true)
    }

    sql """
        CREATE STORAGE POLICY IF NOT EXISTS tmp_policy1
        PROPERTIES(
            "storage_resource" = "${resource_name}",
            "cooldown_ttl" = "60"
        )
    """

    sql """alter table ${tableName} set ("storage_policy" = "tmp_policy1");"""

    // wait for report
    sleep(300000)

    partitions = sql "show partitions from ${tableName}"
    for (par in partitions) {
        assertTrue(par[12] == "tmp_policy1")
    }

    sql """
    select * from ${tableName}
    """

    sql """
    DROP TABLE ${tableName}
    """

    sql """
    drop storage policy tmp_policy;
    """

    sql """
    drop storage policy tmp_policy1;
    """

    sql """
    drop resource tmp_resource;
    """



}
