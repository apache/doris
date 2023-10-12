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

suite("add_drop_partition") {
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
    def tableName = "tbl1"
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

    def resource_name = "test_add_drop_partition_resource"
    def policy_name= "test_add_drop_partition_policy"

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
            DROP RESOURCE ${resource_name}
        """
    }

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource_name}"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "${getS3Endpoint()}",
            "AWS_REGION" = "${getS3Region()}",
            "AWS_ROOT_PATH" = "regression/cooldown",
            "AWS_ACCESS_KEY" = "${getS3AK()}",
            "AWS_SECRET_KEY" = "${getS3SK()}",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "${getS3BucketName()}",
            "s3_validity_check" = "true"
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
            PARTITION BY RANGE(k2)(
                partition p1 VALUES LESS THAN ("2014-01-01"),
                partition p2 VALUES LESS THAN ("2015-01-01"),
                partition p3 VALUES LESS THAN ("2016-01-01")
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES
            (
            "replication_num" = "1",
            "storage_policy" = "${policy_name}"
            );
        """
    sql """
        insert into ${tableName} values(1, "2013-01-01");
    """
    sql """
        insert into ${tableName} values(1, "2014-01-01");
    """
    sql """
        insert into ${tableName} values(1, "2015-01-01");
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
        SHOW TABLETS FROM ${tableName}
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
    drop storage policy add_policy;
    """

    try_sql """
    drop resource add_resource;
    """

    sql """
        CREATE RESOURCE IF NOT EXISTS "add_resource"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "${getS3Endpoint()}",
            "AWS_REGION" = "${getS3Region()}",
            "AWS_ROOT_PATH" = "regression/cooldown",
            "AWS_ACCESS_KEY" = "${getS3AK()}",
            "AWS_SECRET_KEY" = "${getS3SK()}",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "${getS3BucketName()}",
            "s3_validity_check" = "true"
        );
    """

    try_sql """
    create storage policy tmp_policy
    PROPERTIES( "storage_resource" = "add_resource", "cooldown_ttl" = "300");
    """

    // can not set to one policy with different resource
    try {
        sql """alter table ${tableName} set ("storage_policy" = "add_policy");"""
    } catch (java.sql.SQLException t) {
        assertTrue(true)
    }

    sql """
        CREATE STORAGE POLICY IF NOT EXISTS add_policy1
        PROPERTIES(
            "storage_resource" = "${resource_name}",
            "cooldown_ttl" = "60"
        )
    """

    sql """alter table ${tableName} set ("storage_policy" = "add_policy1");"""

    // wait for report
    sleep(300000)
    
    partitions = sql "show partitions from ${tableName}"
    for (par in partitions) {
        assertTrue(par[12] == "add_policy1")
    }

    
    sql """
        alter table ${tableName} ADD PARTITION np
        VALUES LESS THAN ("2016-01-01");
    """

    sql """
        insert into ${tableName} values(1, "2016-01-01");
    """

    partitions = sql "show partitions from ${tableName}"
    for (par in partitions) {
        assertTrue(par[12] == "add_policy1")
    }

    sql """
    sql * from ${tableName}
    """

    sql """
    DROP TABLE ${tableName}
    """

    sql """
    drop storage policy add_policy;
    """

    sql """
    drop storage policy add_policy1;
    """

    sql """
    drop resource add_resource;
    """



}