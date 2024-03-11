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

suite("modify_replica_use_partition_by_hdfs") {
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
    def get_meta = { url ->
        StringBuilder sb = new StringBuilder();
        sb.append("curl ")
        sb.append(url)
        String command = sb.toString()
        log.info(command)
        def process = command.execute()
        int code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Run compaction: code=" + code + ", err=" + err)
        assertEquals(code, 0)
        return out
    }

    def tableName = "lineitem3"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    def stream_load_one_part = { partnum ->
        streamLoad {
            table tableName
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'


            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/tpch/sf1/lineitem.csv.split${partnum}.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def load_lineitem_table = {
        stream_load_one_part("00")
        stream_load_one_part("01")
        def tablets = sql """
        SHOW TABLETS FROM ${tableName}
        """
        while (tablets[0][8] == "0") {
            log.info( "test local size is zero, sleep 10s")
            sleep(10000)
            tablets = sql """
            SHOW TABLETS FROM ${tableName}
            """
        }
    }

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

    def resource_name = "test_table_replica_with_data_resource"
    def policy_name= "test_table_replica_with_data_policy"

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

    // test one replica then we will alter all the replica of all partitions to 3
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            L_ORDERKEY    INTEGER NOT NULL,
            L_PARTKEY     INTEGER NOT NULL,
            L_SUPPKEY     INTEGER NOT NULL,
            L_LINENUMBER  INTEGER NOT NULL,
            L_QUANTITY    DECIMAL(15,2) NOT NULL,
            L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
            L_DISCOUNT    DECIMAL(15,2) NOT NULL,
            L_TAX         DECIMAL(15,2) NOT NULL,
            L_RETURNFLAG  CHAR(1) NOT NULL,
            L_LINESTATUS  CHAR(1) NOT NULL,
            L_SHIPDATE    DATE NOT NULL,
            L_COMMITDATE  DATE NOT NULL,
            L_RECEIPTDATE DATE NOT NULL,
            L_SHIPINSTRUCT CHAR(25) NOT NULL,
            L_SHIPMODE     CHAR(10) NOT NULL,
            L_COMMENT      VARCHAR(44) NOT NULL
            )
            DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            PARTITION BY RANGE(`L_SHIPDATE`)
            (
                PARTITION `p202301` VALUES LESS THAN ("1995-12-01"),
                PARTITION `p202302` VALUES LESS THAN ("2017-03-01")
            )
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
            "storage_policy" = "${policy_name}",
            "replication_num" = "1"
            )
        """

    load_lineitem_table()

    // 等待10min，show tablets from table, 预期not_use_storage_policy_tablet_list 的 RemoteDataSize 为LocalDataSize1，LocalDataSize为0
    sleep(600000)


    def tablets = sql """
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
    def LocalDataSize1 = sizes[0]
    def RemoteDataSize1 = sizes[1]
    log.info( "test local size is zero")
    assertEquals(LocalDataSize1, 0)
    log.info( "test remote size not zero")
    assertTrue(RemoteDataSize1 != 0)
    def originSize = tablets.size()

    // alter change replication num
    if (!isCloudMode()) {
        sql """
        ALTER TABLE ${tableName}
        MODIFY PARTITION (p202301, p202302) SET("replication_num"="3");
        """
    }

    sql """
    select count(*) from ${tableName}
    """

    sql """
    select * from ${tableName} limit 10
    """
    // wait one minute for migration to be completed
    sleep(60000)

    // 对比所有tablets的replicas的rowsets meta是否相同
    tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    while (tablets.size() != 3 * originSize) {
        log.info( "tablets clone not finished, sleep 10s")
        sleep(10000)
        tablets = sql """
        SHOW TABLETS FROM ${tableName}
        """
    }
    def compactionStatusIdx = tablets[0].size() - 1
    // check rowsets inside the 3 replica
    def iterate_num = tablets.size() / 3;
    for (int i = 0; i < iterate_num; i++) {
        int idx = i * 3;
        def dst = tablets[idx][18]
        def text = get_meta(dst)
        def obj = new JsonSlurper().parseText(text)
        def rowsets = obj.rowsets
        for (x in [1,2]) {
            dst = tablets[idx + x][18]
            text = get_meta(dst)
            obj = new JsonSlurper().parseText(text)
            log.info( "test rowset meta is the same")
            assertEquals(rowsets, obj.rowsets)
        }
    }


    sql """
    DROP TABLE ${tableName}
    """

    // test table with 3 replication, then we will alter all the replica of all partitions to 1
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            L_ORDERKEY    INTEGER NOT NULL,
            L_PARTKEY     INTEGER NOT NULL,
            L_SUPPKEY     INTEGER NOT NULL,
            L_LINENUMBER  INTEGER NOT NULL,
            L_QUANTITY    DECIMAL(15,2) NOT NULL,
            L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
            L_DISCOUNT    DECIMAL(15,2) NOT NULL,
            L_TAX         DECIMAL(15,2) NOT NULL,
            L_RETURNFLAG  CHAR(1) NOT NULL,
            L_LINESTATUS  CHAR(1) NOT NULL,
            L_SHIPDATE    DATE NOT NULL,
            L_COMMITDATE  DATE NOT NULL,
            L_RECEIPTDATE DATE NOT NULL,
            L_SHIPINSTRUCT CHAR(25) NOT NULL,
            L_SHIPMODE     CHAR(10) NOT NULL,
            L_COMMENT      VARCHAR(44) NOT NULL
            )
            DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            PARTITION BY RANGE(`L_SHIPDATE`)
            (
                PARTITION `p202301` VALUES LESS THAN ("1995-12-01"),
                PARTITION `p202302` VALUES LESS THAN ("2017-03-01")
            )
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
            "storage_policy" = "${policy_name}",
            "replication_num" = "3"
            )
        """

    load_lineitem_table()

    // show tablets from table, 获取第一个tablet的 LocalDataSize1
    tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    fetchDataSize(sizes, tablets[0])
    log.info( "test tablets not empty")
    assertTrue(tablets.size() > 0)
    LocalDataSize1 = sizes[0]
    RemoteDataSize1 = sizes[1]
    log.info( "test local size not zero")
    assertTrue(LocalDataSize1 != 0)
    log.info( "test remote size is zero")
    assertEquals(RemoteDataSize1, 0)

    // 等待10min，show tablets from table, 预期not_use_storage_policy_tablet_list 的 RemoteDataSize 为LocalDataSize1，LocalDataSize为0
    sleep(600000)


    tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty")
    assertTrue(tablets.size() > 0)
    fetchDataSize(sizes, tablets[0])
    while (sizes[1] == 0) {
        log.info( "test remote size is zero, sleep 10s")
        sleep(10000)
        tablets = sql """
        SHOW TABLETS FROM ${tableName}
        """
        fetchDataSize(sizes, tablets[0])
    }
    LocalDataSize1 = sizes[0]
    RemoteDataSize1 = sizes[1]
    log.info( "test local size is zero")
    assertEquals(LocalDataSize1, 0)
    log.info( "test remote size not zero")
    assertTrue(RemoteDataSize1 != 0)

    // alter change replication num
    if (!isCloudMode()) {
        sql """
        ALTER TABLE ${tableName}
        MODIFY PARTITION (p202301, p202302) SET("replication_num"="1");
        """
    }

    sql """
    select count(*) from ${tableName}
    """

    sql """
    select * from ${tableName} limit 10
    """

    sql """
    DROP TABLE ${tableName}
    """


    // test table with one partition p1 3 replication and another p2 with 1 replication,
    // then we will alter p1 to one replica and p2 to 3
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            L_ORDERKEY    INTEGER NOT NULL,
            L_PARTKEY     INTEGER NOT NULL,
            L_SUPPKEY     INTEGER NOT NULL,
            L_LINENUMBER  INTEGER NOT NULL,
            L_QUANTITY    DECIMAL(15,2) NOT NULL,
            L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
            L_DISCOUNT    DECIMAL(15,2) NOT NULL,
            L_TAX         DECIMAL(15,2) NOT NULL,
            L_RETURNFLAG  CHAR(1) NOT NULL,
            L_LINESTATUS  CHAR(1) NOT NULL,
            L_SHIPDATE    DATE NOT NULL,
            L_COMMITDATE  DATE NOT NULL,
            L_RECEIPTDATE DATE NOT NULL,
            L_SHIPINSTRUCT CHAR(25) NOT NULL,
            L_SHIPMODE     CHAR(10) NOT NULL,
            L_COMMENT      VARCHAR(44) NOT NULL
            )
            DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            PARTITION BY RANGE(`L_SHIPDATE`)
            (
                PARTITION `p202301` VALUES LESS THAN ("1995-12-01") ("replication_num" = "3"),
                PARTITION `p202302` VALUES LESS THAN ("2017-03-01") ("replication_num" = "1")
            )
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
            "storage_policy" = "${policy_name}"
            )
        """

    load_lineitem_table()

    // show tablets from table, 获取第一个tablet的 LocalDataSize1
    tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    fetchDataSize(sizes, tablets[0])
    log.info( "test tablets not empty")
    assertTrue(tablets.size() > 0)
    LocalDataSize1 = sizes[0]
    RemoteDataSize1 = sizes[1]
    log.info( "test local size not zero")
    assertTrue(LocalDataSize1 != 0)
    log.info( "test remote size is zero")
    assertEquals(RemoteDataSize1, 0)

    // 等待10min，show tablets from table, 预期not_use_storage_policy_tablet_list 的 RemoteDataSize 为LocalDataSize1，LocalDataSize为0
    sleep(600000)


    tablets = sql """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty")
    assertTrue(tablets.size() > 0)
    fetchDataSize(sizes, tablets[0])
    while (sizes[1] == 0) {
        log.info( "test remote size is zero, sleep 10s")
        sleep(10000)
        tablets = sql """
        SHOW TABLETS FROM ${tableName}
        """
        fetchDataSize(sizes, tablets[0])
    }
    LocalDataSize1 = sizes[0]
    RemoteDataSize1 = sizes[1]
    log.info( "test local size is zero")
    assertEquals(LocalDataSize1, 0)
    log.info( "test remote size not zero")
    assertTrue(RemoteDataSize1 != 0)

    // alter change replication num
    if (!isCloudMode()) {
        sql """
        ALTER TABLE ${tableName}
        MODIFY PARTITION (p202301) SET("replication_num"="1");
        """

        sql """
        ALTER TABLE ${tableName}
        MODIFY PARTITION (p202302) SET("replication_num"="3");
        """
    }

    sql """
    select count(*) from ${tableName}
    """

    sql """
    select * from ${tableName} limit 10
    """

    // wait one minute for migration to be completed
    sleep(60000)
    // 对比3副本的partition中所有tablets的replicas的rowsets meta是否相同
    tablets = sql """
    SHOW TABLETS FROM ${tableName} PARTITIONS(p202302)
    """
    // sleep to wait for the report
    sleep(15000)
    tablets = sql """
    SHOW TABLETS FROM ${tableName} PARTITIONS(p202302)
    """
    compactionStatusIdx = tablets[0].size() - 1
    // check rowsets inside the 3 replica
    iterate_num = tablets.size() / 3;
    for (int i = 0; i < iterate_num; i++) {
        int idx = i * 3;
        def dst = tablets[idx][18]
        def text = get_meta(dst)
        def obj = new JsonSlurper().parseText(text)
        def rowsets = obj.rowsets
        for (x in [1,2]) {
            dst = tablets[idx + x][18]
            text = get_meta(dst)
            obj = new JsonSlurper().parseText(text)
            log.info( "test rowset meta is the same")
            assertEquals(rowsets, obj.rowsets)
        }
    }


    sql """
    DROP TABLE ${tableName}
    """


}
