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

import groovy.json.JsonOutput

suite("async_load") {
    def externalStageName = "regression_test_tpch"
    def prefix = "tpch/sf1"

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);

    sleep(1000)
    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    wait_cluster_change()

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    wait_cluster_change()

    result  = sql "show clusters"
    assertEquals(result.size(), 2);

    sql "use @regression_cluster_name1"
    result  = sql "show clusters"

    def before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    def before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    def before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    def before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    // tpch_sf1_p1 is writted to test unique key table merge correctly.
    // It creates unique key table and sets bucket num to 1 in order to make sure that
    // many rowsets will be created during loading and then the merge process will be triggered.

    def tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS ${table}; """
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    sql """
        create stage if not exists ${externalStageName} properties(
        'endpoint' = '${getS3Endpoint()}' ,
        'region' = '${getS3Region()}' ,
        'bucket' = '${getS3BucketName()}' ,
        'prefix' = 'regression' ,
        'ak' = '${getS3AK()}' ,
        'sk' = '${getS3SK()}' ,
        'provider' = '${getProvider()}',
        'access_type' = 'aksk',
        'default.file.column_separator' = "|" 
        );
    """

    for (String tableName in tables) {
        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/${tableName}.csv.split00.gz') properties ('file.compression' = 'gz', 'copy.async' = 'true'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        //assertTrue(result[0][1].equals("FINISHED"))
        id = result[0][0]
        while(true) {
            result = sql "show copy where id = \'${id}\'"
            logger.info("copy result: " + result)
            if (result[0][3] == "FINISHED" || result[0][3] == "CANCELLED") {
                break;
            }
            sleep(3000);
        }

        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/${tableName}.csv.split01.gz') properties ('file.compression' = 'gz', 'copy.async' = 'true'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        //assertTrue(result[0][1].equals("FINISHED"))
        id = result[0][0]
        while(true) {
            result = sql "show copy where id = \'${id}\'"
            logger.info("show copy result: " + result)
            if (result[0][3] == "FINISHED" || result[0][3] == "CANCELLED") {
                break;
            }
            sleep(3000);
        }
    }

    def table = "revenue1"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/ddl/${table}.sql""").text

    def after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    def after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    def after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    def after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows == after_cluster0_load_rows)
    assertTrue(before_cluster0_flush == after_cluster0_flush)

    assertTrue(before_cluster1_load_rows < after_cluster1_load_rows)
    assertTrue(before_cluster1_flush < after_cluster1_flush)

    sql "use @regression_cluster_name0"
    result  = sql "show clusters"
    sql """ set enable_profile = true; """

    def before_cluster0_query_scan_rows = get_be_metric(ipList[0], httpPortList[0], "query_scan_rows");
    log.info("before_cluster0_query_scan_rows : ${before_cluster0_query_scan_rows}".toString())
    def before_cluster1_query_scan_rows = get_be_metric(ipList[1], httpPortList[1], "query_scan_rows");
    log.info("before_cluster1_query_scan_rows : ${before_cluster1_query_scan_rows}".toString())

    sql """
        SELECT
          s_acctbal,
          s_name,
          n_name,
          p_partkey,
          p_mfgr,
          s_address,
          s_phone,
          s_comment
        FROM
          part,
          supplier,
          partsupp,
          nation,
          region
        WHERE
          p_partkey = ps_partkey
          AND s_suppkey = ps_suppkey
          AND p_size = 15
          AND p_type LIKE '%BRASS'
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'EUROPE'
          AND ps_supplycost = (
            SELECT min(ps_supplycost)
            FROM
              partsupp, supplier,
              nation, region
            WHERE
              p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
          )
        ORDER BY
          s_acctbal DESC,
          n_name,
          s_name,
          p_partkey
        LIMIT 100
    """

    def after_cluster0_query_scan_rows = get_be_metric(ipList[0], httpPortList[0], "query_scan_rows");
    log.info("after_cluster0_query_scan_rows : ${after_cluster0_query_scan_rows}".toString())
    def after_cluster1_query_scan_rows = get_be_metric(ipList[1], httpPortList[1], "query_scan_rows");
    log.info("after_cluster1_query_scan_rows : ${after_cluster1_query_scan_rows}".toString())

    assertTrue(before_cluster0_query_scan_rows < after_cluster0_query_scan_rows)
    assertTrue(before_cluster1_query_scan_rows == after_cluster1_query_scan_rows)

    before_cluster0_query_scan_rows = get_be_metric(ipList[0], httpPortList[0], "query_scan_rows");
    log.info("before_cluster0_query_scan_rows : ${before_cluster0_query_scan_rows}".toString())
    before_cluster1_query_scan_rows = get_be_metric(ipList[1], httpPortList[1], "query_scan_rows");
    log.info("before_cluster1_query_scan_rows : ${before_cluster1_query_scan_rows}".toString())

    sql """
        SELECT /*+ SET_VAR(cloud_cluster = 'regression_cluster_name1') */
          s_acctbal,
          s_name,
          n_name,
          p_partkey,
          p_mfgr,
          s_address,
          s_phone,
          s_comment
        FROM
          part,
          supplier,
          partsupp,
          nation,
          region
        WHERE
          p_partkey = ps_partkey
          AND s_suppkey = ps_suppkey
          AND p_size = 15
          AND p_type LIKE '%BRASS'
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'EUROPE'
          AND ps_supplycost = (
            SELECT min(ps_supplycost)
            FROM
              partsupp, supplier,
              nation, region
            WHERE
              p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
          )
        ORDER BY
          s_acctbal DESC,
          n_name,
          s_name,
          p_partkey
        LIMIT 100
    """

    after_cluster0_query_scan_rows = get_be_metric(ipList[0], httpPortList[0], "query_scan_rows");
    log.info("after_cluster0_query_scan_rows : ${after_cluster0_query_scan_rows}".toString())
    after_cluster1_query_scan_rows = get_be_metric(ipList[1], httpPortList[1], "query_scan_rows");
    log.info("after_cluster1_query_scan_rows : ${after_cluster1_query_scan_rows}".toString())

    assertTrue(before_cluster0_query_scan_rows == after_cluster0_query_scan_rows)
    assertTrue(before_cluster1_query_scan_rows < after_cluster1_query_scan_rows)
}
