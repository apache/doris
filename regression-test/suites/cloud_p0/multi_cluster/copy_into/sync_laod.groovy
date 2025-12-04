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

suite("sync_load") {
    def externalStageName = "regression_test_tpch"
    def prefix = "tpch/sf1"

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()
    List<String> bePortList = new ArrayList<>()

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
    sql """ set enable_profile = true; """

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
        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/${tableName}.csv.split00.gz') properties ('file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"))

        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/${tableName}.csv.split01.gz') properties ('file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"))
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

    assertTrue(before_cluster1_load_rows != after_cluster1_load_rows)
    assertTrue(before_cluster1_flush != after_cluster1_flush)

    sql "use @regression_cluster_name0"
    result  = sql "show clusters"

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

    assertTrue(before_cluster0_query_scan_rows != after_cluster0_query_scan_rows)
    assertTrue(before_cluster1_query_scan_rows == after_cluster1_query_scan_rows)

    //def sqls = ["q01", "q02"]

    //for (String sql in sqls) {
    //    //sql """ DROP TABLE IF EXISTS ${table}; """
    //    sql new File("""${context.file.parent}/ddl/${sql}.sql""").text
    //}

    // fill bePortList
    for (int i = 0; i < ipList.size(); ++i) {
        result = sql """show backends"""
        for (row : result) {
            println row
            println row[2]
            if (ipList[i] == row[1] && hbPortList[i] == row[2]) {
                bePortList.add(row[3]);
            }
        }
    }
    assertEquals(bePortList.size(), 2);

    // q01
    sql """
        SELECT
          l_returnflag,
          l_linestatus,
          sum(l_quantity)                                       AS sum_qty,
          sum(l_extendedprice)                                  AS sum_base_price,
          sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
          sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
          avg(l_quantity)                                       AS avg_qty,
          avg(l_extendedprice)                                  AS avg_price,
          avg(l_discount)                                       AS avg_disc,
          count(*)                                              AS count_order
        FROM
          lineitem
        WHERE
          l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY
        l_returnflag,
        l_linestatus
        ORDER BY
        l_returnflag,
        l_linestatus
    """

    def set = [ipList[0] + ":" +bePortList[0]] as Set
    checkProfile.call(set)

    // q02
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
    checkProfile.call(set)

    // q03
    sql """
        SELECT
          l_orderkey,
          sum(l_extendedprice * (1 - l_discount)) AS revenue,
          o_orderdate,
          o_shippriority
        FROM
          customer,
          orders,
          lineitem
        WHERE
          c_mktsegment = 'BUILDING'
          AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate < DATE '1995-03-15'
          AND l_shipdate > DATE '1995-03-15'
        GROUP BY
          l_orderkey,
          o_orderdate,
          o_shippriority
        ORDER BY
          revenue DESC,
          o_orderdate
        LIMIT 10
    """
    checkProfile.call(set)

    // q04
    sql """
        SELECT
          o_orderpriority,
          count(*) AS order_count
        FROM orders
        WHERE
          o_orderdate >= DATE '1993-07-01'
          AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
        AND EXISTS (
        SELECT *
        FROM lineitem
        WHERE
        l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate
        )
        GROUP BY
        o_orderpriority
        ORDER BY
        o_orderpriority
    """
    checkProfile.call(set)

    // q05
    sql """
        SELECT
          n_name,
          sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM
          customer,
          orders,
          lineitem,
          supplier,
          nation,
          region
        WHERE
          c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= DATE '1994-01-01'
          AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        GROUP BY
        n_name
        ORDER BY
        revenue DESC
    """
    checkProfile.call(set)

    // q06
    sql """
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM
          lineitem
        WHERE
          l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        AND l_discount BETWEEN 0.06 - 0.01 AND .06 + 0.01
        AND l_quantity < 24
    """
    checkProfile.call(set)

    // q07
    sql """
        SELECT
          supp_nation,
          cust_nation,
          l_year,
          sum(volume) AS revenue
        FROM (
               SELECT
                 n1.n_name                          AS supp_nation,
                 n2.n_name                          AS cust_nation,
                 extract(YEAR FROM l_shipdate)      AS l_year,
                 l_extendedprice * (1 - l_discount) AS volume
               FROM
                 supplier,
                 lineitem,
                 orders,
                 customer,
                 nation n1,
                 nation n2
               WHERE
                 s_suppkey = l_suppkey
                 AND o_orderkey = l_orderkey
                 AND c_custkey = o_custkey
                 AND s_nationkey = n1.n_nationkey
                 AND c_nationkey = n2.n_nationkey
                 AND (
                   (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                   OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                 )
                 AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
             ) AS shipping
        GROUP BY
          supp_nation,
          cust_nation,
          l_year
        ORDER BY
          supp_nation,
          cust_nation,
          l_year
    """
    checkProfile.call(set)

    sql "use @regression_cluster_name1"
    set = [ipList[1] + ":" +bePortList[1]] as Set

    // q08
    sql """
        SELECT
          o_year,
          sum(CASE
              WHEN nation = 'BRAZIL'
                THEN volume
              ELSE 0
              END) / sum(volume) AS mkt_share
        FROM (
               SELECT
                 extract(YEAR FROM o_orderdate)     AS o_year,
                 l_extendedprice * (1 - l_discount) AS volume,
                 n2.n_name                          AS nation
               FROM
                 part,
                 supplier,
                 lineitem,
                 orders,
                 customer,
                 nation n1,
                 nation n2,
                 region
               WHERE
                 p_partkey = l_partkey
                 AND s_suppkey = l_suppkey
                 AND l_orderkey = o_orderkey
                 AND o_custkey = c_custkey
                 AND c_nationkey = n1.n_nationkey
                 AND n1.n_regionkey = r_regionkey
                 AND r_name = 'AMERICA'
                 AND s_nationkey = n2.n_nationkey
                 AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                 AND p_type = 'ECONOMY ANODIZED STEEL'
             ) AS all_nations
        GROUP BY
          o_year
        ORDER BY
          o_year
    """
    checkProfile.call(set)

    // q09
    sql """
        SELECT
          nation,
          o_year,
          sum(amount) AS sum_profit
        FROM (
               SELECT
                 n_name                                                          AS nation,
                 extract(YEAR FROM o_orderdate)                                  AS o_year,
                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
               FROM
                 part,
                 supplier,
                 lineitem,
                 partsupp,
                 orders,
                 nation
               WHERE
                 s_suppkey = l_suppkey
                 AND ps_suppkey = l_suppkey
                 AND ps_partkey = l_partkey
                 AND p_partkey = l_partkey
                 AND o_orderkey = l_orderkey
                 AND s_nationkey = n_nationkey
                 AND p_name LIKE '%green%'
             ) AS profit
        GROUP BY
          nation,
          o_year
        ORDER BY
          nation,
          o_year DESC
    """
    checkProfile.call(set)

    // q10
    sql """
        SELECT
          c_custkey,
          c_name,
          sum(l_extendedprice * (1 - l_discount)) AS revenue,
          c_acctbal,
          n_name,
          c_address,
          c_phone,
          c_comment
        FROM
          customer,
          orders,
          lineitem,
          nation
        WHERE
          c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate >= DATE '1993-10-01'
          AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
          AND l_returnflag = 'R'
          AND c_nationkey = n_nationkey
        GROUP BY
          c_custkey,
          c_name,
          c_acctbal,
          c_phone,
          n_name,
          c_address,
          c_comment
        ORDER BY
          revenue DESC
        LIMIT 20
    """
    checkProfile.call(set)

    // q11
    sql """
        SELECT
          ps_partkey,
          sum(ps_supplycost * ps_availqty) AS value
        FROM
          partsupp,
          supplier,
          nation
        WHERE
          ps_suppkey = s_suppkey
          AND s_nationkey = n_nationkey
          AND n_name = 'GERMANY'
        GROUP BY
          ps_partkey
        HAVING
          sum(ps_supplycost * ps_availqty) > (
            SELECT sum(ps_supplycost * ps_availqty) * 0.0001
            FROM
              partsupp,
              supplier,
              nation
            WHERE
              ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
          )
        ORDER BY
          value DESC
    """
    checkProfile.call(set)

    // q12
    sql """
        SELECT
          l_shipmode,
          sum(CASE
              WHEN o_orderpriority = '1-URGENT'
                   OR o_orderpriority = '2-HIGH'
                THEN 1
              ELSE 0
              END) AS high_line_count,
          sum(CASE
              WHEN o_orderpriority <> '1-URGENT'
                   AND o_orderpriority <> '2-HIGH'
                THEN 1
              ELSE 0
              END) AS low_line_count
        FROM
          orders,
          lineitem
        WHERE
          o_orderkey = l_orderkey
          AND l_shipmode IN ('MAIL', 'SHIP')
          AND l_commitdate < l_receiptdate
          AND l_shipdate < l_commitdate
          AND l_receiptdate >= DATE '1994-01-01'
          AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        GROUP BY
          l_shipmode
        ORDER BY
          l_shipmode
    """
    checkProfile.call(set)

    // q13
    sql """
        SELECT
          c_count,
          count(*) AS custdist
        FROM (
               SELECT
                 c_custkey,
                 count(o_orderkey) AS c_count
               FROM
                 customer
                 LEFT OUTER JOIN orders ON
                                          c_custkey = o_custkey
                                          AND o_comment NOT LIKE '%special%requests%'
               GROUP BY
                 c_custkey
             ) AS c_orders
        GROUP BY
          c_count
        ORDER BY
          custdist DESC,
          c_count DESC
    """
    checkProfile.call(set)

    // q14
    sql """
        SELECT 100.00 * sum(CASE
                            WHEN p_type LIKE 'PROMO%'
                              THEN l_extendedprice * (1 - l_discount)
                            ELSE 0
                            END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM
          lineitem,
          part
        WHERE
          l_partkey = p_partkey
          AND l_shipdate >= DATE '1995-09-01'
          AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
    """
    checkProfile.call(set)

    // q15
    sql """
        SELECT
          s_suppkey,
          s_name,
          s_address,
          s_phone,
          total_revenue
        FROM
          supplier,
          revenue1
        WHERE
          s_suppkey = supplier_no
          AND total_revenue = (
            SELECT max(total_revenue)
            FROM
              revenue1
          )
        ORDER BY
          s_suppkey;
    """
    checkProfile.call(set)

    // q16
    sql """
        SELECT
          p_brand,
          p_type,
          p_size,
          count(DISTINCT ps_suppkey) AS supplier_cnt
        FROM
          partsupp,
          part
        WHERE
          p_partkey = ps_partkey
          AND p_brand <> 'Brand#45'
          AND p_type NOT LIKE 'MEDIUM POLISHED%'
          AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
          AND ps_suppkey NOT IN (
            SELECT s_suppkey
            FROM
              supplier
            WHERE
              s_comment LIKE '%Customer%Complaints%'
          )
        GROUP BY
          p_brand,
          p_type,
          p_size
        ORDER BY
          supplier_cnt DESC,
          p_brand,
          p_type,
          p_size
    """
    checkProfile.call(set)

    // q17
    sql """
        SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
        FROM
          lineitem,
          part
        WHERE
          p_partkey = l_partkey
          AND p_brand = 'Brand#23'
          AND p_container = 'MED BOX'
          AND l_quantity < (
            SELECT 0.2 * avg(l_quantity)
            FROM
              lineitem
            WHERE
              l_partkey = p_partkey
          )
    """
    checkProfile.call(set)

    // q18
    sql """
        SELECT
          c_name,
          c_custkey,
          o_orderkey,
          o_orderdate,
          o_totalprice,
          sum(l_quantity)
        FROM
          customer,
          orders,
          lineitem
        WHERE
          o_orderkey IN (
            SELECT l_orderkey
            FROM
              lineitem
            GROUP BY
              l_orderkey
            HAVING
              sum(l_quantity) > 300
          )
          AND c_custkey = o_custkey
          AND o_orderkey = l_orderkey
        GROUP BY
          c_name,
          c_custkey,
          o_orderkey,
          o_orderdate,
          o_totalprice
        ORDER BY
          o_totalprice DESC,
          o_orderdate
        LIMIT 100
    """
    checkProfile.call(set)

    // q19
    sql """
        SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM
          lineitem,
          part
        WHERE
          (
            p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= 1 AND l_quantity <= 1 + 10
            AND p_size BETWEEN 1 AND 5
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
          )
          OR
          (
            p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 10 + 10
            AND p_size BETWEEN 1 AND 10
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
          )
          OR
          (
            p_partkey = l_partkey
            AND p_brand = 'Brand#34'
            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 20 + 10
            AND p_size BETWEEN 1 AND 15
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
          )
    """
    checkProfile.call(set)

    // q20
    sql """
        SELECT
          s_name,
          s_address
        FROM
          supplier, nation
        WHERE
          s_suppkey IN (
            SELECT ps_suppkey
            FROM
              partsupp
            WHERE
              ps_partkey IN (
                SELECT p_partkey
                FROM
                  part
                WHERE
                  p_name LIKE 'forest%'
              )
              AND ps_availqty > (
                SELECT 0.5 * sum(l_quantity)
                FROM
                  lineitem
                WHERE
                  l_partkey = ps_partkey
                  AND l_suppkey = ps_suppkey
                  AND l_shipdate >= date('1994-01-01')
                  AND l_shipdate < date('1994-01-01') + interval '1' YEAR
        )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
        ORDER BY s_name
    """
    checkProfile.call(set)
}
