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

suite ("partition_curd_union_rewrite_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return
    }

    sql """SET materialized_view_rewrite_enable_contain_external_table = true;"""

    def create_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }
    def compare_res = { def stmt ->
        def mark = true
        sql "SET materialized_view_rewrite_enable_contain_external_table=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET materialized_view_rewrite_enable_contain_external_table=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        if (!((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))) {
            mark = false
            return mark
        }
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            if (!(mv_origin_res[row].size() == origin_res[row].size())) {
                mark = false
                return mark
            }
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                if (!(mv_origin_res[row][col] == origin_res[row][col])) {
                    mark = false
                    return mark
                }
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
        return mark
    }


    String db = context.config.getDbNameByFile(context.file)
    String ctl = "partition_curd_union_hive"
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalog_name = ctl + "_" + hivePrefix
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
        sql """switch ${catalog_name}"""
        sql """create database if not exists ${db}"""
        sql """use `${db}`"""
        String orders_tb_name = catalog_name + "_orders"
        String lineitem_tb_name = catalog_name + "_lineitem"
        def mv_name = catalog_name + "_test_mv"

        sql """drop table if exists ${orders_tb_name}"""
        sql """CREATE TABLE IF NOT EXISTS ${orders_tb_name}  (
              o_orderkey       int,
              o_custkey        int,
              o_orderstatus    VARCHAR(1),
              o_totalprice     DECIMAL(15, 2),
              o_orderpriority  VARCHAR(15),  
              o_clerk          VARCHAR(15), 
              o_shippriority   int,
              o_comment        VARCHAR(15),
              o_orderdate      date
            )
            ENGINE=hive
            PARTITION BY LIST (`o_orderdate`) ()
            PROPERTIES ( 
                "replication_num" = "1",
                'file_format'='orc'
            );
            """

        sql """
            insert into ${orders_tb_name} values 
            (1, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
            (2, 2, 'k', 109.2, 'c','d',2, 'mm', '2023-10-18'),
            (3, 3, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
            """

        sql """drop table if exists ${lineitem_tb_name}"""
        sql"""
            CREATE TABLE IF NOT EXISTS ${lineitem_tb_name} (
              l_orderkey    INT,
              l_partkey     INT,
              l_suppkey     INT,
              l_linenumber  INT,
              l_quantity    DECIMAL(15, 2),
              l_extendedprice  DECIMAL(15, 2),
              l_discount    DECIMAL(15, 2),
              l_tax         DECIMAL(15, 2),
              l_returnflag  VARCHAR(1),
              l_linestatus  VARCHAR(1),
              l_commitdate  date,
              l_receiptdate date,
              l_shipinstruct VARCHAR(10),
              l_shipmode     VARCHAR(10),
              l_comment      VARCHAR(44),
              l_shipdate    date
            ) ENGINE=hive
            PARTITION BY LIST (`l_shipdate`) ()
            PROPERTIES ( 
                "replication_num" = "1",
                'file_format'='orc'
            );
            """

        sql """
        insert into ${lineitem_tb_name} values 
        (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
        (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
        (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19');
        """

        sql """switch internal"""
        sql """create database if not exists ${db}"""
        sql """use ${db}"""

        def mv_def_sql = """
            select l_shipdate, o_orderdate, l_partkey,
            l_suppkey, sum(o_totalprice) as sum_total
            from ${catalog_name}.${db}.${lineitem_tb_name}
            left join ${catalog_name}.${db}.${orders_tb_name} on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey
            """

        def all_partition_sql = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
            from ${catalog_name}.${db}.${lineitem_tb_name} as t1 
            left join ${catalog_name}.${db}.${orders_tb_name} as t2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey
           """

        def partition_sql = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
            from ${catalog_name}.${db}.${lineitem_tb_name} as t1
            left join ${catalog_name}.${db}.${orders_tb_name} as t2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where (l_shipdate>= '2023-10-18' and l_shipdate <= '2023-10-19')
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey
            """

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql """
            CREATE MATERIALIZED VIEW ${mv_name} 
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by(l_shipdate)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1') 
            AS 
            ${mv_def_sql}
            """

        def order_by_stmt = " order by 1,2,3,4,5"
        waitingMTMVTaskFinished(getJobName(db, mv_name))

        // All partition is valid, test query and rewrite by materialized view
        mv_rewrite_success(all_partition_sql, mv_name)
        compare_res(all_partition_sql + order_by_stmt)
        mv_rewrite_success(partition_sql, mv_name)
        compare_res(partition_sql + order_by_stmt)

        // Part partition is invalid, test can not use partition 2023-10-17 to rewrite
        sql """
            insert into ${catalog_name}.${db}.${lineitem_tb_name} values 
            (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
            """
        // wait partition is invalid
        sleep(5000)
        mv_rewrite_success(all_partition_sql, mv_name)
        assertTrue(compare_res(all_partition_sql + order_by_stmt) == false)
        mv_rewrite_success(partition_sql, mv_name)
        assertTrue(compare_res(all_partition_sql + order_by_stmt) == false)


        sql "REFRESH MATERIALIZED VIEW ${mv_name} AUTO"
        waitingMTMVTaskFinished(getJobName(db, mv_name))
        mv_rewrite_success(all_partition_sql, mv_name)
        compare_res(all_partition_sql + order_by_stmt)
        mv_rewrite_success(partition_sql, mv_name)
        compare_res(partition_sql + order_by_stmt)


        // Test when base table create partition
        sql """
            insert into ${catalog_name}.${db}.${lineitem_tb_name} values 
            (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-21', '2023-10-21', 'a', 'b', 'yyyyyyyyy', '2023-10-21');
            """
        // Wait partition is invalid
        sleep(5000)
        mv_rewrite_success(all_partition_sql, mv_name)
        assertTrue(compare_res(all_partition_sql + order_by_stmt) == false)
        mv_rewrite_success(partition_sql, mv_name)
        compare_res(partition_sql + order_by_stmt)

        // Test when base table delete partition test
        sql "REFRESH MATERIALIZED VIEW ${mv_name} AUTO"
        waitingMTMVTaskFinished(getJobName(db, mv_name))
        mv_rewrite_success(all_partition_sql, mv_name)
        compare_res(all_partition_sql + order_by_stmt)
        mv_rewrite_success(partition_sql, mv_name)
        compare_res(partition_sql + order_by_stmt)

    }
}
