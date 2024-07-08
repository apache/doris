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
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.suite.SuiteCluster

suite("test_create_table_exception") {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(3)
    options.feConfigs.add('max_dynamic_partition_num=2000')

    docker(options) {
        sleep 2000
        def table1 = "normal_table"
        def table2 = "range_table"
        def table3 = "dynamic_partition_table"
        try {
            GetDebugPoint().enableDebugPointForAllFEs('FE.createOlapTable.exception', null)
            def createTable = { tableIdx ->
                try_sql """
                    CREATE TABLE ${table1}_${tableIdx} (
                        `k1` int(11) NULL,
                        `k2` int(11) NULL
                    )
                    DUPLICATE KEY(`k1`, `k2`)
                    COMMENT 'OLAP'
                    DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                    PROPERTIES (
                    "colocate_with" = "col_grp_${tableIdx}",
                    "replication_num"="3"
                    );
                """

                try_sql """
                    CREATE TABLE IF NOT EXISTS ${table2}_${tableIdx} (
                        lo_orderdate int(11) NOT NULL COMMENT "",
                        lo_orderkey bigint(20) NOT NULL COMMENT "",
                        lo_linenumber bigint(20) NOT NULL COMMENT "",
                        lo_custkey int(11) NOT NULL COMMENT "",
                        lo_partkey int(11) NOT NULL COMMENT "",
                        lo_suppkey int(11) NOT NULL COMMENT "",
                        lo_orderpriority varchar(64) NOT NULL COMMENT "",
                        lo_shippriority int(11) NOT NULL COMMENT "",
                        lo_quantity bigint(20) NOT NULL COMMENT "",
                        lo_extendedprice bigint(20) NOT NULL COMMENT "",
                        lo_ordtotalprice bigint(20) NOT NULL COMMENT "",
                        lo_discount bigint(20) NOT NULL COMMENT "",
                        lo_revenue bigint(20) NOT NULL COMMENT "",
                        lo_supplycost bigint(20) NOT NULL COMMENT "",
                        lo_tax bigint(20) NOT NULL COMMENT "",
                        lo_commitdate bigint(20) NOT NULL COMMENT "",
                        lo_shipmode varchar(64) NOT NULL COMMENT "" )
                    ENGINE=OLAP
                    UNIQUE KEY(lo_orderdate, lo_orderkey, lo_linenumber)
                    COMMENT "OLAP"
                    PARTITION BY RANGE(lo_orderdate) (
                    PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
                    PARTITION p1993 VALUES [("19930101"), ("19940101")),
                    PARTITION p1994 VALUES [("19940101"), ("19950101")),
                    PARTITION p1995 VALUES [("19950101"), ("19960101")),
                    PARTITION p1996 VALUES [("19960101"), ("19970101")),
                    PARTITION p1997 VALUES [("19970101"), ("19980101")),
                    PARTITION p1998 VALUES [("19980101"), ("19990101")))
                    DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 48;
                """

                try_sql """
                    CREATE TABLE ${table3}_${tableIdx} (
                        time date,
                        key1 int,
                        key2 int,
                        value1 int,
                        value2 int
                    ) ENGINE = OLAP UNIQUE KEY(
                        `time`,
                        `key1`,
                        `key2`
                    ) COMMENT 'OLAP' PARTITION BY RANGE(`time`)()
                    DISTRIBUTED BY HASH(`key1`) BUCKETS 6 PROPERTIES (
                    "file_cache_ttl_seconds" = "0",
                    "bloom_filter_columns" = "time",
                    "dynamic_partition.enable" = "true",
                    "dynamic_partition.time_unit" = "DAY",
                    "dynamic_partition.time_zone" = "Asia/Shanghai",
                    "dynamic_partition.start" = "-730",
                    "dynamic_partition.end" = "3",
                    "dynamic_partition.prefix" = "p",
                    "dynamic_partition.buckets" = "2",
                    "dynamic_partition.create_history_partition" = "true",
                    "dynamic_partition.history_partition_num" = "-1",
                    "dynamic_partition.hot_partition_num" = "0",
                    "dynamic_partition.reserved_history_periods" = "NULL",
                    "enable_unique_key_merge_on_write" = "true",
                    "light_schema_change" = "true"
                    );
                """
            }
            createTable(1)
            def result = sql """show tables;"""
            assertEquals(result.size(), 0)

            def checkResult = { ->
                def tables = sql """show tables;"""
                log.info("tables=" + tables)
                assertEquals(3, tables.size())

                def groups = sql """ show proc "/colocation_group" """
                log.info("groups=" + groups)
                assertEquals(1, groups.size())
            }

            GetDebugPoint().disableDebugPointForAllFEs('FE.createOlapTable.exception')
            createTable(2)
            checkResult()

            sleep 1000
            cluster.restartFrontends(cluster.getMasterFe().index)
            sleep 32_000
            def newMasterFe = cluster.getMasterFe()
            def newMasterFeUrl =  "jdbc:mysql://${newMasterFe.host}:${newMasterFe.queryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
            newMasterFeUrl = context.config.buildUrlWithDb(newMasterFeUrl, context.dbName)
            connect('root', '', newMasterFeUrl) {
                checkResult()
            }

        } finally {
        }
    }
}
