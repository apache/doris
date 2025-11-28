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

suite('test_insert_rpc_order_problem', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'enable_debug_points = true',
        'min_bytes_per_broker_scanner = 100',
        'parallel_pipeline_task_num = 2'
    ]
    options.beConfigs += [
        'enable_debug_points = true',
    ]
    options.beNum = 3

    docker(options) {
        def tableName = "test_insert_rpc_order_problem"
        sql """drop table if exists ${tableName}"""
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
            UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            PARTITION BY RANGE(L_ORDERKEY) (
                PARTITION p2023 VALUES LESS THAN ("6000010") 
            )
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
                "replication_num" = "3"
            );
        """

        // Disable LoadStream to use old LoadChannel mechanism
        sql """ set enable_memtable_on_sink_node = false; """
        sql """ set parallel_pipeline_task_num = 2; """

        try {
            GetDebugPoint().enableDebugPointForAllBEs(
                'FragmentMgr::coordinator_callback.report_delay'
            )
            def label = "test_insert_rpc_order_problem"

            sql """
            LOAD LABEL ${label} (
                DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/{lineitem,lineitem2}.csv.split01.gz")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY "|"
                FORMAT AS "CSV"
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "provider" = "${getS3Provider()}"
            ) 
            """

            def max_try_milli_secs = 600000
            while (max_try_milli_secs > 0) {
                String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
                logger.info("Load result: " + result[0])
                if (result[0][2].equals("FINISHED")) {
                    logger.info("SHOW LOAD : $result")
                    assertTrue(1 == 2, "should not finished")
                }
                if (result[0][2].equals("CANCELLED")) {
                    def reason = result[0][7]
                    assertTrue(reason.contains("DATA_QUALITY_ERROR"), "should have DATA_QUALITY_ERROR or unknown load_id : $reason")
                    break
                }
                Thread.sleep(1000)
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    assertTrue(1 == 2, "load Timeout: $label")
                }
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("FragmentMgr::coordinator_callback.report_delay")
        }
    }
}