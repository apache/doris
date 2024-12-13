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

suite("test_resource_tag") {
        sql "drop user if exists test_rg;"
        sql "create user test_rg"
        sql "GRANT SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV ON *.*.* TO test_rg;"
        sql "set property for test_rg 'resource_tags.location' = 'c3p0';"
        //cloud-mode
        if (isCloudMode()) {
                def clusters = sql " SHOW CLUSTERS; "
                assertTrue(!clusters.isEmpty())
                def validCluster = clusters[0][0]
                sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO test_rg""";
        }

        // test query
        connect('test_rg', '', context.config.jdbcUrl) {
                sql "drop table if exists test_skip_rg_bad_replica_tab;"
                sql """
            CREATE TABLE test_skip_rg_bad_replica_tab
                (
                    k1 int,
                    k2 int,
                )ENGINE=OLAP
                duplicate KEY(k1)
                DISTRIBUTED BY HASH (k1) BUCKETS 1
                PROPERTIES(
                'replication_allocation' = 'tag.location.default: 1'
                );
            """
                sql """
        insert into test_skip_rg_bad_replica_tab values
        (9,10),
        (1,2)
        """
                test {
                        sql "select count(1) as t1 from test_skip_rg_bad_replica_tab;"
                        exception "which is not in user's resource tags: [{\"location\" : \"c3p0\"}], If user specified tag has no queryable replica, you can set property 'allow_resource_tag_downgrade'='true' to skip resource tag."
                }
        }
        sql "set property for test_rg 'allow_resource_tag_downgrade' = 'true';"

        connect('test_rg', '', context.config.jdbcUrl) {
                sql "select count(1) as t2 from test_skip_rg_bad_replica_tab;"
                sql "drop table test_skip_rg_bad_replica_tab";
        }


        // test stream load
        sql "set property for test_rg 'allow_resource_tag_downgrade' = 'false';"
        sql """ DROP TABLE IF EXISTS ${context.config.defaultDb}.skip_rg_test_table """
        sql """
            CREATE TABLE IF NOT EXISTS ${context.config.defaultDb}.skip_rg_test_table (
                `k1` int NULL,
                `k2` int NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        def test_failed_command = "curl --location-trusted -u test_rg: -H column_separator:| -H Transfer-Encoding:chunked -H columns:k1,k2  -T ${context.dataPath}/skip_rg_test_table.csv http://${context.config.feHttpAddress}/api/${context.config.defaultDb}/skip_rg_test_table/_stream_load"
        log.info("stream load skip_rg_test_table failed test cmd: ${test_failed_command}")
        def process = test_failed_command.execute()
        def code1 = process.waitFor()
        def out1 = process.text
        log.info("stream load skip_rg_test_table failed test result, ${out1}".toString())
        assertTrue("${out1}".toString().contains("No backend load available") || "${out1}".toString().contains("No available backends"))

        sql "set property for test_rg 'allow_resource_tag_downgrade' = 'true';"

        def test_succ_command = "curl --location-trusted -u test_rg: -H column_separator:| -H Transfer-Encoding:chunked -H columns:k1,k2  -T ${context.dataPath}/skip_rg_test_table.csv http://${context.config.feHttpAddress}/api/${context.config.defaultDb}/skip_rg_test_table/_stream_load"
        def process2 = test_succ_command.execute()
        def code2 = process2.waitFor()
        def out2 = process2.text
        def jsonRet = parseJson(out2)
        log.info("stream load skip_rg_test_table succ test result, ${out2}".toString())
        assertFalse("${out2}".toString().contains("No backend load available"))
        assertTrue(jsonRet['Status'] == 'Success')


        // clear
        sql "drop user test_rg"
        sql "drop table ${context.config.defaultDb}.skip_rg_test_table"
}