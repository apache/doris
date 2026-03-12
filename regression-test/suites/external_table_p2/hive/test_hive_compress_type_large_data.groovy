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

suite("test_hive_compress_type_large_data", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    def backends = sql """show backends"""
    def backendNum = backends.size()
    logger.info("get backendNum: ${backendNum}")
    // `parallel_fragment_exec_instance_num` may be displayed as
    // `deprecated_parallel_fragment_exec_instance_num` in newer branches.
    def parallelExecInstanceRows = sql("show variables like '%parallel_fragment_exec_instance_num%'")
    assertTrue(parallelExecInstanceRows.size() > 0)
    def parallelExecInstanceNum = (parallelExecInstanceRows[0][1] as String).toInteger()
    logger.info("get ${parallelExecInstanceRows[0][0]}: ${parallelExecInstanceNum}")

    for (String hivePrefix : ["hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_compress_type_large_data"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """use `${catalog_name}`.`multi_catalog`"""

        // table test_compress_partitioned has mixed compressed files and larger data volume.
        sql """set file_split_size=0"""
        def expectedSplitNum = 16
        if (backendNum > 1) {
            expectedSplitNum = (16 < parallelExecInstanceNum * backendNum) ? 28 : 16
        }
        explain {
            sql("select count(*) from test_compress_partitioned")
            contains "inputSplitNum=${expectedSplitNum}, totalFileSize=734675596, scanRanges=${expectedSplitNum}"
            contains "partition=8/8"
        }

        def countMix1 = sql """select count(*) from test_compress_partitioned where dt="gzip" or dt="mix""""
        assertEquals(600005, countMix1[0][0])
        def countAll1 = sql """select count(*) from test_compress_partitioned"""
        assertEquals(1510010, countAll1[0][0])
        def countWatchId1 = sql """select count(*) from test_compress_partitioned where watchid=4611870011201662970"""
        assertEquals(15, countWatchId1[0][0])

        sql """set file_split_size=8388608"""
        explain {
            sql("select count(*) from test_compress_partitioned")
            contains "inputSplitNum=16, totalFileSize=734675596, scanRanges=16"
            contains "partition=8/8"
        }

        def countMix2 = sql """select count(*) from test_compress_partitioned where dt="gzip" or dt="mix""""
        assertEquals(600005, countMix2[0][0])
        def countAll2 = sql """select count(*) from test_compress_partitioned"""
        assertEquals(1510010, countAll2[0][0])
        def countWatchId2 = sql """select count(*) from test_compress_partitioned where watchid=4611870011201662970"""
        assertEquals(15, countWatchId2[0][0])

        sql """set file_split_size=0"""
    }
}
