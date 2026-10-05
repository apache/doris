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

suite("test_wal_mem_back_pressure_time_out_fault_injection","nonConcurrent") {


    def tableName = "wal_test"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1")
        """

    def enable_back_pressure = {
        try {
            def fes = sql_return_maparray "show frontends"
            def bes = sql_return_maparray "show backends"
            logger.info("frontends: ${fes}")
                def fe = fes[0]
                def be = bes[0]
                    def url = "jdbc:mysql://${fe.Host}:${fe.QueryPort}/"
                    logger.info("observer url: " + url)
                    StringBuilder sb = new StringBuilder();
                    sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}")
                    sb.append("/rest/v2/manager/node/set_config/be")
                    sb.append(" -H \"Content-Type: application/json\" -H \"Authorization: Basic cm9vdDo= \"")
                    sb.append(""" -d \"{\\"group_commit_queue_mem_limit\\": {\\"node\\": [\\"${be.Host}:${be.HttpPort}\\"],\\"value\\": \\"0\\",\\"persist\\": \\"false\\"}}\"""")
                    String command = sb.toString()
                    logger.info(command)
                    def process = command.execute()

                    sb = new StringBuilder();
                    sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}")
                    sb.append("/rest/v2/manager/node/set_config/be")
                    sb.append(" -H \"Content-Type: application/json\" -H \"Authorization: Basic cm9vdDo= \"")
                    sb.append(""" -d \"{\\"group_commit_memory_rows_for_max_filter_ratio\\": {\\"node\\": [\\"${be.Host}:${be.HttpPort}\\"],\\"value\\": \\"0\\",\\"persist\\": \\"false\\"}}\"""")
                    command = sb.toString()
                    logger.info(command)
                    process = command.execute()
        } finally {
        }
    }

    def disable_back_pressure = {
        try {
            def fes = sql_return_maparray "show frontends"
            def bes = sql_return_maparray "show backends"
            logger.info("frontends: ${fes}")
                def fe = fes[0]
                def be = bes[0]
                    def url = "jdbc:mysql://${fe.Host}:${fe.QueryPort}/"
                    logger.info("observer url: " + url)
                    StringBuilder sb = new StringBuilder();
                    sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}")
                    sb.append("/rest/v2/manager/node/set_config/be")
                    sb.append(" -H \"Content-Type: application/json\" -H \"Authorization: Basic cm9vdDo= \"")
                    sb.append(""" -d \"{\\"group_commit_queue_mem_limit\\": {\\"node\\": [\\"${be.Host}:${be.HttpPort}\\"],\\"value\\": \\"67108864\\",\\"persist\\": \\"false\\"}}\"""")
                    String command = sb.toString()
                    logger.info(command)
                    def process = command.execute()

                    sb = new StringBuilder();
                    sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}")
                    sb.append("/rest/v2/manager/node/set_config/be")
                    sb.append(" -H \"Content-Type: application/json\" -H \"Authorization: Basic cm9vdDo= \"")
                    sb.append(""" -d \"{\\"group_commit_memory_rows_for_max_filter_ratio\\": {\\"node\\": [\\"${be.Host}:${be.HttpPort}\\"],\\"value\\": \\"10000\\",\\"persist\\": \\"false\\"}}\"""")
                    command = sb.toString()
                    logger.info(command)
                    process = command.execute()
        } finally {
        }
    }

    GetDebugPoint().clearDebugPointsForAllBEs()
    enable_back_pressure()

    sql """ set group_commit = async_mode; """
        try {
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue.add_block.back_pressure_time_out")
            sql """insert into ${tableName} values(1,1)"""
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('Wal memory back pressure wait too much time!'))
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue.add_block.back_pressure_time_out")
        }

    disable_back_pressure()

}