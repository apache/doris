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

suite("auto_partition_quorum_race_docker", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 1
    options.beNum = 3
    options.beConfigs += [
        "enable_quorum_success_write=true"
    ]

    docker(options) {
        // Exercise the normal scheduler. No preferred key, affinity mode, or
        // non-default backend location tag participates in this test.
        sql "set enable_load_backend_selection = false"
        sql "set preferred_backend_selection_key = ''"
        sql "set backend_selection_mode = 'default'"

        assertEquals("false",
                sql("show variables like 'enable_load_backend_selection'")[0][1].toString())
        assertEquals("",
                sql("show variables like 'preferred_backend_selection_key'")[0][1].toString())
        assertEquals("default",
                sql("show variables like 'backend_selection_mode'")[0][1].toString())

        sql "drop table if exists auto_partition_race_src force"
        sql """
            create table auto_partition_race_src (
                k0 date not null
            )
            distributed by hash(k0) buckets 2
            properties("replication_num" = "1")
        """

        // The two buckets are scanned by two sink instances. Their completion
        // times intentionally differ, matching the original DORIS-27500 case.
        sql """insert into auto_partition_race_src values ("2012-12-11")"""
        sql """
            insert into auto_partition_race_src
            select "2020-12-12" from numbers("number" = "20000")
        """

        def sourceTablets = sql_return_maparray("show tablets from auto_partition_race_src")
        def sourceBackendIds = sourceTablets.collect { it.BackendId.toString() }.toSet()
        assertEquals(2, sourceTablets.size())
        assertEquals(2, sourceBackendIds.size(),
                "source tablets must reside on two BEs to create two remote sink instances")

        int attempts = 20
        int failures = 0
        def failureMessages = []
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            def tableName = "auto_partition_race_dst_${attempt}"
            sql "drop table if exists ${tableName} force"
            sql """
                create table ${tableName} (
                    k0 date not null
                )
                auto partition by range (date_trunc(k0, 'day')) ()
                distributed by hash(k0) buckets 10
                properties("replication_num" = "3")
            """

            try {
                sql "insert into ${tableName} select * from auto_partition_race_src"
                assertEquals(20001L, sql("select count(*) from ${tableName}")[0][0] as long)
            } catch (Exception e) {
                ++failures
                failureMessages.add("attempt ${attempt}: ${e.getMessage()}")
                logger.warn("DORIS-27500 reproduced on attempt ${attempt}: ${e.getMessage()}")
            } finally {
                sql "drop table if exists ${tableName} force"
            }
        }

        logger.info("DORIS-27500 reproduction result: ${failures}/${attempts} failed")
        assertEquals(0, failures,
                "ordinary auto partition loads failed without affinity:\n"
                        + failureMessages.join("\n"))
    }
}
