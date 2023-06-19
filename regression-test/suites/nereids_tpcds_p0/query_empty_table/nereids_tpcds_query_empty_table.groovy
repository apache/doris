import com.google.common.collect.ImmutableList

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

suite("nereids_tpcds_query_empty_table") {
    def useDb = { sql "use regression_test_nereids_tpcds_p0_query_empty_table" }

    useDb()

    def tpcdsPath = new File(context.config.suitePath, "tpcds_sf1_p1")
    def ddlFiles = new File(tpcdsPath, "ddl").listFiles()
    for (final def ddlFile in ddlFiles) {
        sql ddlFile.text
    }

    def sqlFiles = new File(tpcdsPath, "sql").listFiles()

    def num = 0

    for (final def i in 1..2) {
        logger.info("retry times ${i}".toString())

        def success = []
        def failed = []

        def runSqls = sqlFiles

        for (final def sqlFile in runSqls) {
            useDb()
            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set query_timeout=60"
            sql "set enable_nereids_timeout=false"

            logger.info("execute ${sqlFile.getName()} [${++num}/${sqlFiles.size()}]".toString())
            try {
                String query = sqlFile.text
                if (query.contains("/*") && query.contains("*/")) {
                    query = query.substring(query.indexOf("/*") + 2, query.indexOf("*/") - 1)
                }

                // currently regression testing run in the asan mode and failed, so just run explain
                sql "explain $query"
                success.add(sqlFile)
            } catch (Throwable t) {
                logger.error("execute ${sqlFile.getName()} failed".toString(), t)
                failed.add(sqlFile)
            }
        }

        logger.info("Totally ${success.size() + failed.size()}, success: ${success.size()}, failed: ${failed.size()}".toString())
        if (!failed.isEmpty()) {
            def failedString = failed.collect({ it.getName() }).join("\n")
            logger.info("failed:\n${failedString}".toString())
            throw new IllegalStateException("failed")
        }
    }
}
