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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
suite("nereids_tpcds_sf1") {
    def success = []
    def failed = []
    def files = new File(context.file.parent, "sql").listFiles()
    def num = 0
    for (final def sqlFile in files) {
        def name = sqlFile.getName()
        if (!["q43.sql", "q73.sql", "q25.sql", "q83.sql", "q88.sql"].contains(sqlFile.getName())) {
            continue
        }

        sql "use tpcds1"
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set query_timeout=60"

        logger.info("execute ${sqlFile.getName()} [${++num}/${files.size()}]".toString())
        try {
            sql sqlFile.text
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
    }
}
