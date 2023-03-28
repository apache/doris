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

    def current_failed_tests = [
            // errCode = 2, detailMessage = (127.0.0.1)[CANCELLED]Expected EQ 1 to be returned by expression  (SCALARSUBQUERY) SubqueryExpr ( QueryPlan=LogicalAggregate[611] ( groupByExpr=[d_month_seq#130], outputExpr=[d_month_seq#130], hasRepeat=false ), CorrelatedSlots=[], typeCoercionExpr=null )
            "q06.sql",
            // errCode = 2, detailMessage = Unexpected exception: null
            "q10.sql",
            // errCode = 2, detailMessage = (127.0.0.1)[CANCELLED]Expected EQ 1 to be returned by expression  (SCALARSUBQUERY) SubqueryExpr ( QueryPlan=LogicalProject[1642] ( distinct=false, projects=[d_week_seq#536], excepts=[], canEliminate=true ), CorrelatedSlots=[], typeCoercionExpr=null )
            "q14_2.sql",
            // errCode = 2, detailMessage = Unexpected exception: null
            "q35.sql",
            // errCode = 2, detailMessage = (127.0.0.1)[CANCELLED]Expected EQ 1 to be returned by expression  (SCALARSUBQUERY) SubqueryExpr ( QueryPlan=LogicalAggregate[469] ( groupByExpr=[ss_store_sk#82], outputExpr=[avg(ss_net_profit#97) AS `rank_col`#98], hasRepeat=false ), CorrelatedSlots=[], typeCoercionExpr=null )
            "q44.sql",
            // errCode = 2, detailMessage = (127.0.0.1)[CANCELLED]Expected EQ 1 to be returned by expression  (SCALARSUBQUERY) SubqueryExpr ( QueryPlan=LogicalAggregate[619] ( groupByExpr=[(d_month_seq#241 + 1) AS `(d_month_seq + 1)`#266], outputExpr=[(d_month_seq#241 + 1) AS `(d_month_seq + 1)`#266], hasRepeat=false ), CorrelatedSlots=[], typeCoercionExpr=null )
            "q54.sql",
            // errCode = 2, detailMessage = (127.0.0.1)[CANCELLED]Expected EQ 1 to be returned by expression  (SCALARSUBQUERY) SubqueryExpr ( QueryPlan=LogicalProject[473] ( distinct=false, projects=[d_week_seq#105], excepts=[], canEliminate=true ), CorrelatedSlots=[], typeCoercionExpr=null )
            "q58.sql",
            // Memo.mergeGroup() dead loop
            "q64.sql",
            // RpcException, msg: io.grpc.StatusRuntimeException: UNAVAILABLE: Network closed for unknown reason
            "q77.sql"
    ].toSet()

    for (final def i in 1..10) {
        logger.info("retry times ${i}".toString())

        def success = []
        def failed = []

        for (final def sqlFile in sqlFiles) {
            if (current_failed_tests.contains(sqlFile.getName())) {
                continue
            }

            useDb()
            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set query_timeout=60"

            logger.info("execute ${sqlFile.getName()} [${++num}/${sqlFiles.size()}]".toString())
            try {
                // currently regression testing run in the asan mod and failed, so just run explain
                sql "explain ${sqlFile.text}"
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
