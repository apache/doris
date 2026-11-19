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

package org.apache.doris.regression.suite

import org.apache.doris.regression.Config
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertSame
import static org.junit.jupiter.api.Assertions.assertThrows

class SuitePaimonIncrementalTest {
    private static SuiteContext newContext() {
        def config = new Config()
        config.suitePath = new File("regression-test/suites").absolutePath
        config.dataPath = new File("regression-test/data").absolutePath
        config.realDataPath = new File("regression-test/realdata").absolutePath
        config.defaultDb = "regression_test"
        def file = new File(config.suitePath, "paimon_incremental.groovy")
        def scriptContext = new ScriptContext(file, null, null, config, [], { true })
        def cluster = new SuiteCluster("paimon_incremental", config)
        return new SuiteContext(file, "paimon_incremental", "", scriptContext,
                cluster, null, null, config)
    }

    private static class RecordingSuite extends Suite {
        final List<String> statements = []
        final RuntimeException queryFailure = new IllegalStateException("query failed")

        RecordingSuite() {
            this(SuitePaimonIncrementalTest.newContext())
        }

        private RecordingSuite(SuiteContext context) {
            super("paimon_incremental", "", context, context.cluster)
        }

        @Override
        List<List<Object>> spark_paimon(String query, boolean isOrder = false) {
            statements.add(query.toString())
            if (query == "invalid query") {
                throw queryFailure
            }
            return [["+I", 3]]
        }
    }

    @Test
    void boundsReadToSuiteDatabaseAndReturnsRows() {
        def suite = new RecordingSuite()
        def rows = suite.spark_paimon_incremental("suite_db", "1", "2", "query")

        assertEquals([["+I", 3]], rows)
        assertEquals([
                'SET `spark.paimon.paimon.suite_db.*.incremental-between`=1,2',
                'query',
                'RESET `spark.paimon.paimon.suite_db.*.incremental-between`'
        ], suite.statements)
    }

    @Test
    void clearsSnapshotRangeWhenQueryFails() {
        def suite = new RecordingSuite()
        def failure = assertThrows(IllegalStateException) {
            suite.spark_paimon_incremental("suite_db", 1L, 2L, "invalid query")
        }

        assertSame(suite.queryFailure, failure)
        assertEquals('RESET `spark.paimon.paimon.suite_db.*.incremental-between`',
                suite.statements.last())
    }
}
