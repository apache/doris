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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;

class VariantEqualityContextTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("variant_equality_context_test");
        connectContext.setDatabase("variant_equality_context_test");
        createTables(
                "CREATE TABLE t1 (k INT, v VARIANT, j JSON) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")",
                "CREATE TABLE t2 (k INT, v VARIANT, j JSON) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")"
        );
    }

    @Test
    void testAllowedCanonicalHashContexts() {
        assertAllAccepted(
                "SELECT v, COUNT(*) FROM t1 GROUP BY v",
                "SELECT DISTINCT v FROM t1",
                "SELECT * FROM t1 JOIN t2 ON CAST(t1.v AS STRING) = CAST(t2.v AS STRING)",
                "SELECT v FROM t1 INTERSECT SELECT v FROM t2",
                "SELECT v FROM t1 EXCEPT SELECT v FROM t2",
                "SELECT v FROM t1 UNION SELECT v FROM t2",
                "SELECT COUNT(DISTINCT v) FROM t1");
    }

    @Test
    void testRejectedVariantOrderingAndMixedComparisons() {
        assertVariantComparisonRejected("SELECT v = k FROM t1");
        assertVariantComparisonRejected("SELECT v > v FROM t1");
        assertVariantComparisonRejected("SELECT * FROM t1 JOIN t2 ON t1.v > t2.v");
    }

    @Test
    void testVariantEquality() {
        assertAllAccepted(
                "SELECT v = v, v != v, v <=> v FROM t1",
                "SELECT * FROM t1 JOIN t2 ON t1.v = t2.v",
                "SELECT * FROM t1 LEFT JOIN t2 ON t1.v <=> t2.v",
                "SELECT * FROM t1 FULL JOIN t2 ON t1.v = t2.v AND t1.k = t2.k",
                "SELECT * FROM t1 JOIN t2 ON t1.v = t2.v OR t1.k = t2.k",
                "SELECT EXISTS(SELECT 1 FROM t2 WHERE t1.v = t2.v) FROM t1",
                "SELECT * FROM t1 WHERE v IN (SELECT v FROM t2)",
                "SELECT * FROM t1 WHERE v NOT IN (SELECT v FROM t2)",
                "SELECT * FROM t1 JOIN t2 ON t1.v['id'] = t2.v['id']");
        assertVariantComparisonRejected("SELECT * FROM t1 JOIN t2 ON t1.v > t2.v");
        assertVariantComparisonRejected("SELECT * FROM t1 JOIN t2 ON t1.v = t2.k");
    }

    @Test
    void testVariantOrdering() {
        assertPlanAccepted("SELECT v FROM t1 ORDER BY v");
        assertPlanAccepted("SELECT v FROM t1 ORDER BY v LIMIT 10");
        assertPlanAccepted("SELECT row_number() OVER (ORDER BY v) FROM t1");
    }

    @Test
    void testOtherMetricAndJsonBehaviorDoesNotChange() {
        assertRejected("SELECT MAX(v) FROM t1", "Doris hll, bitmap");
        assertRejected("SELECT MIN(v) FROM t1", "Doris hll, bitmap");
        assertRejected("SELECT NDV(v) FROM t1", "Doris hll, bitmap");
        assertAccepted("SELECT j, COUNT(*) FROM t1 GROUP BY j");
        assertAccepted("SELECT j FROM t1 INTERSECT SELECT j FROM t2");
        assertRejected("SELECT * FROM t1 JOIN t2 ON t1.j = t2.j",
                "comparison predicate could not contains json type");
    }

    private void assertAccepted(String sql) {
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
    }

    private void assertAllAccepted(String... sqlStatements) {
        Assertions.assertAll(Arrays.stream(sqlStatements)
                .map(sql -> (Executable) () -> assertAccepted(sql)));
    }

    private void assertPlanAccepted(String sql) {
        connectContext.setQueryId(new TUniqueId(1, 1));
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).plan(sql), sql);
    }

    private void assertVariantComparisonRejected(String sql) {
        assertRejected(sql, "CAST to a concrete type first");
    }

    private void assertRejected(String sql, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
    }
}
