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

import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;

class VariantEqualityContextTest extends TestWithFeService {

    private boolean originalEnableVariantV2;

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
    void testLegacyVariantBehaviorIsPreservedWhenV2IsDisabled() {
        boolean originalEnableVariantV2 = Config.enable_variant_v2;
        try {
            Config.enable_variant_v2 = false;

            assertRejected("SELECT v, COUNT(*) FROM t1 GROUP BY v", "Doris hll, bitmap");
            assertCheckRejected("SELECT v FROM t1 ORDER BY v", "Doris hll, bitmap");
            assertCheckRejected("SELECT v FROM t1 ORDER BY v LIMIT 10", "Doris hll, bitmap");
            assertRejected("SELECT row_number() OVER (ORDER BY v) FROM t1", "Doris hll, bitmap");
            assertRejected("SELECT * FROM t1 JOIN t2 ON t1.v = t2.v", "could not used in ComparisonPredicate");
            assertRejected("SELECT DISTINCT v FROM t1", "Doris hll, bitmap");
            assertRejected("SELECT COUNT(DISTINCT v) FROM t1", "COUNT DISTINCT");
            assertRejected("SELECT v FROM t1 INTERSECT SELECT v FROM t2", "Doris hll, bitmap");
            assertRejected("SELECT v FROM t1 EXCEPT SELECT v FROM t2", "Doris hll, bitmap");
            assertRejected("SELECT v FROM t1 UNION SELECT v FROM t2", "Doris hll, bitmap");

            assertAllAccepted(
                    "SELECT v = k FROM t1",
                    "SELECT v > k FROM t1",
                    "SELECT parse_to_variant('1') = 1",
                    "SELECT json_extract(v, '$') FROM t1",
                    "SELECT v FROM t1 UNION ALL SELECT v FROM t2",
                    "SELECT * FROM t1 JOIN t2 ON CAST(t1.v AS STRING) = CAST(t2.v AS STRING)",
                    "SELECT CAST(v AS STRING) = CAST(v AS STRING) FROM t1");
        } finally {
            Config.enable_variant_v2 = originalEnableVariantV2;
        }
    }

    @BeforeEach
    void enableVariantV2() {
        originalEnableVariantV2 = Config.enable_variant_v2;
        Config.enable_variant_v2 = true;
    }

    @AfterEach
    void restoreVariantV2() {
        Config.enable_variant_v2 = originalEnableVariantV2;
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
    void testLegacyVariantEqualityWhenDefaultIsDisabled() {
        Config.enable_variant_v2 = false;
        assertRejected("SELECT parse_to_variant('1') = parse_to_variant('1.0')",
                "could not used in ComparisonPredicate");
        assertRejected("SELECT parse_to_variant('1') <=> NULL",
                "could not used in ComparisonPredicate");
        assertRejected("SELECT * FROM t1 JOIN t2 ON t1.v = t2.v",
                "could not used in ComparisonPredicate");
    }

    @Test
    void testVariantV2CanonicalHashContexts() {
        assertAllAccepted(
                "SELECT parse_to_variant(CAST(k AS STRING)), COUNT(*) FROM t1 "
                        + "GROUP BY parse_to_variant(CAST(k AS STRING))",
                "SELECT DISTINCT parse_to_variant(CAST(k AS STRING)) FROM t1",
                "SELECT COUNT(DISTINCT parse_to_variant(CAST(k AS STRING))) FROM t1",
                "SELECT parse_to_variant(CAST(k AS STRING)) FROM t1 INTERSECT "
                        + "SELECT parse_to_variant(CAST(k AS STRING)) FROM t2",
                "SELECT parse_to_variant(CAST(k AS STRING)) FROM t1 EXCEPT "
                        + "SELECT parse_to_variant(CAST(k AS STRING)) FROM t2",
                "SELECT parse_to_variant(CAST(k AS STRING)) FROM t1 UNION "
                        + "SELECT parse_to_variant(CAST(k AS STRING)) FROM t2");
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

    private void assertCheckRejected(String sql, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql).applyBottomUp(new CheckAfterRewrite()), sql);
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
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
