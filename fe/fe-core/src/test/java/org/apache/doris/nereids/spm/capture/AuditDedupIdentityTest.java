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

package org.apache.doris.nereids.spm.capture;

import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The scan-selector pre-filter of the audit dedup identity.
 *
 * The audit digest MASKS every selector value (PARTITION(p1) and PARTITION(p2) both
 * render as PARTITION(?)), while SPM compares the selectors CONCRETELY
 * (sameScanIdentity / sameScanParams), so the concrete fingerprint must join the identity
 * for every statement that can carry one. The gate named "FOR TIMESTAMP", a form the
 * grammar does not accept: time-travel statements (FOR VERSION AS OF / FOR TIME AS OF) and
 * relation scan parameters (t @incr(...) / @branch(...) / @tag(...))
 * therefore never got a fingerprint, and two same-digest variants collapsed into ONE
 * identity - one of the two baselines was silently dropped by the capture.
 */
public class AuditDedupIdentityTest {

    private static final long MODE = SqlModeHelper.MODE_DEFAULT;

    /** The digest of such a statement: the concrete version / time is masked. */
    private static final String DIGEST = "SELECT * FROM db.t FOR TIME AS OF ?";

    private static String identity(String stmt) {
        return AuditLogScanner.dedupIdentity(stmt, DIGEST, MODE);
    }

    // ==================== the gate recognizes the real syntax ====================

    @Test
    public void testGateRecognizesTheRealSelectorSyntaxes() {
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t FOR VERSION AS OF 3"));
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t FOR TIME AS OF '2026-01-01 00:00:00'"));
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t @incr(branch = 'main')"));
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t PARTITION(p1)"));
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t TABLET(10)"));
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t TABLESAMPLE(10 ROWS)"));
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t INDEX(idx)"));

        Assertions.assertFalse(AuditLogScanner.mentionsScanSelector(null));
        Assertions.assertFalse(AuditLogScanner.mentionsScanSelector("SELECT a FROM db.t"),
                "a statement without a selector keeps the plain digest");
    }

    /**
     * Audit_log.stmt is the statement AS SUBMITTED, so the
     * multi-token gates (LATERAL VIEW, FOR TIME AS OF, FOR VERSION
     * AS OF) must survive a line break between their tokens. The gate compared against
     * the raw upper-cased text, so a selector on its own line was missed, the fingerprint
     * stayed off the dedup identity, and two variants differing only in the masked
     * argument (split delimiter, snapshot) collapsed into one baseline.
     */
    @Test
    public void testGateSurvivesLineBreaksBetweenTokens() {
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT * FROM db.t\nFOR TIME AS OF '2026-01-01 00:00:00'"),
                "a time-travel selector on its own line must be recognized");
        Assertions.assertTrue(AuditLogScanner.mentionsScanSelector(
                "SELECT *\nFROM db.t\nFOR VERSION\nAS OF 3"),
                "even a break inside the marker must be tolerated");
        Assertions.assertTrue(AuditLogScanner.mentionsGenerator(
                "SELECT a, c\nFROM db.t\nLATERAL VIEW explode(split(s, ',')) e AS c"),
                "an indented LATERAL VIEW owns concrete generator arguments");
        Assertions.assertTrue(AuditLogScanner.mentionsGenerator(
                "SELECT a, c\nFROM db.t\nUNNEST(s) AS c"),
                "UNNEST stays recognized as well");
        Assertions.assertFalse(AuditLogScanner.mentionsGenerator(
                "SELECT a FROM db.t\nWHERE a > 0"),
                "a plain statement still takes the plain digest");

        // the identity level: a masked snapshot on its own line still refines the digest
        Assertions.assertNotEquals(
                identity("SELECT * FROM db.t\nFOR TIME AS OF '2026-01-01 00:00:00'"),
                identity("SELECT * FROM db.t\nFOR TIME AS OF '2026-01-02 00:00:00'"),
                "two snapshots must not collapse into one identity because of a newline");
    }

    /** A statement without a selector is unchanged: its identity IS the digest. */
    @Test
    public void testPlainStatementKeepsTheDigestIdentity() {
        Assertions.assertEquals(DIGEST,
                AuditLogScanner.dedupIdentity("SELECT a FROM db.t", DIGEST, MODE));
    }

    // ==================== time travel variants stay apart ====================

    @Test
    public void testTimeTravelVariantsHaveDistinctIdentities() {
        String version3 = identity("SELECT * FROM db.t FOR VERSION AS OF 3");
        String version4 = identity("SELECT * FROM db.t FOR VERSION AS OF 4");
        Assertions.assertNotEquals(version3, version4,
                "FOR VERSION AS OF 3 and AS OF 4 are different baselines (the digest masks"
                        + " the version)");
        Assertions.assertTrue(version3.startsWith(DIGEST),
                "the identity is the digest refined with the concrete selectors: " + version3);

        String timeA = identity("SELECT * FROM db.t FOR TIME AS OF '2026-01-01 00:00:00'");
        String timeB = identity("SELECT * FROM db.t FOR TIME AS OF '2026-01-02 00:00:00'");
        Assertions.assertNotEquals(timeA, timeB,
                "two time-travel points are different baselines");
        Assertions.assertNotEquals(version3, timeA,
                "a version and a time are different snapshots");
    }

    // ==================== relation scan parameters stay apart ====================

    @Test
    public void testScanParamVariantsHaveDistinctIdentities() {
        String branchA = identity("SELECT * FROM db.t @incr(branch = 'b1')");
        String branchB = identity("SELECT * FROM db.t @incr(branch = 'b2')");
        Assertions.assertNotEquals(branchA, branchB,
                "the @incr branch argument is compared concretely by SPM (sameScanParams)");
        Assertions.assertNotEquals(identity("SELECT * FROM db.t @incr(branch = 'b1')"),
                identity("SELECT * FROM db.t @tag(branch = 'b1')"),
                "the scan parameter TYPE takes part in the identity");
        Assertions.assertNotEquals(identity("SELECT * FROM db.t @options(branch = 'b1')"),
                identity("SELECT * FROM db.t @options(branch = 'b1', x = 'y')"),
                "the parameter payload takes part in the identity");
    }

    /** The partition / tablet / sample selectors keep working (unchanged path). */
    @Test
    public void testClassicSelectorVariantsHaveDistinctIdentities() {
        Assertions.assertNotEquals(identity("SELECT * FROM db.t PARTITION(p1)"),
                identity("SELECT * FROM db.t PARTITION(p2)"));
        Assertions.assertNotEquals(identity("SELECT * FROM db.t TABLET(1)"),
                identity("SELECT * FROM db.t TABLET(2)"));
        Assertions.assertNotEquals(
                identity("SELECT * FROM db.t TABLESAMPLE(10 ROWS) REPEATABLE 1000"),
                identity("SELECT * FROM db.t TABLESAMPLE(11 ROWS) REPEATABLE 39"),
                "the sample's concrete fields are the identity (see SPMRound29SafetyTest)");
    }
}
