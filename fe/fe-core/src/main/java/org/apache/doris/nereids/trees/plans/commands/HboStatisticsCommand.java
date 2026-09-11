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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.stats.GroupStructInfo;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.PinnedType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Manual HBO statistics management statements:
 * <pre>
 *   HBO SET [PINNED|LEARNED] STATISTICS '&lt;fingerprint&gt;' = &lt;rows&gt; [TYPE EXACT|FILTER_SMALL]
 *       [STRUCT '&lt;canonical&gt;']
 *   HBO DELETE [PINNED|LEARNED] STATISTICS '&lt;fingerprint&gt;'
 * </pre>
 * The optional scope selects which cache is written: {@code PINNED} (default) injects an entry that
 * is authoritative over the learned entries, {@code LEARNED} injects into the learned cache so the
 * learned lookup path can be exercised without a real profile publish. The injected row count is
 * bound to the fingerprint the user copied from EXPLAIN; whether that fingerprint carries literals
 * (filter exact form) or not (filter shape form and all join / aggregation keys) is derived
 * automatically on first use and reported by {@code HBO SHOW STATISTICS}.
 */
public class HboStatisticsCommand extends Command {

    /** Operation kind. */
    public enum Op {
        SET,
        DELETE
    }

    /** Which cache the statement targets. */
    public enum Scope {
        PINNED,
        LEARNED
    }

    private static final Pattern FINGERPRINT_PATTERN = Pattern.compile("[0-9a-fA-F]{64}");

    private final Op op;
    private final Scope scope;
    private final String fingerprint;
    private final long rows;
    private final PinnedType type;
    private final String structCanonical;

    /**
     * HboStatisticsCommand
     * @param op SET or DELETE
     * @param scopeName optional PINNED / LEARNED, null or empty means PINNED
     * @param fingerprint hbo fingerprint (sha256 of the simplified group struct info)
     * @param rows injected output row count (only meaningful for SET)
     * @param typeName optional EXACT / FILTER_SMALL, null means EXACT
     * @param structCanonical optional human-readable simplified struct info canonical string
     */
    public HboStatisticsCommand(Op op, String scopeName, String fingerprint, long rows,
            String typeName, String structCanonical) {
        super(PlanType.HBO_STATISTICS_COMMAND);
        this.op = op;
        this.scope = parseScope(scopeName);
        // sha256 hex fingerprints are lowercase everywhere (group struct info, read-side lookup);
        // normalize user input so an uppercase fingerprint cannot silently miss its entry
        this.fingerprint = fingerprint == null ? null : fingerprint.toLowerCase(Locale.ROOT);
        this.rows = rows;
        this.type = typeName == null ? PinnedType.EXACT : PinnedType.fromName(typeName);
        this.structCanonical = structCanonical == null ? "" : structCanonical;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied: HBO statistics management requires ADMIN privilege");
        }
        HboPlanStatisticsManager hboManager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        // validate the fingerprint shape on both operations: a malformed DELETE would otherwise
        // silently leave the pinned entry active (it can never match a stored fingerprint)
        validateFingerprint(fingerprint);
        if (type == null) {
            throw new AnalysisException("invalid hbo statistics type, expect EXACT or FILTER_SMALL");
        }
        if (scope == Scope.LEARNED && type != PinnedType.EXACT) {
            // a learned entry only carries the row count; the guard type is a pinned-only concept
            throw new AnalysisException("TYPE is not supported for hbo learned statistics");
        }
        if (op == Op.SET) {
            // a pinned entry is only displayable when it carries its struct info, and a struct info
            // which does not belong to the fingerprint would make the SHOW output misleading
            validateStructCanonical(fingerprint, structCanonical, scope != Scope.LEARNED);
        }
        switch (op) {
            case SET:
                if (rows < 0) {
                    throw new AnalysisException("hbo statistics rows must be non-negative: " + rows);
                }
                if (scope == Scope.LEARNED) {
                    hboManager.putLearnedPlanStatistics(fingerprint, rows, structCanonical);
                } else {
                    hboManager.putPinnedPlanStatistics(fingerprint, rows, type, structCanonical);
                }
                break;
            case DELETE:
                if (scope == Scope.LEARNED) {
                    hboManager.removeLearnedPlanStatistics(fingerprint);
                } else {
                    hboManager.removePinnedPlanStatistics(fingerprint);
                }
                break;
            default:
                throw new IllegalStateException("unexpected hbo statistics op: " + op);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    public Op getOp() {
        return op;
    }

    public Scope getScope() {
        return scope;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public long getRows() {
        return rows;
    }

    public PinnedType getPinnedType() {
        return type;
    }

    public String getStructCanonical() {
        return structCanonical;
    }

    private static Scope parseScope(String scopeName) {
        if (scopeName == null || scopeName.isEmpty() || "pinned".equalsIgnoreCase(scopeName)) {
            return Scope.PINNED;
        }
        if ("learned".equalsIgnoreCase(scopeName)) {
            return Scope.LEARNED;
        }
        throw new IllegalArgumentException(
                "invalid hbo statistics scope, expect PINNED or LEARNED: " + scopeName);
    }

    /**
     * Check that the pasted struct info describes the sub tree the fingerprint was taken from. The
     * canonical form is accepted as it is printed by EXPLAIN, and also with every literal replaced
     * by {@code lit(*)} (see {@link GroupStructInfo#toNoLiteral}) because the constant agnostic
     * fingerprint of a filter node is looked up with exactly that struct info.
     *
     * @param required whether a missing struct info is an error (pinned entries are displayed by
     *                 {@code HBO SHOW STATISTICS}, so they must carry one)
     */
    private void validateStructCanonical(String targetFingerprint, String structCanonical, boolean required)
            throws AnalysisException {
        if (structCanonical.isEmpty()) {
            if (required) {
                throw new AnalysisException("hbo statistics STRUCT is required,"
                        + " copy the struct= value of the target node from EXPLAIN");
            }
            return;
        }
        if (isFingerprintOf(targetFingerprint, structCanonical)
                || isFingerprintOf(targetFingerprint, GroupStructInfo.toNoLiteral(structCanonical))) {
            return;
        }
        throw new AnalysisException("hbo statistics STRUCT does not match the fingerprint " + targetFingerprint
                + ", copy the struct= value of the target node from EXPLAIN");
    }

    private static boolean isFingerprintOf(String targetFingerprint, String structCanonical) {
        return targetFingerprint.equals(Hashing.sha256()
                .hashString(structCanonical, StandardCharsets.UTF_8).toString());
    }

    private void validateFingerprint(String targetFingerprint) throws AnalysisException {
        if (targetFingerprint == null || !FINGERPRINT_PATTERN.matcher(targetFingerprint).matches()) {
            throw new AnalysisException("invalid hbo fingerprint, expect 64 hex chars: " + targetFingerprint);
        }
    }
}
