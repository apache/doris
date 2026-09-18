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
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.LiteralMode;
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
    // a row count, or the fan-out factor of a PinnedType.JOIN_EXPANSION entry
    private final double value;
    private final PinnedType type;
    private final String structCanonical;
    // which struct info the entry is keyed by (WITH_LITERAL / NO_LITERAL), null means "derive"
    private final String literalModeName;

    /**
     * HboStatisticsCommand
     * @param op SET or DELETE
     * @param scopeName optional PINNED / LEARNED, null or empty means PINNED
     * @param fingerprint hbo fingerprint (sha256 of the simplified group struct info)
     * @param value injected output row count, or the fan-out factor for JOIN_EXPANSION
     * @param typeName optional EXACT / FILTER_SMALL, null means EXACT
     * @param structCanonical optional human-readable simplified struct info canonical string
     * @param literalModeName optional WITH_LITERAL / NO_LITERAL, null means the constant agnostic
     *                        (NO_LITERAL) form, which is also what the struct is folded to
     */
    public HboStatisticsCommand(Op op, String scopeName, String fingerprint, double value,
            String typeName, String structCanonical, String literalModeName) {
        super(PlanType.HBO_STATISTICS_COMMAND);
        this.op = op;
        this.scope = parseScope(scopeName);
        // sha256 hex fingerprints are lowercase everywhere (group struct info, read-side lookup);
        // normalize user input so an uppercase fingerprint cannot silently miss its entry
        this.fingerprint = fingerprint == null ? null : fingerprint.toLowerCase(Locale.ROOT);
        this.value = value;
        this.type = typeName == null ? PinnedType.EXACT : PinnedType.fromName(typeName);
        this.structCanonical = structCanonical == null ? "" : structCanonical;
        this.literalModeName = literalModeName;
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
            throw new AnalysisException(
                    "invalid hbo statistics type, expect EXACT, FILTER_SMALL or JOIN_EXPANSION");
        }
        if (scope == Scope.LEARNED && type != PinnedType.EXACT) {
            // a learned entry only carries the row count; the other types are pinned-only concepts
            throw new AnalysisException("TYPE is not supported for hbo learned statistics");
        }
        if (scope == Scope.LEARNED && literalModeName != null) {
            throw new AnalysisException("LITERAL_MODE is not supported for hbo learned statistics");
        }
        if (op == Op.SET) {
            LiteralMode literalMode = resolveLiteralMode(literalModeName);
            // a pinned entry is only displayable when it carries its struct info, and a struct info
            // which does not belong to the fingerprint would make the SHOW output misleading
            String canonical = validateStructCanonical(fingerprint, structCanonical, type, literalMode,
                    scope != Scope.LEARNED);
            if (type == PinnedType.JOIN_EXPANSION) {
                // a fan-out factor: >= 1 means the join expands, < 1 means it filters (0.1 keeps 10%
                // of the left input), so only zero and negative values are meaningless
                if (value <= 0) {
                    throw new AnalysisException(
                            "hbo join expansion must be greater than 0: " + value);
                }
            } else if (value < 0 || value != Math.floor(value)) {
                // the VALUE of every other type is a row count
                throw new AnalysisException("hbo statistics VALUE must be a non-negative integer"
                        + " for type " + type + ": " + value);
            }
            if (scope == Scope.LEARNED) {
                hboManager.putLearnedPlanStatistics(fingerprint, (long) value, structCanonical);
            } else if (type == PinnedType.JOIN_EXPANSION) {
                hboManager.putPinnedExpansionStatistics(fingerprint, value, canonical);
            } else {
                hboManager.putPinnedPlanStatistics(fingerprint, (long) value, type, canonical, literalMode);
            }
            return;
        }
        switch (op) {
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

    /**
     * Which struct info the entry is keyed by. An explicit LITERAL_MODE wins (the pasted struct is
     * folded accordingly); the default is the constant agnostic form, because that is the key the
     * join / aggregation read paths use and it makes a filter injection survive a constant change.
     */
    private static LiteralMode resolveLiteralMode(String modeName) throws AnalysisException {
        if (modeName == null) {
            return LiteralMode.NO_LITERAL;
        }
        for (LiteralMode mode : LiteralMode.values()) {
            if (mode.name().equalsIgnoreCase(modeName)) {
                return mode;
            }
        }
        throw new AnalysisException(
                "invalid hbo statistics LITERAL_MODE, expect WITH_LITERAL or NO_LITERAL: " + modeName);
    }

    /** The canonical the entry is keyed by: the pasted struct, folded for the agnostic mode. */
    private static String canonicalFor(String structCanonical, LiteralMode literalMode) {
        return literalMode == LiteralMode.NO_LITERAL
                ? GroupStructInfo.toNoLiteral(structCanonical) : structCanonical;
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
        return (long) value;
    }

    /** The row count, or the fan-out factor of a JOIN_EXPANSION entry. */
    public double getValue() {
        return value;
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
    private String validateStructCanonical(String targetFingerprint, String structCanonical, PinnedType type,
            LiteralMode literalMode, boolean required) throws AnalysisException {
        if (structCanonical.isEmpty()) {
            if (required) {
                throw new AnalysisException("hbo statistics STRUCT is required,"
                        + " copy the struct= value of the target node from EXPLAIN");
            }
            return "";
        }
        // the type decides which struct info the entry can be keyed by at all
        if (type == PinnedType.JOIN_EXPANSION && !structCanonical.startsWith("JE{")) {
            throw new AnalysisException("TYPE=JOIN_EXPANSION is keyed by the join conditions,"
                    + " its STRUCT must be the condition canonical: STRUCT='JE{...}'");
        }
        if (type == PinnedType.FILTER_SMALL && !structCanonical.startsWith("F{")) {
            throw new AnalysisException("TYPE=FILTER_SMALL can only guard a filter entry,"
                    + " its STRUCT must be a filter root: STRUCT='F{...}(...)'");
        }
        if (type != PinnedType.JOIN_EXPANSION && literalMode == LiteralMode.WITH_LITERAL
                && (structCanonical.startsWith("J{") || structCanonical.startsWith("A{"))) {
            throw new AnalysisException("a join / aggregation entry has no literal carrying form"
                    + " (their canonical is always constant agnostic), use LITERAL_MODE=NO_LITERAL");
        }
        if (type == PinnedType.JOIN_EXPANSION && literalMode == LiteralMode.WITH_LITERAL) {
            throw new AnalysisException("a join expansion entry is keyed by the constant agnostic"
                    + " condition canonical, use LITERAL_MODE=NO_LITERAL");
        }
        String canonical = canonicalFor(structCanonical, literalMode);
        if (isFingerprintOf(targetFingerprint, canonical)) {
            return canonical;
        }
        throw new AnalysisException("hbo statistics STRUCT does not match the fingerprint " + targetFingerprint
                + " for LITERAL_MODE=" + literalMode + ", copy the struct= value of the target node from EXPLAIN");
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
