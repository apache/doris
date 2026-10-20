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
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.LiteralMode;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.PinnedType;
import org.apache.doris.nereids.stats.HboScanDescriptor;
import org.apache.doris.nereids.stats.HboStatisticsStore;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Manual HBO statistics management statements:
 * <pre>
 *   HBO SET [PINNED|LEARNED] STATISTICS VALUE=&lt;rows&gt; [TYPE=&lt;type&gt;] [STRUCT='&lt;canonical&gt;']
 *       FINGERPRINT='&lt;fingerprint&gt;' [LITERAL_MODE=&lt;mode&gt;]
 *   HBO DELETE [PINNED|LEARNED] STATISTICS FINGERPRINT='&lt;fingerprint&gt;'
 * </pre>
 * The optional scope selects which cache is written: {@code PINNED} (default) injects an entry that
 * is authoritative over the learned entries, {@code LEARNED} injects into the learned cache so the
 * learned lookup path can be exercised without a real profile publish. The injected value is bound
 * to the fingerprint the user copied from EXPLAIN; which struct info that fingerprint belongs to is
 * decided here (an explicit {@code LITERAL_MODE} wins, the default is the constant agnostic form
 * every join / aggregation key uses) and persisted with the entry.
 *
 * <p>The struct info is not just a label: it carries the data state of every scan it reads
 * ({@code S{db.t,v3,r1000,p1/5}}, see {@link org.apache.doris.nereids.stats.HboScanDescriptor}),
 * which the read side compares with
 * the data state of the query before applying the entry
 * (see {@link org.apache.doris.nereids.stats.HboStructFreshness}). Copying
 * the {@code struct=} value of EXPLAIN therefore also records what the injected value was measured
 * on.
 *
 * <p>A full statement carries two things which the same description of the node produces. The
 * <b>fingerprint</b> is the {@code sha256} of the shape the FE computes from the plan while it
 * optimizes; it is the key an entry is looked up by. The <b>canonical string</b> is the text EXPLAIN
 * prints next to that fingerprint, and it is what a user copies into {@code STRUCT}. It is a rendering
 * of the description, not the input of the fingerprint: when this statement runs there is no plan for
 * the named fingerprint and no way to build one (a fingerprint is a one way hash, not an index), so
 * only the user can bring the description along.
 *
 * <p>The two lines EXPLAIN prints for one node are two different keys. The constant agnostic line is
 * the one the default mode accepts; the literal carrying line is accepted with
 * {@code LITERAL_MODE=WITH_LITERAL} and is <em>not</em> always the fold of the other one, because the
 * conditions of a node are ordered (and a join de-duplicates them) independently per mode: a node with
 * conditions which fold alike has a literal carrying text which folds into something other than its own
 * constant agnostic text, so the line to copy is the one whose mode is passed.
 *
 * <p>A pinned entry requires it, for three reasons. The type of the entry is validated against the
 * description (a {@code TYPE=JOIN_EXPANSION} entry must name a {@code JE{...}} condition canonical and a
 * {@code TYPE=FILTER_SMALL} entry a filter root). The scan tokens of the description are the data state
 * the read side compares with the query before applying the entry (see
 * {@link org.apache.doris.nereids.stats.HboStructFreshness}). And the relations those tokens name are
 * the key of the pinned lookup index - an entry without relations would make the pre filter of every
 * group a no-op instead of being filtered by the tables it reads.
 *
 * <p>The learned scope accepts a missing {@code STRUCT}: such an entry records no data state, so it is
 * applied without a freshness comparison and its state is reported as {@code unknown}. That is
 * acceptable for a measurement, which a later publish overwrites, and it is why the pinned scope - whose
 * entries are authoritative over the measurements - does not allow it.
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
    // the VALUE token as written, null when the value did not come from a statement: a row count has to be
    // an exact integer, which can only be checked on the token, and only once the type is known
    private final String valueText;
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
        this(op, scopeName, fingerprint, value, null, typeName, structCanonical, literalModeName);
    }

    /**
     * HboStatisticsCommand
     * @param op SET or DELETE
     * @param scopeName optional PINNED / LEARNED, null or empty means PINNED
     * @param fingerprint hbo fingerprint (sha256 of the simplified group struct info)
     * @param value injected output row count, or the fan-out factor for JOIN_EXPANSION
     * @param valueText the VALUE token as written, null for a programmatically constructed value
     * @param typeName optional EXACT / FILTER_SMALL, null means EXACT
     * @param structCanonical optional human-readable simplified struct info canonical string
     * @param literalModeName optional WITH_LITERAL / NO_LITERAL, null means the constant agnostic
     *                        (NO_LITERAL) form, which is also what the struct is folded to
     */
    public HboStatisticsCommand(Op op, String scopeName, String fingerprint, double value, String valueText,
            String typeName, String structCanonical, String literalModeName) {
        super(PlanType.HBO_STATISTICS_COMMAND);
        this.op = op;
        this.scope = parseScope(scopeName);
        // sha256 hex fingerprints are lowercase everywhere (group struct info, read-side lookup);
        // normalize user input so an uppercase fingerprint cannot silently miss its entry
        this.fingerprint = fingerprint == null ? null : fingerprint.toLowerCase(Locale.ROOT);
        this.value = value;
        this.valueText = valueText;
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
            // a learned entry has no key of its own (it is looked up by the fingerprint the read side
            // computes), so its struct info is a label only: it is validated as pasted instead of
            // being folded to the constant agnostic form
            LiteralMode literalMode = scope == Scope.LEARNED ? LiteralMode.WITH_LITERAL
                    : resolveLiteralMode(literalModeName);
            // a pinned entry is only displayable when it carries its struct info, and a struct info
            // which does not belong to the fingerprint would make the SHOW output misleading
            String canonical = validateStructCanonical(fingerprint, structCanonical, type, literalMode,
                    scope != Scope.LEARNED, scope != Scope.LEARNED);
            if (!HboStatisticsStore.fitsStructColumn(canonical)) {
                throw new AnalysisException("hbo statistics STRUCT is too long to be persisted without"
                        + " truncation (the scan baseline would be cut off), shorten the node it was"
                        + " copied from");
            }
            if (type == PinnedType.JOIN_EXPANSION) {
                // a fan-out factor relative to the larger input of the join: >= 1 means the join
                // expands, < 1 means it filters (0.1 keeps 10% of that input), so only zero and
                // negative values are meaningless
                // `!(value > 0)` also rejects NaN, and a value which overflows a double parses to
                // positive infinity: without the finiteness check such a factor would be accepted and
                // the store would then try to persist `Infinity` as a SQL number
                if (!(value > 0) || !Double.isFinite(value)) {
                    throw new AnalysisException(
                            "hbo join expansion must be a finite number greater than 0: " + value);
                }
            } else {
                // the VALUE of every other type is a row count: an integer token has to be exact, because a
                // double rounds a value above 2^53 before it can be stored. The check needs the token and
                // the type - a fan-out factor above that bound is legitimate, which is why the parser can
                // not decide it - so it happens here
                if (valueText != null) {
                    // a row count has to be an exact integer: a double rounds a value above 2^53, and the
                    // token is the only place where the number the user wrote is still exact - a decimal
                    // token (`9007199254740993.0`) has to reach the same check as an integer one
                    long exact;
                    try {
                        exact = new BigDecimal(valueText).longValueExact();
                    } catch (ArithmeticException | NumberFormatException e) {
                        throw new AnalysisException("hbo statistics VALUE is not an exact row count: " + valueText
                                + " (the largest exact value is " + (1L << 53) + ")");
                    }
                    if (!Double.isFinite(value) || (long) value != exact) {
                        throw new AnalysisException("hbo statistics VALUE is not an exact row count: " + valueText
                                + " (the largest exact value is " + (1L << 53) + ")");
                    }
                }
                // a double above Long.MAX_VALUE would not be rejected by the integer check but would
                // silently saturate when it is cast (and persisted) as a long
                if (value < 0 || value != Math.floor(value) || value >= (double) Long.MAX_VALUE) {
                    throw new AnalysisException("hbo statistics VALUE must be a non-negative integer"
                            + " no greater than " + Long.MAX_VALUE + " for type " + type + ": " + value);
                }
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
    private static String canonicalFor(String structCanonical, LiteralMode literalMode)
            throws AnalysisException {
        if (literalMode != LiteralMode.NO_LITERAL) {
            return structCanonical;
        }
        try {
            return GroupStructInfo.toNoLiteral(structCanonical);
        } catch (IllegalArgumentException e) {
            // report the text which cannot be folded instead of a fingerprint mismatch it would cause
            throw new AnalysisException("hbo statistics STRUCT is not well formed: " + e.getMessage());
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
     * @param keyedEntry whether the struct info is the key of the entry. A pinned entry is looked up
     *                   by the sha256 of what the user pasted, so the struct info has to be a form
     *                   the read side can produce; the struct info of a learned entry is a label
     *                   only (its key is generated internally), so it is accepted as it is - which
     *                   is also why a learned join / aggregation entry may carry either of the two
     *                   forms the annotation prints for them.
     */
    private String validateStructCanonical(String targetFingerprint, String structCanonical, PinnedType type,
            LiteralMode literalMode, boolean required, boolean keyedEntry) throws AnalysisException {
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
        if (keyedEntry && type != PinnedType.JOIN_EXPANSION && structCanonical.startsWith("S{")) {
            // pinned row counts are applied at filter, join and aggregation nodes only, so a scan
            // root can never be looked up - report it instead of storing an entry which silently
            // does nothing
            throw new AnalysisException("a scan entry can not be pinned: pinned row counts are"
                    + " applied at filter, join and aggregation nodes, copy the struct= value of"
                    + " such a node from EXPLAIN");
        }
        if (keyedEntry && type != PinnedType.JOIN_EXPANSION && structCanonical.startsWith("JE{")) {
            // the row count of a join is pinned by the struct info of its group (J{...}); a
            // condition canonical is only read by the expansion path, so such an entry could never
            // be applied - report it instead of storing an entry which silently does nothing
            throw new AnalysisException("a join row count entry is keyed by the group struct info,"
                    + " its STRUCT must be the group canonical: STRUCT='J{...}(...)'. A JE{...}"
                    + " condition canonical is only read by TYPE=JOIN_EXPANSION");
        }
        if (keyedEntry && type != PinnedType.JOIN_EXPANSION
                && HboScanDescriptor.parseAll(canonicalFor(structCanonical, literalMode)).isEmpty()) {
            // the lookup index of the pinned entries is keyed by the relations of an entry, and an entry
            // without a scan at all can not be filtered by one: it would turn the pre filter into a no-op
            // for every group, so it is reported instead of stored
            throw new AnalysisException("a pinned row count entry must describe at least one scan:"
                    + " copy the struct= value of a node above a scan from EXPLAIN");
        }
        if (keyedEntry && type == PinnedType.JOIN_EXPANSION && literalMode == LiteralMode.WITH_LITERAL) {
            // the canonical of an expansion key carries its literals in both modes, so the mode is only a
            // label here; EXPLAIN prints the line as the constant agnostic one and that label is required
            throw new AnalysisException("a join expansion entry is keyed by the condition canonical and"
                    + " is labelled like the line EXPLAIN prints, use LITERAL_MODE=NO_LITERAL");
        }
        // the key of a join expansion entry is the condition canonical itself, with its literals: the
        // expansion path compares exactly the text HboJoinConditions.canonicalOf renders (it renders every
        // residual with its literal, because `b > c + 1` and `b > c + 2` do not expand the same rows). The
        // text is therefore used verbatim and never folded - folding a literal the read side keeps would
        // make the sha256 differ from the fingerprint EXPLAIN printed next to the very same text, and the
        // statement would reject a pair the user copied correctly.
        // the fingerprint is the key and the struct is only its readable form: the entry is registered
        // under the pasted fingerprint, which the planner compares with the key of its own description
        // (a structural hash, see GroupStructInfo#getFingerprint), so the two are not derived from one
        // another anymore and a struct which does not hash to the fingerprint is not rejected. The struct
        // is still folded for the constant agnostic mode, because that is the form the entry is stored and
        // displayed with, not because its hash is compared.
        return type == PinnedType.JOIN_EXPANSION ? structCanonical : canonicalFor(structCanonical, literalMode);
    }

    private void validateFingerprint(String targetFingerprint) throws AnalysisException {
        if (targetFingerprint == null || !FINGERPRINT_PATTERN.matcher(targetFingerprint).matches()) {
            throw new AnalysisException("invalid hbo fingerprint, expect 64 hex chars: " + targetFingerprint);
        }
    }
}
