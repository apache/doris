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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.qe.ConnectContext;

import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Simplified struct info of a memo {@link Group}, used by History Based Optimization (HBO).
 *
 * <p>The struct info is a property of the <b>group</b>, not of one of its group expressions: all
 * expressions of a group are logically equivalent, so they describe the same set of relations with
 * the same predicates, and the canonical form below is deliberately insensitive to which expression
 * is picked.
 * <ul>
 *   <li>join commutativity: children of a join are canonicalized by sorting;</li>
 *   <li>join associativity: a whole inner / cross join chain is emitted as one node
 *       ({@code J{inner,c:[...]}(...;...)}) whose children are the chain leaves sorted by their
 *       canonical form and whose conditions are the sorted union of every condition of the chain,
 *       so {@code (A join B) join C} and {@code A join (B join C)} produce the same descriptor;</li>
 *   <li>aggregation: only the group by keys and the child participate, because the output row count
 *       of an aggregation is the number of groups and cannot be changed by the aggregate functions
 *       or by the output expressions;</li>
 *   <li>project is transparent for structure matching.</li>
 * </ul>
 * This mirrors what the MV oriented {@code rules.exploration.mv.StructInfo} does (it identifies a
 * sub tree by its relation set and by level independent - shuttled - predicates), with every MV
 * specific field dropped: no hyper graph, no shuttle map, no equivalence class, no lineage.
 *
 * <p>What the canonical form keeps:
 * <ul>
 *   <li>for each scan: the table qualifier and - as an annotation only - the data state of the scan
 *       (its visible version, the rows it reads and how many of the table partitions it selects,
 *       see {@link HboScanDescriptor});</li>
 *   <li>for filter / join / aggregate: the normalized predicates / join conditions / grouping keys,
 *       i.e. the structural pattern only (slot and expression ids are stripped by
 *       {@link #normalizeExpression}).</li>
 * </ul>
 * The string contains no per-query identifier, so it is reproducible across runs and queries for
 * structurally identical sub trees, and its sha256 is used as the HBO cache key (fingerprint).
 *
 * <p>The sha256 is taken over the canonical string <b>without</b> the scan baselines
 * ({@link #stripScanBaseline}): a hbo key identifies a plan pattern and the relation it reads, not a
 * data state, so an entry whose table simply keeps growing stays matchable instead of silently never
 * matching again. Whether an entry may still be applied is decided on the read side, by comparing
 * the baseline of the entry with the data state of the current query (see
 * {@link HboStructFreshness}).
 *
 * <p>The descriptor is derived from the group's logical expression and its child groups (memo level
 * traversal, with a visited set to guard shared sub graphs such as CTE). Groups whose content is not
 * supported (e.g. contains TVF / CTE consumer, or the head operator is not one of the supported
 * kinds) are {@link #isValid() invalid} and callers must fall back to the legacy behavior.
 */
public class GroupStructInfo {
    private static final List<HboScanDescriptor> INVALID_SCANS = Collections.emptyList();

    /** Shared invalid instance: the group content is not supported by the simplified struct info. */
    public static final GroupStructInfo INVALID = new GroupStructInfo(false, "", "", "", INVALID_SCANS);
    /**
     * Shared instance for a failure which is not a property of the plan: the table state could not be
     * read (e.g. the visible version of a cloud table could not be fetched). Unlike
     * {@link #INVALID} it is not cached by the memo (see {@code Group#getOrComputeHboStructInfo}), so
     * the next lookup of the same group retries instead of disabling hbo struct info for the whole
     * query.
     */
    public static final GroupStructInfo TRANSIENT_FAILURE =
            new GroupStructInfo(false, "", "", "", INVALID_SCANS);

    private static final String SEP = ";";
    private static final Logger LOG = LogManager.getLogger(GroupStructInfo.class);

    /**
     * Whether literal values participate in the canonical string / fingerprint.
     * <ul>
     *   <li>{@link #WITH_LITERAL} keeps the literal value ({@code lit(10:int)}): the fingerprint is
     *       bound to the exact constants, used for filter roots where the constant changes the
     *       output row count;</li>
     *   <li>{@link #NO_LITERAL} replaces every literal value with {@code *} ({@code lit(*)}): the
     *       fingerprint is constant agnostic, used for join / aggregate roots and for the
     *       constant-free filter shape.</li>
     * </ul>
     * The literal data type is intentionally not kept: nereids inserts an explicit {@code Cast}
     * for mismatching operand types, so two semantically different literal forms already differ by
     * the cast node, while equivalent forms (e.g. {@code d > 1} and {@code d > 1.0} on a decimal
     * column) are allowed to collapse.
     */
    public enum LiteralMode {
        WITH_LITERAL,
        NO_LITERAL
    }

    private final boolean valid;
    private final String canonicalString;
    /** The canonical string without the scan baselines: the fingerprint input. */
    private final String shapeString;
    private final String fingerprint;
    /** The scans of the sub tree, in the order their tokens appear in the canonical string. */
    private final List<HboScanDescriptor> scans;
    /** Why no struct info was built, when it was refused by a limit instead of being unsupported. */
    private final String skipReason;

    private GroupStructInfo(boolean valid, String canonicalString, String shapeString, String fingerprint,
            List<HboScanDescriptor> scans) {
        this(valid, canonicalString, shapeString, fingerprint, scans, "");
    }

    private GroupStructInfo(boolean valid, String canonicalString, String shapeString, String fingerprint,
            List<HboScanDescriptor> scans, String skipReason) {
        this.valid = valid;
        this.canonicalString = canonicalString;
        this.shapeString = shapeString;
        this.fingerprint = fingerprint;
        this.scans = scans;
        this.skipReason = skipReason;
    }

    /** The reason the struct info was refused by a limit, or empty. Only for diagnostics. */
    public String getSkipReason() {
        return skipReason;
    }

    public boolean isValid() {
        return valid;
    }

    public String getCanonicalString() {
        return canonicalString;
    }

    /**
     * The fingerprint input: the canonical string with the baseline of every scan token removed
     * ({@code S{db.t}}). It is built from the traversal itself and not by parsing the canonical
     * string, so no text inside a table name or a literal can be mistaken for a scan token.
     */
    public String getShapeString() {
        return shapeString;
    }

    /**
     * The row count and partition selection of every scan of the sub tree, in the order the scan
     * tokens appear in the canonical string (i.e. the order a user sees when pasting the struct
     * info). Empty when the struct info is invalid.
     */
    public List<HboScanDescriptor> getScans() {
        return scans;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    /**
     * Resolve the fingerprint of the memo group that {@code planNode} belongs to, when the node
     * still carries its {@link GroupExpression} back reference (memo inner plans, or plans from
     * {@code chooseBestPlan}). Empty when the group content is unsupported (invalid struct info).
     */
    public static Optional<String> fingerprintOfPlanNode(AbstractPlan planNode) {
        return fingerprintOfPlanNode(planNode, LiteralMode.WITH_LITERAL);
    }

    /**
     * Resolve the group fingerprint of a plan node in the given literal mode.
     */
    public static Optional<String> fingerprintOfPlanNode(AbstractPlan planNode, LiteralMode mode) {
        return fingerprintOfGroup(planNode.getGroupExpression()
                .map(GroupExpression::getOwnerGroup).orElse(null), mode);
    }

    /**
     * Resolve the fingerprint of the group that {@code planNode} belongs to, with a fallback to
     * the {@link MutableState#KEY_GROUP} group-id state that post processors propagate to their
     * rewritten copies (see {@code copyStatsAndGroupIdFrom}). {@code groupsById} maps memo group
     * id to group, and is looked up only when the back reference is absent.
     */
    public static Optional<String> fingerprintOfPlanNode(AbstractPlan planNode, Map<Integer, Group> groupsById) {
        return fingerprintOfPlanNode(planNode, groupsById, LiteralMode.WITH_LITERAL);
    }

    /**
     * Resolve the group fingerprint of a plan node in the given literal mode, with the
     * {@link MutableState#KEY_GROUP} fallback for post-processed copies.
     */
    public static Optional<String> fingerprintOfPlanNode(AbstractPlan planNode, Map<Integer, Group> groupsById,
            LiteralMode mode) {
        return structInfoOfPlanNode(planNode, groupsById, mode).map(GroupStructInfo::getFingerprint);
    }

    private static Optional<String> fingerprintOfGroup(Group group, LiteralMode mode) {
        return structInfoOfGroup(group, mode).map(GroupStructInfo::getFingerprint);
    }

    /**
     * Resolve the {@link GroupStructInfo} (with canonical string and fingerprint) of the group a
     * plan node belongs to, using the group-expression back reference or the KEY_GROUP group-id
     * state propagated by post processors (see {@link #fingerprintOfPlanNode}).
     */
    public static Optional<GroupStructInfo> structInfoOfPlanNode(AbstractPlan planNode,
            Map<Integer, Group> groupsById) {
        return structInfoOfPlanNode(planNode, groupsById, LiteralMode.WITH_LITERAL);
    }

    /**
     * Resolve the struct info of the group a plan node belongs to, in the given literal mode.
     */
    public static Optional<GroupStructInfo> structInfoOfPlanNode(AbstractPlan planNode,
            Map<Integer, Group> groupsById, LiteralMode mode) {
        Group group = planNode.getGroupExpression().map(GroupExpression::getOwnerGroup).orElse(null);
        if (group == null) {
            Optional<Object> groupState = planNode.getMutableState(MutableState.KEY_GROUP);
            if (groupState.isPresent() && groupsById != null) {
                try {
                    group = groupsById.get(Integer.valueOf(groupState.get().toString()));
                } catch (NumberFormatException ignored) {
                    group = null;
                }
            }
        }
        return structInfoOfGroup(group, mode);
    }

    private static Optional<GroupStructInfo> structInfoOfGroup(Group group, LiteralMode mode) {
        if (group == null) {
            return Optional.empty();
        }
        GroupStructInfo structInfo = group.getOrComputeHboStructInfo(mode);
        return structInfo.isValid() ? Optional.of(structInfo) : Optional.empty();
    }

    /**
     * The struct info of the group a plan node belongs to, computed now instead of being taken from
     * the per-group cache. The fingerprint does not depend on any data state, so the cached struct
     * info is the right source for a lookup; its <b>baseline</b> (the visible version and the row
     * counts of its scans) is however a snapshot of the moment it was computed, which may be well
     * before the query is planned. Whoever displays that baseline (EXPLAIN prints it as the
     * {@code struct=} a user pastes into {@code HBO SET STATISTICS}) or compares it (the drift
     * verdict) must therefore ask for it here, so that the whole query reports one data state.
     */
    public static Optional<GroupStructInfo> dataStateOfPlanNode(AbstractPlan planNode,
            Map<Integer, Group> groupsById, LiteralMode mode) {
        Group group = planNode.getGroupExpression().map(GroupExpression::getOwnerGroup).orElse(null);
        if (group == null) {
            Optional<Object> groupState = planNode.getMutableState(MutableState.KEY_GROUP);
            if (groupState.isPresent() && groupsById != null) {
                try {
                    group = groupsById.get(Integer.valueOf(groupState.get().toString()));
                } catch (NumberFormatException ignored) {
                    group = null;
                }
            }
        }
        if (group == null) {
            return Optional.empty();
        }
        GroupStructInfo structInfo = of(group, mode);
        return structInfo.isValid() ? Optional.of(structInfo) : Optional.empty();
    }

    /**
     * Compute the simplified struct info (and its fingerprint) of a memo group, by traversing the
     * group's logical expression and its child groups.
     */
    public static GroupStructInfo of(Group group) {
        return of(group, LiteralMode.WITH_LITERAL);
    }

    /**
     * Compute the simplified struct info of a memo group in the given literal mode.
     */
    public static GroupStructInfo of(Group group, LiteralMode mode) {
        HboStructSummary summary = group.getOrComputeHboStructSummary();
        int maxScansPerGroup = hboMaxScansPerGroup();
        if (maxScansPerGroup > 0 && summary.getScanCount() > maxScansPerGroup) {
            // The canonical string of a group describes its whole sub tree, and for a chain of joins
            // it is built once per group of that chain, so a cheap structural summary is used as a
            // gate: a sub tree with more scan tokens than hbo_max_scans_per_group is not described at
            // all (the read side falls back to the optimizer estimation and EXPLAIN reports why).
            return new GroupStructInfo(false, "", "", "", INVALID_SCANS, "subtree scans="
                    + summary.getScanCount() + " > hbo_max_scans_per_group=" + maxScansPerGroup);
        }
        Ctx ctx = new Ctx(mode);
        try {
            Canonical out = new Canonical();
            visit(group, out, ctx);
            if (!ctx.valid) {
                return INVALID;
            }
            String canonicalString = out.annotated.toString();
            String shapeString = out.shape.toString();
            String fingerprint = Hashing.sha256()
                    .hashString(shapeString, StandardCharsets.UTF_8).toString();
            return new GroupStructInfo(true, canonicalString, shapeString, fingerprint,
                    new ArrayList<>(out.getScans()));
        } catch (RuntimeException e) {
            // memo content not supported by the simplified struct info: treat as invalid and
            // fall back to legacy behavior instead of failing the optimizer on the hot path
            LOG.debug("failed to compute hbo struct info for group {}", group.getGroupId(), e);
            return ctx.retryable ? TRANSIENT_FAILURE : INVALID;
        }
    }

    /**
     * Visit a group and append its canonical description to {@code sb}.
     */
    private static void visit(Group group, Canonical out, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        if (!ctx.visited.add(group)) {
            // shared sub graph (e.g. CTE / repeated child group): cannot be expressed by a single
            // canonical tree, mark invalid so that callers fall back to legacy behavior
            invalid(ctx);
            return;
        }
        // Use the first logical expression to know the kind of the group head. The canonical forms
        // below are insensitive to which expression is picked, so this choice cannot change the
        // fingerprint of the group (see the class comment).
        GroupExpression ge = group.getFirstLogicalExpression();
        if (ge == null) {
            invalid(ctx);
            return;
        }
        appendPlan(ge.getPlan(), ge, out, ctx);
    }

    private static void appendPlan(Plan plan, GroupExpression ge, Canonical out, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        if (plan instanceof LogicalOlapScan) {
            appendScan((LogicalOlapScan) plan, out, ctx);
        } else if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            out.append("F{").append(normalizedSorted(filter.getConjuncts(), ctx.mode)).append("}(");
            appendChild(ge, 0, out, ctx);
            out.append(")");
        } else if (plan instanceof LogicalProject) {
            // project is transparent for structure matching
            if (ge.arity() == 1) {
                appendChild(ge, 0, out, ctx);
            } else {
                invalid(ctx);
            }
        } else if (plan instanceof LogicalJoin) {
            appendJoin((LogicalJoin<?, ?>) plan, ge, out, ctx);
        } else if (plan instanceof LogicalAggregate) {
            appendAggregate((LogicalAggregate<?>) plan, ge, out, ctx);
        } else {
            // unsupported head operator (sort/topn/limit/window/union/cte/tvf/...)
            invalid(ctx);
        }
    }

    private static void appendJoin(LogicalJoin<?, ?> join, GroupExpression ge, Canonical out, Ctx ctx) {
        if (isFlattenableJoinType(join.getJoinType())) {
            appendJoinChain(join, ge, out, ctx);
            return;
        }
        // Outer / semi / anti / asof joins: the two sides are semantically different and the join
        // cannot be reassociated, so the memo order of the children and the join type are kept.
        out.append("J{").append(join.getJoinType().toString());
        out.append(",c:[").append(normalizedJoinConditions(join, ctx.mode)).append("]}(");
        appendChild(ge, 0, out, ctx);
        out.append(SEP);
        appendChild(ge, 1, out, ctx);
        out.append(")");
    }

    /**
     * Append the canonical form of a whole inner / cross join chain as a single node.
     *
     * <p>Inner and cross joins are associative and commutative, so the grouping of the chain is an
     * artifact of the explored expression and must not be part of the fingerprint: the chain is
     * emitted as {@code J{inner,c:[...]}(leaf;leaf;...)} with the conditions of every join of the
     * chain merged into one sorted set and the leaves (scans, filters, aggregations, non
     * flattenable joins) sorted by their canonical form. Every expression of a group therefore
     * produces the same descriptor. Cross joins are merged with inner joins because a cross join is
     * exactly an inner join without condition, and both have the same output row count.
     */
    private static void appendJoinChain(LogicalJoin<?, ?> join, GroupExpression ge, Canonical out, Ctx ctx) {
        TreeSet<String> conditions = new TreeSet<>();
        List<Leaf> leaves = new ArrayList<>();
        collectJoinConditions(join, conditions, ctx.mode);
        for (int i = 0; i < ge.arity(); i++) {
            collectJoinChain(ge.child(i), conditions, leaves, ctx);
        }
        if (!ctx.valid) {
            return;
        }
        // the leaves are ordered by their fingerprint input (their own shape, i.e. without the data
        // state of their scans): the order is part of the fingerprint, so it must not depend on a
        // data state which the fingerprint deliberately ignores
        leaves.sort(Comparator.comparing(Leaf::getShape));
        out.append("J{inner,c:[").append(String.join(SEP, conditions)).append("]}(");
        for (int i = 0; i < leaves.size(); i++) {
            if (i > 0) {
                out.append(SEP);
            }
            out.append(leaves.get(i).getCanonical());
        }
        out.append(")");
    }

    /**
     * Collect one member of an inner / cross join chain: either descend into another join of the
     * chain (associativity), or append the canonical form of the whole sub tree as one chain leaf.
     * The group of {@code ge} is already marked visited by the caller.
     */
    private static void collectJoinChain(Group group, TreeSet<String> conditions, List<Leaf> leaves, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        if (!ctx.visited.add(group)) {
            invalid(ctx);
            return;
        }
        GroupExpression ge = group.getFirstLogicalExpression();
        if (ge == null) {
            invalid(ctx);
            return;
        }
        Plan plan = ge.getPlan();
        if (plan instanceof LogicalJoin && isFlattenableJoinType(((LogicalJoin<?, ?>) plan).getJoinType())) {
            collectJoinConditions((LogicalJoin<?, ?>) plan, conditions, ctx.mode);
            for (int i = 0; i < ge.arity(); i++) {
                collectJoinChain(ge.child(i), conditions, leaves, ctx);
            }
            return;
        }
        if (plan instanceof LogicalProject && ge.arity() == 1) {
            // keep descending through projects so that a project between two joins cannot break the
            // chain in one expression and not in another
            collectJoinChain(ge.child(0), conditions, leaves, ctx);
            return;
        }
        Canonical leaf = new Canonical();
        appendPlan(plan, ge, leaf, ctx);
        if (ctx.valid) {
            leaves.add(new Leaf(leaf));
        }
    }

    /** One leaf of a flattened join chain: both of its canonical forms and its scans. */
    private static class Leaf {
        private final Canonical canonical;

        Leaf(Canonical canonical) {
            this.canonical = canonical;
        }

        Canonical getCanonical() {
            return canonical;
        }

        String getShape() {
            return canonical.shape.toString();
        }
    }

    /** Inner and cross joins may be reordered, reassociated and therefore flattened. */
    private static boolean isFlattenableJoinType(JoinType joinType) {
        return joinType == JoinType.INNER_JOIN || joinType == JoinType.CROSS_JOIN;
    }

    private static void appendAggregate(LogicalAggregate<?> agg, GroupExpression ge, Canonical out, Ctx ctx) {
        // Only the grouping keys and the child take part: the output row count of an aggregation is
        // the number of groups, which the aggregate functions and the output expressions cannot
        // change, so keeping them would only split one plan pattern into several cache entries.
        out.append("A{gb:").append(normalizedSorted(agg.getGroupByExpressions(), ctx.mode));
        out.append("}(");
        appendChild(ge, 0, out, ctx);
        out.append(")");
    }

    private static void invalid(Ctx ctx) {
        ctx.valid = false;
    }

    /** Mark the struct info invalid because the table state could not be read (retryable). */
    private static void transientFailure(Ctx ctx) {
        ctx.valid = false;
        ctx.retryable = true;
    }

    private static void appendScan(LogicalOlapScan scan, Canonical out, Ctx ctx) {
        try {
            // the scan token is the table plus the data state of the scan (baseline). Its shape form
            // is the table alone and is written by this very call, so the fingerprint input is never
            // derived by parsing the canonical string: no table name and no literal value can be
            // mistaken for a scan token.
            HboScanDescriptor descriptor = HboScanDescriptor.of(scan);
            if (descriptor.getTable().indexOf(',') >= 0 || descriptor.getTable().indexOf('}') >= 0) {
                // the token delimiters cannot appear in a table name: the printed struct info could
                // not be read back, and a pasted one could resolve to another table
                LOG.debug("scan table {} contains a struct info delimiter, hbo is disabled for it",
                        descriptor.getTable());
                invalid(ctx);
                return;
            }
            out.annotated.append("S{").append(descriptor.render()).append('}');
            out.shape.append("S{").append(descriptor.getTable()).append('}');
            out.getScans().add(descriptor);
        } catch (org.apache.doris.rpc.RpcException e) {
            // the table version may not be readable (e.g. a cloud rpc failure): the group stays
            // invalid for this lookup, but the failure is retried by the next one
            LOG.debug("failed to get visible version for scan {}", scan.getTable().getNameWithFullQualifiers(), e);
            transientFailure(ctx);
        }
    }

    /**
     * Remove the baseline of every scan token of a canonical string ({@code S{db.t,v3,r1000,p1/5}}
     * becomes {@code S{db.t}}), i.e. the data state of the scan - visible version, scanned rows and
     * pruned partition count.
     *
     * <p>The fingerprint of a plan and the sort key of a join chain leaf are built by the traversal
     * itself, which knows where every scan token starts and ends. This text based variant is only for
     * the struct info a user pasted into {@code HBO SET STATISTICS}, whose sha256 has to match the
     * fingerprint the user copied. A literal value which looks like a scan token (a string which
     * starts like one and contains a comma) cannot be told apart from one there, so such a struct
     * info is rejected: that statement cannot be used for that node, in either literal mode
     * (there is no workaround). That is a rejected statement, never a wrong key - the fingerprint of
     * a plan is not computed by this method. A table name which contains the delimiters is not
     * printed at all, because such a canonical form could not be read back (see the scan token in
     * {@code appendScan}).
     */
    public static String stripScanBaseline(String canonicalString) {
        StringBuilder sb = new StringBuilder(canonicalString.length());
        int index = 0;
        while (true) {
            int start = HboScanDescriptor.nextScanTokenStart(canonicalString, index);
            int end = start < 0 ? -1 : canonicalString.indexOf('}', start);
            if (end < 0) {
                sb.append(canonicalString, index, canonicalString.length());
                break;
            }
            String header = canonicalString.substring(start + 2, end);
            int comma = header.indexOf(',');
            sb.append(canonicalString, index, start).append("S{")
                    .append(comma < 0 ? header : header.substring(0, comma)).append('}');
            index = end + 1;
        }
        return sb.toString();
    }

    private static void appendChild(GroupExpression ge, int index, Canonical out, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        // the child is built on its own form, so that its scans are appended in the order its tokens
        // appear in the parent string
        Canonical child = new Canonical();
        visit(ge.child(index), child, ctx);
        out.append(child);
    }

    // -----------------------------------------------------------------------------------
    // expression normalization: strip slot/expr ids, keep qualifier + column + literal value
    // -----------------------------------------------------------------------------------

    private static String normalizedSorted(Set<Expression> conjuncts, LiteralMode mode) {
        return conjuncts.stream().map(e -> normalizeExpression(e, mode)).sorted()
                .collect(Collectors.joining(SEP));
    }

    private static String normalizedSorted(List<Expression> conjuncts, LiteralMode mode) {
        return conjuncts.stream().map(e -> normalizeExpression(e, mode)).sorted()
                .collect(Collectors.joining(SEP));
    }

    /**
     * Render all conditions of one join node (hash and other conjuncts) as one sorted, de
     * duplicated set: whether a given conjunct is classified as a hash conjunct or as an other
     * conjunct depends on the grouping of the chain, so that classification cannot be part of a
     * canonical form that has to be insensitive to the grouping.
     */
    private static String normalizedJoinConditions(LogicalJoin<?, ?> join, LiteralMode mode) {
        TreeSet<String> conditions = new TreeSet<>();
        collectJoinConditions(join, conditions, mode);
        return String.join(SEP, conditions);
    }

    private static void collectJoinConditions(LogicalJoin<?, ?> join, Set<String> conditions, LiteralMode mode) {
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            conditions.add(normalizeExpression(conjunct, mode));
        }
        for (Expression conjunct : join.getOtherJoinConjuncts()) {
            conditions.add(normalizeExpression(conjunct, mode));
        }
    }

    /**
     * Rewrite every literal of a canonical string into its constant agnostic form ({@code lit(*)}),
     * i.e. turn a WITH_LITERAL canonical string into the NO_LITERAL one. Used to accept a struct info
     * which a user copied from a filter node of EXPLAIN for the constant agnostic fingerprint of the
     * same node.
     *
     * <p>A literal value is not quoted in the canonical form and a data type may even contain
     * parentheses ({@code lit(abc:VARCHAR(10))}), so the closing parenthesis is found by counting
     * instead of by a regular expression; a value with balanced parentheses
     * ({@code lit(IL (north))}) is therefore folded correctly. Only an <b>unbalanced</b> parenthesis
     * in a value (e.g. {@code where city = 'IL)'}) makes the fold consume too much, and the pasted
     * struct info is then rejected by the fingerprint check of {@code HBO SET STATISTICS} with the
     * generic "does not match the fingerprint" message; there is no workaround, because the check
     * runs in both literal modes. The constant agnostic struct info printed by EXPLAIN is built by the
     * group traversal itself (see {@link #dataStateOfPlanNode}), so it has no such limitation.
     */
    public static String toNoLiteral(String canonicalString) {
        StringBuilder sb = new StringBuilder(canonicalString.length());
        int index = 0;
        while (index < canonicalString.length()) {
            int start = canonicalString.indexOf("lit(", index);
            if (start < 0) {
                sb.append(canonicalString, index, canonicalString.length());
                break;
            }
            sb.append(canonicalString, index, start).append("lit(*)");
            int depth = 0;
            int cursor = start + 3;
            while (cursor < canonicalString.length()) {
                char c = canonicalString.charAt(cursor);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        cursor++;
                        break;
                    }
                }
                cursor++;
            }
            index = cursor;
        }
        return sb.toString();
    }

    /**
     * Normalize a single expression into its canonical component (slot -&gt; {@code col(qualifier.name)},
     * literal -&gt; {@code lit(value:type)} / {@code lit(*)}, other nodes -&gt; {@code ClassName(children)}).
     * Shared with the hbo join-condition canonicalizer.
     */
    public static String normalizeExpression(Expression expression) {
        return normalizeExpression(expression, LiteralMode.WITH_LITERAL);
    }

    private static String normalizeExpression(Expression expression, LiteralMode mode) {
        if (expression instanceof SlotReference) {
            SlotReference slot = (SlotReference) expression;
            return "col(" + String.join(".", slot.getQualifier()) + "." + slot.getName() + ")";
        } else if (expression instanceof Literal) {
            if (mode == LiteralMode.NO_LITERAL) {
                return "lit(*)";
            }
            Literal literal = (Literal) expression;
            return "lit(" + literal.getValue() + ":" + literal.getDataType() + ")";
        } else if (expression.children().isEmpty()) {
            return expression.getClass().getSimpleName();
        } else {
            List<Expression> children = expression.children();
            List<String> normalizedChildren = children.stream()
                    .map(e -> normalizeExpression(e, mode)).collect(Collectors.toList());
            if (isOrderInsensitive(expression)) {
                // only order-insensitive operators (equal-to / and / or) may have their operands
                // sorted; ordered comparisons (e.g. '<') must keep the original order, otherwise
                // `col < 5` and `5 < col` would collapse to the same descriptor (review B2).
                normalizedChildren.sort(String::compareTo);
            }
            String childrenStr = String.join(",", normalizedChildren);
            return expression.getClass().getSimpleName() + "(" + childrenStr + ")";
        }
    }

    /** Equal-to, boolean and/or are commutative; everything else keeps its operand order. */
    private static boolean isOrderInsensitive(Expression expression) {
        return expression instanceof EqualTo
                || expression instanceof Or
                || expression instanceof And;
    }

    /** Traversal state; shared along the whole subtree so the visited set guards shared sub graphs. */
    /**
     * The default of {@code hbo_max_scans_per_group}, used when no session is available. 20 keeps
     * every TPC-DS shape observed in this repository (the largest single sub tree is query64's
     * 19 scan join group) while refusing trees which are pathologically wide.
     */
    public static final int DEFAULT_MAX_SCANS_PER_GROUP = 20;

    private static int hboMaxScansPerGroup() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null || connectContext.getSessionVariable() == null) {
            return DEFAULT_MAX_SCANS_PER_GROUP;
        }
        return connectContext.getSessionVariable().getHboMaxScansPerGroup();
    }

    /**
     * The structural summary of a memo group: how many scan tokens the struct info of its sub tree
     * would contain, and which tables it reads.
     *
     * <p>It is aggregated bottom up from the children's summaries - no string is rendered and no
     * catalog is read - and memoized per group (see {@code Group#getOrComputeHboStructSummary}), so
     * it is the cheap gate in front of the canonical string: a group which is too large for
     * {@code hbo_max_scans_per_group} never renders one, and a group whose relation key can not
     * match any entry does not render one either (see {@code HboPlanStatisticsManager}).
     */
    public static final class HboStructSummary {
        private final int scanCount;
        /** The tables of the sub tree, in traversal order and with duplicates (a self join twice). */
        private final List<String> relations;
        private final String relationKey;

        private HboStructSummary(int scanCount, List<String> relations) {
            this.scanCount = scanCount;
            this.relations = Collections.unmodifiableList(new ArrayList<>(relations));
            List<String> sorted = new ArrayList<>(relations);
            Collections.sort(sorted);
            this.relationKey = String.join(",", sorted);
        }

        /** The number of scan tokens in the sub tree (a self join counts the table twice). */
        public int getScanCount() {
            return scanCount;
        }

        /**
         * The tables of the sub tree as one order independent key (sorted, duplicates kept). A group
         * whose struct info could equal another one's necessarily shares this key, so it is a safe
         * necessary condition for an entry lookup; a group with no scan at all keys to "".
         */
        public String getRelationKey() {
            return relationKey;
        }

        List<String> getRelations() {
            return relations;
        }
    }

    /** The summary of a group, computed from the (memoized) summaries of its child groups. */
    public static HboStructSummary summaryOf(Group group) {
        GroupExpression ge = group.getFirstLogicalExpression();
        if (ge == null) {
            return new HboStructSummary(0, Collections.emptyList());
        }
        Plan plan = ge.getPlan();
        List<String> relations = new ArrayList<>();
        int scanCount = 0;
        if (plan instanceof LogicalOlapScan) {
            scanCount = 1;
            relations.add(((LogicalOlapScan) plan).getTable().getNameWithFullQualifiers());
        }
        for (int i = 0; i < ge.arity(); i++) {
            Group child = ge.child(i);
            if (child == null) {
                continue;
            }
            HboStructSummary childSummary = child.getOrComputeHboStructSummary();
            scanCount += childSummary.getScanCount();
            relations.addAll(childSummary.getRelations());
        }
        return new HboStructSummary(scanCount, relations);
    }

    /**
     * Why the struct info of the group of {@code planNode} was refused, if it was refused by a limit.
     * Cheap: only the memoized structural summary is read, no canonical string is built.
     */
    public static Optional<String> limitSkipReasonOfPlanNode(AbstractPlan planNode,
            Map<Integer, Group> groupsById) {
        Group group = resolveGroup(planNode, groupsById);
        if (group == null) {
            return Optional.empty();
        }
        int maxScansPerGroup = hboMaxScansPerGroup();
        int scans = group.getOrComputeHboStructSummary().getScanCount();
        if (maxScansPerGroup > 0 && scans > maxScansPerGroup) {
            return Optional.of("subtree scans=" + scans
                    + " > hbo_max_scans_per_group=" + maxScansPerGroup);
        }
        return Optional.empty();
    }

    /** The relation key of the group of a plan node, or null when that group is not reachable. */
    public static String relationKeyOfPlanNode(AbstractPlan planNode, Map<Integer, Group> groupsById) {
        Group group = resolveGroup(planNode, groupsById);
        return group == null ? null : group.getOrComputeHboStructSummary().getRelationKey();
    }

    /** The memo group a plan node belongs to: its back reference, or the post processed group id. */
    private static Group resolveGroup(AbstractPlan planNode, Map<Integer, Group> groupsById) {
        Group group = planNode.getGroupExpression().map(GroupExpression::getOwnerGroup).orElse(null);
        if (group != null) {
            return group;
        }
        Optional<Object> groupState = planNode.getMutableState(MutableState.KEY_GROUP);
        if (groupState.isPresent() && groupsById != null) {
            try {
                return groupsById.get(Integer.valueOf(groupState.get().toString()));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static class Ctx {
        private final LiteralMode mode;
        private boolean valid = true;
        /** whether the failure was an environment problem which a later lookup may survive */
        private boolean retryable = false;
        private final Set<Group> visited = new HashSet<>();

        Ctx(LiteralMode mode) {
            this.mode = mode;
        }
    }

    /**
     * The two forms of the description a traversal builds at the same time: the canonical string
     * (which carries the data state of every scan as an annotation) and its shape (which does not,
     * and whose sha256 is the fingerprint). Everything but a scan token is written to both.
     */
    private static class Canonical {
        private final StringBuilder annotated = new StringBuilder();
        private final StringBuilder shape = new StringBuilder();
        // the scans of this part, in the order their tokens appear in the two strings: a caller which
        // compares them with the scans of another struct info (see HboStructFreshness) has to see the
        // same order the printed struct info has, which is not always the traversal order (a join
        // chain prints its leaves sorted)
        private final List<HboScanDescriptor> scans = new ArrayList<>();

        Canonical append(String text) {
            annotated.append(text);
            shape.append(text);
            return this;
        }

        /** Append a part which differs between the two forms (a scan token). */
        void append(String annotatedPart, String shapePart) {
            annotated.append(annotatedPart);
            shape.append(shapePart);
        }

        /** Append another canonical form (a child sub tree or a join chain leaf). */
        void append(Canonical other) {
            annotated.append(other.annotated);
            shape.append(other.shape);
            scans.addAll(other.scans);
        }

        List<HboScanDescriptor> getScans() {
            return scans;
        }
    }
}
