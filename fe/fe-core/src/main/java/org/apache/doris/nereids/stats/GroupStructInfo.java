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

import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
 *   <li>for each scan: table qualifier, pruned partition count and the table visible version (so
 *       that data changes invalidate the fingerprint instead of reusing a stale row count);</li>
 *   <li>for filter / join / aggregate: the normalized predicates / join conditions / grouping keys,
 *       i.e. the structural pattern only (slot and expression ids are stripped by
 *       {@link #normalizeExpression}).</li>
 * </ul>
 * The string contains no per-query identifier, so it is reproducible across runs and queries for
 * structurally identical sub trees, and its sha256 is used as the HBO cache key (fingerprint).
 *
 * <p>The descriptor is derived from the group's logical expression and its child groups (memo level
 * traversal, with a visited set to guard shared sub graphs such as CTE). Groups whose content is not
 * supported (e.g. contains TVF / CTE consumer, or the head operator is not one of the supported
 * kinds) are {@link #isValid() invalid} and callers must fall back to the legacy behavior.
 */
public class GroupStructInfo {
    /** Shared invalid instance. */
    public static final GroupStructInfo INVALID = new GroupStructInfo(false, "", "");

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
    private final String fingerprint;

    private GroupStructInfo(boolean valid, String canonicalString, String fingerprint) {
        this.valid = valid;
        this.canonicalString = canonicalString;
        this.fingerprint = fingerprint;
    }

    public boolean isValid() {
        return valid;
    }

    public String getCanonicalString() {
        return canonicalString;
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
        try {
            Ctx ctx = new Ctx(mode);
            StringBuilder sb = new StringBuilder();
            visit(group, sb, ctx);
            if (!ctx.valid) {
                return INVALID;
            }
            String canonicalString = sb.toString();
            String fingerprint = Hashing.sha256()
                    .hashString(canonicalString, StandardCharsets.UTF_8).toString();
            return new GroupStructInfo(true, canonicalString, fingerprint);
        } catch (RuntimeException e) {
            // memo content not supported by the simplified struct info: treat as invalid and
            // fall back to legacy behavior instead of failing the optimizer on the hot path
            LOG.debug("failed to compute hbo struct info for group {}", group.getGroupId(), e);
            return INVALID;
        }
    }

    /**
     * Visit a group and append its canonical description to {@code sb}.
     */
    private static void visit(Group group, StringBuilder sb, Ctx ctx) {
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
        appendPlan(ge.getPlan(), ge, sb, ctx);
    }

    private static void appendPlan(Plan plan, GroupExpression ge, StringBuilder sb, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        if (plan instanceof LogicalOlapScan) {
            appendScan((LogicalOlapScan) plan, sb, ctx);
        } else if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            sb.append("F{").append(normalizedSorted(filter.getConjuncts(), ctx.mode)).append("}(");
            appendChild(ge, 0, sb, ctx);
            sb.append(")");
        } else if (plan instanceof LogicalProject) {
            // project is transparent for structure matching
            if (ge.arity() == 1) {
                appendChild(ge, 0, sb, ctx);
            } else {
                invalid(ctx);
            }
        } else if (plan instanceof LogicalJoin) {
            appendJoin((LogicalJoin<?, ?>) plan, ge, sb, ctx);
        } else if (plan instanceof LogicalAggregate) {
            appendAggregate((LogicalAggregate<?>) plan, ge, sb, ctx);
        } else {
            // unsupported head operator (sort/topn/limit/window/union/cte/tvf/...)
            invalid(ctx);
        }
    }

    private static void appendJoin(LogicalJoin<?, ?> join, GroupExpression ge, StringBuilder sb, Ctx ctx) {
        if (isFlattenableJoinType(join.getJoinType())) {
            appendJoinChain(join, ge, sb, ctx);
            return;
        }
        // Outer / semi / anti / asof joins: the two sides are semantically different and the join
        // cannot be reassociated, so the memo order of the children and the join type are kept.
        sb.append("J{").append(join.getJoinType());
        sb.append(",c:[").append(normalizedJoinConditions(join, ctx.mode)).append("]}(");
        appendChild(ge, 0, sb, ctx);
        sb.append(SEP);
        appendChild(ge, 1, sb, ctx);
        sb.append(")");
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
    private static void appendJoinChain(LogicalJoin<?, ?> join, GroupExpression ge, StringBuilder sb, Ctx ctx) {
        TreeSet<String> conditions = new TreeSet<>();
        List<String> leaves = new ArrayList<>();
        collectJoinConditions(join, conditions, ctx.mode);
        for (int i = 0; i < ge.arity(); i++) {
            collectJoinChain(ge.child(i), conditions, leaves, ctx);
        }
        if (!ctx.valid) {
            return;
        }
        leaves.sort(String::compareTo);
        sb.append("J{inner,c:[").append(String.join(SEP, conditions)).append("]}(");
        sb.append(String.join(SEP, leaves)).append(")");
    }

    /**
     * Collect one member of an inner / cross join chain: either descend into another join of the
     * chain (associativity), or append the canonical form of the whole sub tree as one chain leaf.
     * The group of {@code ge} is already marked visited by the caller.
     */
    private static void collectJoinChain(Group group, TreeSet<String> conditions, List<String> leaves, Ctx ctx) {
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
        StringBuilder leaf = new StringBuilder();
        appendPlan(plan, ge, leaf, ctx);
        if (ctx.valid) {
            leaves.add(leaf.toString());
        }
    }

    /** Inner and cross joins may be reordered, reassociated and therefore flattened. */
    private static boolean isFlattenableJoinType(JoinType joinType) {
        return joinType == JoinType.INNER_JOIN || joinType == JoinType.CROSS_JOIN;
    }

    private static void appendAggregate(LogicalAggregate<?> agg, GroupExpression ge, StringBuilder sb, Ctx ctx) {
        // Only the grouping keys and the child take part: the output row count of an aggregation is
        // the number of groups, which the aggregate functions and the output expressions cannot
        // change, so keeping them would only split one plan pattern into several cache entries.
        sb.append("A{gb:").append(normalizedSorted(agg.getGroupByExpressions(), ctx.mode));
        sb.append("}(");
        appendChild(ge, 0, sb, ctx);
        sb.append(")");
    }

    private static void invalid(Ctx ctx) {
        ctx.valid = false;
    }

    private static void appendScan(LogicalOlapScan scan, StringBuilder sb, Ctx ctx) {
        try {
            String fullName = scan.getTable().getNameWithFullQualifiers();
            String partitions = "";
            int partitionCount = scan.getTable().getPartitionNames().size();
            if (scan.getSelectedPartitionIds().size() != partitionCount) {
                partitions = ",p" + scan.getSelectedPartitionIds().size() + "/" + partitionCount;
            }
            long version = scan.getTable().getVisibleVersion();
            // No occurrence ordinal: an occurrence is identified by the aliases used in the
            // conditions of the enclosing operators, and two occurrences with the same table,
            // partition selection and version are interchangeable.
            sb.append("S{").append(fullName).append(partitions).append(",v").append(version).append("}");
        } catch (org.apache.doris.rpc.RpcException e) {
            // table version may not be available (e.g. cloud rpc failure): mark invalid and fall back
            LOG.debug("failed to get visible version for scan {}", scan.getTable().getNameWithFullQualifiers(), e);
            invalid(ctx);
        }
    }

    private static void appendChild(GroupExpression ge, int index, StringBuilder sb, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        visit(ge.child(index), sb, ctx);
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
     * instead of by a regular expression.
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
    private static class Ctx {
        private final LiteralMode mode;
        private boolean valid = true;
        private final Set<Group> visited = new HashSet<>();

        Ctx(LiteralMode mode) {
            this.mode = mode;
        }
    }
}
