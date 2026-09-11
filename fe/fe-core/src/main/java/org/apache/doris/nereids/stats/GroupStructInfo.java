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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
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
import java.util.HashMap;
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
 * <p>Unlike the MV oriented {@code rules.exploration.mv.StructInfo}, this struct info only keeps
 * the minimum information needed for HBO plan-subtree matching:
 * <ul>
 *   <li>the kind of the group head operator (scan / filter-on-scan / join / aggregate);</li>
 *   <li>for each scan: table qualifier, occurrence ordinal, pruned partition count and the table
 *       visible version (so that data changes invalidate the fingerprint);</li>
 *   <li>for filter / join / aggregate: normalized predicates / join conditions / agg keys, i.e.
 *       the structural pattern only.</li>
 * </ul>
 * No shuttle maps, no expression lineage, no per-query ids (slot/expr ids are stripped by
 * {@link #normalizeExpression}), no plan object references are produced: the canonical string is
 * reproducible across runs for structurally identical sub trees, and its sha256 is used as the
 * HBO cache key (fingerprint).
 *
 * <p>The descriptor is derived from the group's logical expression and its child groups (memo
 * level traversal, with a visited set to guard shared sub graphs such as CTE). Groups whose
 * content is not supported (e.g. contains TVF / CTE consumer, or the head operator is not one of
 * the supported kinds) are {@link #isValid() invalid} and callers must fall back to the legacy
 * behavior.
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
            String minToken = visit(group, sb, ctx);
            if (!ctx.valid || minToken == null) {
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
     *
     * @return the smallest scan-leaf token of this subtree, or null if the subtree contains no
     *         supported scan (in which case the whole struct info is invalid).
     */
    private static String visit(Group group, StringBuilder sb, Ctx ctx) {
        if (!ctx.valid) {
            return null;
        }
        if (!ctx.visited.add(group)) {
            // shared sub graph (e.g. CTE / repeated child group): cannot be expressed by a single
            // canonical tree, mark invalid so that callers fall back to legacy behavior
            return invalid(ctx);
        }
        // Use the first logical expression: memo may merge logically equivalent expressions
        // (e.g. commuted inner joins) into one group; equivalents share the same canonical
        // structure, so picking the first one is deterministic enough for the hbo fingerprint.
        GroupExpression ge = group.getFirstLogicalExpression();
        if (ge == null) {
            ctx.valid = false;
            return null;
        }
        Plan plan = ge.getPlan();
        if (plan instanceof LogicalOlapScan) {
            return appendScan((LogicalOlapScan) plan, sb, ctx);
        } else if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            sb.append("F{").append(normalizedSorted(filter.getConjuncts(), ctx.mode)).append("}(");
            String minToken = visitChild(ge, 0, sb, ctx);
            sb.append(")");
            return minToken;
        } else if (plan instanceof LogicalProject) {
            // project is transparent for structure matching
            return ge.arity() == 1 ? visitChild(ge, 0, sb, ctx) : invalid(ctx);
        } else if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            sb.append("J{").append(join.getJoinType());
            sb.append(",h:").append(normalizedSorted(join.getHashJoinConjuncts(), ctx.mode));
            sb.append(",o:").append(normalizedSorted(join.getOtherJoinConjuncts(), ctx.mode));
            sb.append("}(");
            // only commutative joins may reorder their inputs; for outer/semi/anti joins the
            // left/right order is semantically significant and the memo order is kept
            boolean sortChildren = join.getJoinType() == JoinType.INNER_JOIN
                    || join.getJoinType() == JoinType.CROSS_JOIN;
            String minToken = appendSortedChildren(ge, sb, ctx, sortChildren);
            sb.append(")");
            return minToken;
        } else if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> agg = (LogicalAggregate<?>) plan;
            sb.append("A{gb:").append(normalizedSorted(agg.getGroupByExpressions(), ctx.mode));
            sb.append(",fn:").append(normalizedSortedAggFunctions(agg.getOutputExpressions(), ctx.mode));
            sb.append("}(");
            String minToken = visitChild(ge, 0, sb, ctx);
            sb.append(")");
            return minToken;
        } else {
            // unsupported head operator (sort/topn/limit/window/union/cte/tvf/...)
            return invalid(ctx);
        }
    }

    private static String invalid(Ctx ctx) {
        ctx.valid = false;
        return null;
    }

    private static String appendScan(LogicalOlapScan scan, StringBuilder sb, Ctx ctx) {
        try {
            String fullName = scan.getTable().getNameWithFullQualifiers();
            int ordinal = ctx.occurrenceCount.computeIfAbsent(fullName, k -> new int[1])[0]++;
            String partitions = "";
            int partitionCount = scan.getTable().getPartitionNames().size();
            if (scan.getSelectedPartitionIds().size() != partitionCount) {
                partitions = ",p" + scan.getSelectedPartitionIds().size() + "/" + partitionCount;
            }
            long version = scan.getTable().getVisibleVersion();
            String token = "S{" + fullName + "#" + ordinal + partitions + ",v" + version + "}";
            sb.append(token);
            return token;
        } catch (org.apache.doris.rpc.RpcException e) {
            // table version may not be available (e.g. cloud rpc failure): mark invalid and fall back
            LOG.debug("failed to get visible version for scan {}", scan.getTable().getNameWithFullQualifiers(), e);
            return invalid(ctx);
        }
    }

    /**
     * Visit child groups; when {@code sortChildren} is true (commutative joins) the children are
     * emitted in a canonical (sorted by min leaf token) order so that join sides are
     * interchangeable; otherwise the memo order is kept (semantically significant for outer/
     * semi/anti joins and for deterministic cross-run reproducibility).
     */
    private static String appendSortedChildren(GroupExpression ge, StringBuilder sb, Ctx ctx,
            boolean sortChildren) {
        List<String[]> children = new ArrayList<>();
        for (int i = 0; i < ge.arity(); i++) {
            StringBuilder childSb = new StringBuilder();
            String minToken = visitChild(ge, i, childSb, ctx);
            if (!ctx.valid) {
                return null;
            }
            children.add(new String[] {minToken, childSb.toString()});
        }
        if (sortChildren) {
            children.sort((a, b) -> {
                int c = a[0].compareTo(b[0]);
                return c != 0 ? c : a[1].compareTo(b[1]);
            });
        }
        boolean first = true;
        String minToken = null;
        for (String[] child : children) {
            if (!first) {
                sb.append(SEP);
            }
            sb.append(child[1]);
            first = false;
            if (minToken == null || child[0].compareTo(minToken) < 0) {
                minToken = child[0];
            }
        }
        return minToken;
    }

    private static String visitChild(GroupExpression ge, int index, StringBuilder sb, Ctx ctx) {
        if (!ctx.valid) {
            return null;
        }
        StringBuilder childSb = new StringBuilder();
        String minToken = visit(ge.child(index), childSb, ctx);
        if (!ctx.valid) {
            return null;
        }
        if (minToken == null) {
            // group subtree contains no supported scan (e.g. empty/const relation)
            return invalid(ctx);
        }
        sb.append(childSb);
        return minToken;
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

    private static String normalizedSortedAggFunctions(List<? extends Expression> outputs, LiteralMode mode) {
        TreeSet<String> fnSet = new TreeSet<>();
        for (Expression output : outputs) {
            Expression inner = output;
            if (inner.children().size() == 1) {
                // unwrap alias / single-child wrappers so that aggregate functions are visible
                inner = inner.children().get(0);
            }
            if (inner instanceof AggregateFunction) {
                AggregateFunction fn = (AggregateFunction) inner;
                // function arguments keep their original order: argument lists are not freely
                // commutable (e.g. percentile_approx(col, ratio)), sorting them would collapse
                // distinct signatures into one descriptor (review round3 Major). DISTINCT is a
                // semantic modifier on the function and must be part of the signature too.
                String args = fn.children().stream().map(e -> normalizeExpression(e, mode))
                        .collect(Collectors.joining(","));
                fnSet.add((fn.isDistinct() ? "distinct " : "")
                        + fn.getClass().getSimpleName() + "(" + args + ")");
            }
        }
        return String.join(SEP, fnSet);
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

    /** Traversal state; shared along the whole subtree so occurrence ordinals are deterministic. */
    private static class Ctx {
        private final LiteralMode mode;
        private boolean valid = true;
        private final Set<Group> visited = new HashSet<>();
        private final Map<String, int[]> occurrenceCount = new HashMap<>();

        Ctx(LiteralMode mode) {
            this.mode = mode;
        }
    }
}
