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
    private static final String SEP = ";";

    /** Shared invalid instance. */
    public static final GroupStructInfo INVALID = new GroupStructInfo(false, "", "");

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
        return fingerprintOfGroup(planNode.getGroupExpression()
                .map(GroupExpression::getOwnerGroup).orElse(null));
    }

    /**
     * Resolve the fingerprint of the group that {@code planNode} belongs to, with a fallback to
     * the {@link MutableState#KEY_GROUP} group-id state that post processors propagate to their
     * rewritten copies (see {@code copyStatsAndGroupIdFrom}). {@code groupsById} maps memo group
     * id to group, and is looked up only when the back reference is absent.
     */
    public static Optional<String> fingerprintOfPlanNode(AbstractPlan planNode, Map<Integer, Group> groupsById) {
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
        return fingerprintOfGroup(group);
    }

    private static Optional<String> fingerprintOfGroup(Group group) {
        return structInfoOfGroup(group).map(GroupStructInfo::getFingerprint);
    }

    /**
     * Resolve the {@link GroupStructInfo} (with canonical string and fingerprint) of the group a
     * plan node belongs to, using the group-expression back reference or the KEY_GROUP group-id
     * state propagated by post processors (see {@link #fingerprintOfPlanNode}).
     */
    public static Optional<GroupStructInfo> structInfoOfPlanNode(AbstractPlan planNode, Map<Integer, Group> groupsById) {
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
        return structInfoOfGroup(group);
    }

    private static Optional<GroupStructInfo> structInfoOfGroup(Group group) {
        if (group == null) {
            return Optional.empty();
        }
        GroupStructInfo structInfo = group.getOrComputeHboStructInfo();
        return structInfo.isValid() ? Optional.of(structInfo) : Optional.empty();
    }

    /**
     * Compute the simplified struct info (and its fingerprint) of a memo group, by traversing the
     * group's logical expression and its child groups.
     */
    public static GroupStructInfo of(Group group) {
        Ctx ctx = new Ctx();
        StringBuilder sb = new StringBuilder();
        String minToken = visit(group, sb, ctx);
        if (!ctx.valid || minToken == null) {
            return INVALID;
        }
        String canonicalString = sb.toString();
        String fingerprint = Hashing.sha256()
                .hashString(canonicalString, StandardCharsets.UTF_8).toString();
        return new GroupStructInfo(true, canonicalString, fingerprint);
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
            sb.append("F{").append(normalizedSorted(filter.getConjuncts())).append("}(");
            String minToken = visitChild(ge, 0, sb, ctx);
            sb.append(")");
            return minToken;
        } else if (plan instanceof LogicalProject) {
            // project is transparent for structure matching
            return ge.arity() == 1 ? visitChild(ge, 0, sb, ctx) : invalid(ctx);
        } else if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            sb.append("J{").append(join.getJoinType());
            sb.append(",h:").append(normalizedSorted(join.getHashJoinConjuncts()));
            sb.append(",o:").append(normalizedSorted(join.getOtherJoinConjuncts()));
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
            sb.append("A{gb:").append(normalizedSorted(agg.getGroupByExpressions()));
            sb.append(",fn:").append(normalizedSortedAggFunctions(agg.getOutputExpressions()));
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
        } catch (Exception e) {
            // table version may not be available (e.g. cloud rpc failure): mark invalid and fall back
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

    private static String normalizedSorted(Set<Expression> conjuncts) {
        return conjuncts.stream().map(GroupStructInfo::normalizeExpression).sorted()
                .collect(Collectors.joining(SEP));
    }

    private static String normalizedSorted(List<Expression> conjuncts) {
        return conjuncts.stream().map(GroupStructInfo::normalizeExpression).sorted()
                .collect(Collectors.joining(SEP));
    }

    private static String normalizedSortedAggFunctions(List<? extends Expression> outputs) {
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
                // distinct signatures into one descriptor (review round3 Major)
                String args = fn.children().stream().map(GroupStructInfo::normalizeExpression)
                        .collect(Collectors.joining(","));
                fnSet.add(fn.getClass().getSimpleName() + "(" + args + ")");
            }
        }
        return String.join(SEP, fnSet);
    }

    private static String normalizeExpression(Expression expression) {
        if (expression instanceof SlotReference) {
            SlotReference slot = (SlotReference) expression;
            return "col(" + String.join(".", slot.getQualifier()) + "." + slot.getName() + ")";
        } else if (expression instanceof Literal) {
            Literal literal = (Literal) expression;
            return "lit(" + literal.getValue() + ":" + literal.getDataType() + ")";
        } else if (expression.children().isEmpty()) {
            return expression.getClass().getSimpleName();
        } else {
            List<Expression> children = expression.children();
            List<String> normalizedChildren = children.stream()
                    .map(GroupStructInfo::normalizeExpression).collect(Collectors.toList());
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
        private boolean valid = true;
        private final Set<Group> visited = new HashSet<>();
        private final Map<String, int[]> occurrenceCount = new HashMap<>();
    }
}
