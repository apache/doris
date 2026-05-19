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

package org.apache.doris.planner;

import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Predicate;

/**
 * Plan-shape DSL for asserting {@link PlanNode} tree structure in tests.
 * Inspired by Trino's {@code PlanMatchPattern}.
 *
 * <p>Use with {@code import static org.apache.doris.planner.PlanShape.*;} to
 * drop the {@code PlanShape.} prefix from call sites:
 * <pre>{@code
 * import static org.apache.doris.planner.PlanShape.*;
 * import static org.apache.doris.planner.LocalExchangeNode.LocalExchangeType.*;
 *
 * assertMatchesAnyFragment(planner.getFragments(),
 *     anyTree(
 *         agg(
 *             localExchange(LOCAL_EXECUTION_HASH_SHUFFLE,
 *                 olapScan("t1")))));
 * }</pre>
 *
 * <p>This reads bottom-up: somewhere in the plan tree there must be an
 * {@link AggregationNode} whose direct child is a
 * {@link LocalExchangeNode}(LOCAL_EXECUTION_HASH_SHUFFLE) whose direct child
 * is an {@link OlapScanNode} on table {@code t1}.
 *
 * <p>Add a predicate with the fluent {@link #where(Predicate)} method.
 * The lambda parameter is typed to the most specific {@link PlanNode}
 * subclass declared by the factory — no explicit cast is required:
 * <pre>{@code
 * agg(...).where(a -> a.isColocate())
 * }</pre>
 *
 * @param <T> the {@link PlanNode} subclass this shape matches against; lets
 *     {@link #where(Predicate)} infer the lambda parameter type automatically.
 */
public final class PlanShape<T extends PlanNode> {
    private final Class<T> nodeClass;
    private final Predicate<T> filter;
    private final List<PlanShape<?>> children;
    private final boolean anyTreeMatch;
    private final String label;

    private PlanShape(Class<T> nodeClass, Predicate<T> filter,
                      List<PlanShape<?>> children, boolean anyTreeMatch, String label) {
        this.nodeClass = nodeClass;
        this.filter = filter;
        this.children = children;
        this.anyTreeMatch = anyTreeMatch;
        this.label = label;
    }

    // ---- generic factories ----

    /** Match a node of the given class with the given direct children patterns. */
    public static <T extends PlanNode> PlanShape<T> node(Class<T> cls, PlanShape<?>... children) {
        return new PlanShape<>(cls, null, ImmutableList.copyOf(children), false, cls.getSimpleName());
    }

    /**
     * Skip any number of intermediate nodes; the {@code inner} pattern can match
     * anywhere in the subtree rooted at the current node (including the node itself).
     */
    public static PlanShape<PlanNode> anyTree(PlanShape<?> inner) {
        return new PlanShape<>(PlanNode.class, null, ImmutableList.of(inner), true, "anyTree");
    }

    /** Match any single node (regardless of class) that has the given children. */
    public static PlanShape<PlanNode> anyNode(PlanShape<?>... children) {
        return new PlanShape<>(PlanNode.class, null, ImmutableList.copyOf(children), false, "anyNode");
    }

    /**
     * Match a node that is NOT an instance of {@code excludedClass}, with the given
     * direct children patterns.  Useful for asserting that a particular node type is
     * absent at a specific position.
     *
     * <p>Typical use: assert that an LE was skipped where the framework should not
     * have inserted one.
     * <pre>{@code
     * agg(anyNot(LocalExchangeNode.class, olapScan("t1")))
     * }</pre>
     * reads as "AggregationNode whose direct child is NOT a LocalExchangeNode and
     * is itself an OlapScan('t1')".
     */
    public static PlanShape<PlanNode> anyNot(Class<? extends PlanNode> excludedClass,
                                              PlanShape<?>... children) {
        return new PlanShape<>(PlanNode.class,
                n -> !excludedClass.isInstance(n),
                ImmutableList.copyOf(children), false,
                "anyNot(" + excludedClass.getSimpleName() + ")");
    }

    // ---- convenience wrappers for common Doris nodes ----

    public static PlanShape<LocalExchangeNode> localExchange(LocalExchangeType type, PlanShape<?>... children) {
        return new PlanShape<>(LocalExchangeNode.class,
                le -> le.getExchangeType() == type,
                ImmutableList.copyOf(children), false,
                "LocalExchange(" + type + ")");
    }

    public static PlanShape<AggregationNode> agg(PlanShape<?>... children) {
        return node(AggregationNode.class, children);
    }

    public static PlanShape<OlapScanNode> olapScan(String tableName, PlanShape<?>... children) {
        return new PlanShape<>(OlapScanNode.class,
                s -> tableName.equalsIgnoreCase(s.getOlapTable().getName()),
                ImmutableList.copyOf(children), false,
                "OlapScan(" + tableName + ")");
    }

    public static PlanShape<OlapScanNode> olapScan() {
        return node(OlapScanNode.class);
    }

    public static PlanShape<ExchangeNode> exchange(PlanShape<?>... children) {
        return node(ExchangeNode.class, children);
    }

    public static PlanShape<HashJoinNode> hashJoin(PlanShape<?>... children) {
        return node(HashJoinNode.class, children);
    }

    public static PlanShape<SortNode> sort(PlanShape<?>... children) {
        return node(SortNode.class, children);
    }

    public static PlanShape<AnalyticEvalNode> analytic(PlanShape<?>... children) {
        return node(AnalyticEvalNode.class, children);
    }

    public static PlanShape<UnionNode> union(PlanShape<?>... children) {
        return node(UnionNode.class, children);
    }

    public static PlanShape<NestedLoopJoinNode> nestedLoopJoin(PlanShape<?>... children) {
        return node(NestedLoopJoinNode.class, children);
    }

    public static PlanShape<PartitionSortNode> partitionSort(PlanShape<?>... children) {
        return node(PartitionSortNode.class, children);
    }

    // ---- chained predicate ----

    /**
     * Constrain the current node to also satisfy {@code predicate}.  The lambda
     * parameter type is inferred from the receiver's generic type parameter
     * (e.g. {@link AggregationNode} for {@link #agg}); no explicit cast required.
     */
    public PlanShape<T> where(Predicate<T> predicate) {
        Predicate<T> combined = filter == null ? predicate : filter.and(predicate);
        return new PlanShape<>(nodeClass, combined, children, anyTreeMatch, label);
    }

    // ---- assertion entry points ----

    /** Assert that {@code shape} matches the plan tree rooted at {@code root}. */
    public static void assertMatches(PlanNode root, PlanShape<?> shape) {
        shape.assertMatchesImpl(root);
    }

    /**
     * Assert that {@code shape} matches the plan tree of at least one fragment.
     * Useful when the interesting fragment is buried among several (typical for
     * distributed plans with scan / shuffle fragments).
     */
    public static void assertMatchesAnyFragment(List<PlanFragment> fragments, PlanShape<?> shape) {
        for (PlanFragment fragment : fragments) {
            if (shape.match(fragment.getPlanRoot()).matched) {
                return;
            }
        }
        StringBuilder dump = new StringBuilder();
        for (PlanFragment fragment : fragments) {
            dump.append("[fragment ").append(fragment.getFragmentId()).append("]\n");
            dump.append(indent(dumpTree(fragment.getPlanRoot()), "  "));
        }
        throw new AssertionError("Plan shape did not match any of the " + fragments.size()
                + " fragments.\n  expected: " + shape.describe() + "\n  fragments:\n"
                + indent(dump.toString(), "    "));
    }

    private void assertMatchesImpl(PlanNode root) {
        MatchResult r = match(root);
        if (!r.matched) {
            throw new AssertionError("Plan shape mismatch.\n"
                    + "  expected: " + describe() + "\n"
                    + "  failure : " + r.failureReason + "\n"
                    + "  actual tree:\n" + indent(dumpTree(root), "    "));
        }
    }

    // ---- matching impl ----

    private MatchResult match(PlanNode node) {
        if (anyTreeMatch) {
            return findInSubtree(node, children.get(0));
        }
        if (!nodeClass.isInstance(node)) {
            return MatchResult.fail("expected " + label + " but got "
                    + node.getClass().getSimpleName() + "#" + node.getId().asInt());
        }
        if (filter != null && !filter.test(nodeClass.cast(node))) {
            return MatchResult.fail("predicate failed on " + label + "#" + node.getId().asInt());
        }
        if (children.isEmpty()) {
            return MatchResult.OK;
        }
        if (node.getChildren().size() != children.size()) {
            return MatchResult.fail(label + "#" + node.getId().asInt() + " expected "
                    + children.size() + " children, got " + node.getChildren().size());
        }
        for (int i = 0; i < children.size(); i++) {
            MatchResult r = children.get(i).match(node.getChild(i));
            if (!r.matched) {
                return MatchResult.fail("at " + label + "#" + node.getId().asInt() + ".child[" + i
                        + "]: " + r.failureReason);
            }
        }
        return MatchResult.OK;
    }

    private static MatchResult findInSubtree(PlanNode root, PlanShape<?> inner) {
        MatchResult r = inner.match(root);
        if (r.matched) {
            return r;
        }
        for (PlanNode child : root.getChildren()) {
            r = findInSubtree(child, inner);
            if (r.matched) {
                return r;
            }
        }
        return MatchResult.fail("no node in subtree matches " + inner.describe());
    }

    private String describe() {
        if (anyTreeMatch) {
            return "anyTree(" + children.get(0).describe() + ")";
        }
        StringBuilder sb = new StringBuilder(label);
        if (!children.isEmpty()) {
            sb.append("(");
            for (int i = 0; i < children.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(children.get(i).describe());
            }
            sb.append(")");
        }
        return sb.toString();
    }

    /**
     * Render a plan tree as a text outline (one node per line, indented by depth).
     * Useful for debugging when writing new shape assertions — print the actual
     * plan, then copy the structure into a {@code PlanShape} pattern.
     */
    public static String prettyPrint(PlanNode root) {
        return dumpTree(root);
    }

    private static String dumpTree(PlanNode root) {
        StringBuilder sb = new StringBuilder();
        dumpTree(root, sb, 0);
        return sb.toString();
    }

    private static void dumpTree(PlanNode node, StringBuilder sb, int depth) {
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
        sb.append(node.getClass().getSimpleName()).append("#").append(node.getId().asInt());
        if (node instanceof LocalExchangeNode) {
            sb.append("(").append(((LocalExchangeNode) node).getExchangeType()).append(")");
        }
        sb.append("\n");
        for (PlanNode child : node.getChildren()) {
            dumpTree(child, sb, depth + 1);
        }
    }

    private static String indent(String text, String prefix) {
        StringBuilder sb = new StringBuilder();
        for (String line : text.split("\n", -1)) {
            if (!line.isEmpty()) {
                sb.append(prefix);
            }
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    private static final class MatchResult {
        final boolean matched;
        final String failureReason;

        private MatchResult(boolean matched, String failureReason) {
            this.matched = matched;
            this.failureReason = failureReason;
        }

        static final MatchResult OK = new MatchResult(true, null);

        static MatchResult fail(String reason) {
            return new MatchResult(false, reason);
        }
    }
}
