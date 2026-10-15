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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Except;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.algebra.Intersect;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.algebra.Sort;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

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
    /**
     * The default of {@code hbo_max_scans_per_group}, used when no session is available. 20 keeps
     * every TPC-DS shape observed in this repository (the largest single sub tree is query64's
     * 19 scan join group) while refusing trees which are pathologically wide.
     */
    public static final int DEFAULT_MAX_SCANS_PER_GROUP = 20;
    /**
     * Snapshot value which marks a plan node that is deliberately excluded from the hbo
     * fingerprint (its group is unsupported, e.g. a sort / topn). It is not a fingerprint and is
     * never hashed: the publish path uses it to tell an expected gap from a lost snapshot. A
     * concurrent map can not store a null marker, so an empty string is used.
     */
    public static final String UNSUPPORTED_FINGERPRINT = "";

    private static final List<HboScanDescriptor> INVALID_SCANS = Collections.emptyList();

    /**
     * Characters which structure the canonical form and therefore can not appear raw inside a
     * literal value (see {@link #encodeLiteralValue}).
     */
    private static final String RESERVED_LITERAL_CHARS = "\\%(){}[],;:|";

    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    /**
     * How many groups this query described. A group is described once: the description is memoized on
     * the group and composed by its parents, so this number only grows when a new group is reached.
     */
    private static final AtomicInteger describedGroupCount = new AtomicInteger();

    /**
     * How many descriptions were walked while materializing a string. A description whose string is
     * already materialized is copied instead of being walked again, so this number does not grow for
     * the sub tree of an already materialized child.
     */
    private static final AtomicInteger materializedGroupCount = new AtomicInteger();

    /** Shared invalid instance: the group content is not supported by the simplified struct info. */
    public static final GroupStructInfo INVALID =
            new GroupStructInfo(false, "", Collections.emptyList(), INVALID_SCANS);

    private static final String SEP = ";";

    /** The headers whose arguments the traversal writes in both literal modes. */
    private static final String PROTECTED_HEADERS = "GTWUIEL";

    private static final String NO_LOOKUP_PATH_REASON =
            "storage layer aggregate: the read side applies the row count of the aggregation above it";

    private static final Logger LOG = LogManager.getLogger(GroupStructInfo.class);

    /**
     * Whether literal values participate in the canonical string / fingerprint.
     * <ul>
     *   <li>{@link #WITH_LITERAL} keeps the literal value ({@code lit(10:int)}): the fingerprint is
     *       bound to the exact constants, used for filter roots where the constant changes the
     *       output row count;</li>
     *   <li>{@link #NO_LITERAL} replaces every literal value with {@code *} ({@code lit(*)}): the
     *       fingerprint is constant agnostic. It is the primary key of a scan, of a constant-free
     *       filter shape and of the key the publish path generates for a join / aggregate root; every
     *       root which is annotated also carries a literal carrying form.</li>
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

    /**
     * Whether the description could be built. It is invalid - and the memo caches that answer, so every
     * node above the sub tree gets no fingerprint for the whole query - when one of the following holds
     * for the sub tree it covers:
     * <ul>
     *   <li>it contains an operator the canonical form does not describe, which the annotation prints as
     *       {@code unsupported=<Operator>}: a table valued function relation ({@code numbers},
     *       {@code s3}), an apply, a recursive union, a partition top-n, ...;</li>
     *   <li>it reads more scans than {@code hbo_max_scans_per_group}:
     *       {@code subtree scans=N > hbo_max_scans_per_group=M};</li>
     *   <li>a table name carries a struct info delimiter ({@code ,} or {@code }}), so the printed token
     *       could not be read back: {@code delimiter in table name <table>};</li>
     *   <li>the memo does not look like a canonical form can express it: a unary operator whose group
     *       expression has another arity, a CTE consumer which has no memo edge to its producer, a
     *       reused sub tree whose description is not available;</li>
     *   <li>the data state of a scan cannot be read (an external table whose metadata is not available,
     *       a cloud rpc failure): {@code no data state for <table> (<cause>)}.</li>
     * </ul>
     * A node whose group cannot be resolved at all is not described either, which the annotation reports
     * as {@code no group for <Operator>}.
     */
    private final boolean valid;
    /**
     * The rope of this description: the text fragments of its own layer (in both literal modes) and
     * references to the descriptions of its child groups. A parent therefore records only its own
     * layer and never rebuilds the canonical string of a child; the strings are materialized on the
     * first request (see {@link #getCanonical(LiteralMode)}).
     */
    private final List<Piece> pieces;
    /** The scans of the sub tree, in the order their tokens appear in the canonical string. */
    private final List<HboScanDescriptor> scans;
    /** Why no struct info was built, when it was refused by a limit instead of being unsupported. */
    private final String skipReason;
    // the forms materialized on first request: the read side only needs a fingerprint, while the
    // explain annotation needs the canonical string, so building all of them up front would be wasted
    private String canonicalWithLiteral;
    private String canonicalNoLiteral;
    private String shapeWithLiteral;
    private String shapeNoLiteral;
    private String fingerprintWithLiteral;
    private String fingerprintNoLiteral;

    private GroupStructInfo(boolean valid, String skipReason, List<Piece> pieces,
            List<HboScanDescriptor> scans) {
        this.valid = valid;
        this.skipReason = skipReason;
        this.pieces = pieces;
        this.scans = scans;
    }

    private static GroupStructInfo ofPieces(List<Piece> pieces, List<HboScanDescriptor> scans) {
        return new GroupStructInfo(true, "", pieces, Collections.unmodifiableList(scans));
    }

    private static GroupStructInfo invalidStructInfo(String skipReason) {
        return skipReason.isEmpty()
                ? INVALID
                : new GroupStructInfo(false, skipReason, Collections.emptyList(), INVALID_SCANS);
    }

    /** The reason the struct info was refused by a limit, or empty. Only for diagnostics. */
    public String getSkipReason() {
        return skipReason;
    }

    public boolean isValid() {
        return valid;
    }

    /**
     * The row count and partition selection of every scan of the sub tree, in the order the scan
     * tokens appear in the canonical string (i.e. the order a user sees when pasting the struct
     * info). Empty when the struct info is invalid.
     */
    public List<HboScanDescriptor> getScans() {
        return scans;
    }

    /** The canonical string of one literal mode, with the data state of every scan. */
    public String getCanonical(LiteralMode mode) {
        boolean noLiteral = mode == LiteralMode.NO_LITERAL;
        String materialized = noLiteral ? canonicalNoLiteral : canonicalWithLiteral;
        if (materialized != null) {
            return materialized;
        }
        StringBuilder sb = new StringBuilder(256);
        appendTo(sb, mode, false);
        materialized = sb.toString();
        if (noLiteral) {
            canonicalNoLiteral = materialized;
        } else {
            canonicalWithLiteral = materialized;
        }
        return materialized;
    }

    /**
     * The fingerprint input of one literal mode: the canonical string with the data state of every
     * scan removed ({@code S{db.t}}). It is assembled from the rope and never parsed out of the
     * canonical string, so no text inside a table name or a literal can be mistaken for a scan token.
     */
    public String getShape(LiteralMode mode) {
        boolean noLiteral = mode == LiteralMode.NO_LITERAL;
        String materialized = noLiteral ? shapeNoLiteral : shapeWithLiteral;
        if (materialized != null) {
            return materialized;
        }
        StringBuilder sb = new StringBuilder(256);
        appendTo(sb, mode, true);
        materialized = sb.toString();
        if (noLiteral) {
            shapeNoLiteral = materialized;
        } else {
            shapeWithLiteral = materialized;
        }
        return materialized;
    }

    /** The sha256 of {@link #getShape(LiteralMode)}: the key an hbo entry is registered under. */
    public String getFingerprint(LiteralMode mode) {
        boolean noLiteral = mode == LiteralMode.NO_LITERAL;
        String materialized = noLiteral ? fingerprintNoLiteral : fingerprintWithLiteral;
        if (materialized != null) {
            return materialized;
        }
        materialized = sha256(getShape(mode));
        if (noLiteral) {
            fingerprintNoLiteral = materialized;
        } else {
            fingerprintWithLiteral = materialized;
        }
        return materialized;
    }

    /**
     * Write one form of this description: the pieces of its own layer and, for a child whose same
     * form is already materialized, that materialized string instead of walking the child again.
     */
    private void appendTo(StringBuilder out, LiteralMode mode, boolean shape) {
        materializedGroupCount.incrementAndGet();
        for (Piece piece : pieces) {
            piece.appendTo(out, mode, shape);
        }
    }

    private String materialized(LiteralMode mode, boolean shape) {
        if (shape) {
            return mode == LiteralMode.NO_LITERAL ? shapeNoLiteral : shapeWithLiteral;
        }
        return mode == LiteralMode.NO_LITERAL ? canonicalNoLiteral : canonicalWithLiteral;
    }

    private static String sha256(String value) {
        return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
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
        if (hasNoHboLookupPath(planNode)) {
            return Optional.empty();
        }
        Group group = planNode.getGroupExpression().map(GroupExpression::getOwnerGroup).orElse(null);
        return fingerprintOfGroup(group, mode);
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
        if (hasNoHboLookupPath(planNode)) {
            return Optional.empty();
        }
        return fingerprintOfGroup(resolveGroup(planNode, groupsById), mode);
    }

    private static Optional<String> fingerprintOfGroup(Group group, LiteralMode mode) {
        GroupStructInfo structInfo = group == null ? INVALID : group.getOrComputeHboStructInfo();
        return structInfo.isValid() ? Optional.of(structInfo.getFingerprint(mode)) : Optional.empty();
    }

    /**
     * Resolve the description of the group a plan node belongs to, using the group-expression back
     * reference or the KEY_GROUP group-id state propagated by post processors (see
     * {@link #fingerprintOfPlanNode}). Empty when the group content is unsupported.
     */
    public static Optional<GroupStructInfo> structInfoOfPlanNode(AbstractPlan planNode,
            Map<Integer, Group> groupsById) {
        if (hasNoHboLookupPath(planNode)) {
            // the annotation asks for the struct info first: without this guard the node would advertise
            // a key which no read path ever looks up (see hasNoHboLookupPath)
            return Optional.empty();
        }
        return structInfoOfGroup(resolveGroup(planNode, groupsById));
    }

    private static Optional<GroupStructInfo> structInfoOfGroup(Group group) {
        if (group == null) {
            return Optional.empty();
        }
        GroupStructInfo structInfo = group.getOrComputeHboStructInfo();
        return structInfo.isValid() ? Optional.of(structInfo) : Optional.empty();
    }

    /**
     * The description of a group: computed once per query and memoized on the group (see
     * {@link Group#getOrComputeHboStructInfo}), so that a parent composes the description of its
     * children instead of describing their sub trees again.
     */
    public static GroupStructInfo of(Group group) {
        return describeGroup(group);
    }

    public static GroupStructInfo describeGroup(Group group) {
        return describeGroup(group, new Ctx());
    }

    private static GroupStructInfo describeGroup(Group group, Ctx ctx) {
        if (!ctx.visited.add(group)) {
            // the same group is reached twice (a CTE which is consumed twice, a scan which feeds two
            // consumers, a shared sub tree): a tree cannot express the reuse, so the sub tree is
            // described again - the description is the one of a plan which writes that sub tree twice
            // (a self join for instance), and the read side of a query builds the same description, so
            // an entry recorded from one form applies to the other. The sub tree was read once, so the
            // repeated reference composes its canonical form but not its scans again (the drift guard
            // compares the scan states of the tables a query actually reads).
            GroupStructInfo memoized = group.getHboStructInfoOrNull();
            if (memoized != null) {
                return memoized;
            }
            invalid(ctx, "sharing=group " + group.getGroupId().asInt() + " reached twice");
            return INVALID;
        }
        return describeVisited(group, ctx);
    }

    /**
     * The description of a group which the caller already marked visited (the join chain marks the
     * groups it descends through itself, so that a group is never added twice within one build).
     */
    private static GroupStructInfo describeVisited(Group group, Ctx ctx) {
        GroupStructInfo memoized = group.getHboStructInfoOrNull();
        if (memoized != null) {
            return memoized;
        }
        GroupStructInfo computed = build(group, ctx);
        group.setHboStructInfo(computed);
        return computed;
    }

    /**
     * The first logical expression of a group, or null when it holds none: {@link
     * Group#getFirstLogicalExpression()} asserts that there is one, so a group which only holds
     * physical expressions (an enforcer group, or a group a rule filled with physical plans) cannot be
     * asked that way.
     */
    private static GroupExpression firstLogicalExpression(Group group) {
        List<GroupExpression> logical = group.getLogicalExpressions();
        return logical.isEmpty() ? null : logical.get(0);
    }

    /** Why a group has nothing to describe: its chosen id and how many expressions it holds. */
    private static String describeGroupState(Group group) {
        return group.getGroupId().asInt()
                + " (logical=" + group.getLogicalExpressions().size()
                + ", physical=" + group.getPhysicalExpressions().size()
                + ", enforcers=" + group.getEnforcers().size() + ")";
    }

    /**
     * The expression which is described for a group. The first logical expression is used, and the logical
     * form is what the key of a group has to be made of: the read side looks an entry up while the plan is
     * still being optimized, i.e. before any expression is chosen, and a physical plan may hold nodes the
     * logical plan abstracts away (the two staged aggregation of a {@code count(*)}, for instance), which
     * would move the key of every such group and make a pinned entry miss. The expression the plan chooser
     * picked is deliberately not consulted for the same reason - a key must not depend on when it is
     * computed. The method is package visible so that a test can pin that the recorded choice of the plan
     * chooser is not consulted.
     */
    static GroupExpression getRepresentativeExpression(Group group) {
        GroupExpression logical = firstLogicalExpression(group);
        if (logical != null) {
            return logical;
        }
        // Some groups have no logical plan at all, for instance the group of a
        // {@link PhysicalStorageLayerAggregate} (an aggregation answered from the storage layer is a
        // catalog relation a rule inserts, there is no logical aggregation left to describe) or a group
        // which only holds an enforcer. Such a group is described through the physical expression it
        // holds: it is the only form it has, so both sides - the read side and the annotation - see the
        // same one, and giving the whole struct info up would hide every node above it.
        List<GroupExpression> physical = group.getPhysicalExpressions();
        if (!physical.isEmpty()) {
            return physical.get(0);
        }
        // an enforcer group keeps its expression in the enforcer list, not among the physical ones
        return group.getEnforcers().isEmpty() ? null : group.getEnforcers().keySet().iterator().next();
    }

    /**
     * Describe one group by composing the descriptions of its children (which are memoized, so each
     * group of a plan is described exactly once per query), in both literal modes at once.
     */
    private static GroupStructInfo build(Group group, Ctx ctx) {
        describedGroupCount.incrementAndGet();
        HboStructSummary summary = group.getOrComputeHboStructSummary();
        int maxScansPerGroup = hboMaxScansPerGroup();
        if (maxScansPerGroup > 0 && summary.getScanCount() > maxScansPerGroup) {
            // The canonical string of a group describes its whole sub tree, so a cheap structural
            // summary is used as a gate: a sub tree with more scan tokens than
            // hbo_max_scans_per_group is not described at all (the read side falls back to the
            // optimizer estimation and EXPLAIN reports why).
            return invalidStructInfo("subtree scans=" + summary.getScanCount()
                    + " > hbo_max_scans_per_group=" + maxScansPerGroup);
        }
        GroupExpression logical = firstLogicalExpression(group);
        if (logical != null) {
            return describeExpression(group, logical, ctx);
        }
        GroupExpression representative = getRepresentativeExpression(group);
        if (representative == null) {
            // a group without any expression (no logical one, no physical one, no enforcer): there is
            // nothing to describe
            return invalidStructInfo("no expression in group " + describeGroupState(group));
        }
        return describeExpression(group, representative, ctx);
    }

    /** Describe one expression of a group: the body of {@link #build} without the choice of it. */
    private static GroupStructInfo describeExpression(Group group, GroupExpression ge, Ctx ctx) {
        Canonical out = new Canonical();
        try {
            appendPlan(ge.getPlan(), ge, out, ctx);
        } catch (RuntimeException e) {
            // memo content not supported by the simplified struct info: treat as invalid and
            // fall back to legacy behavior instead of failing the optimizer on the hot path
            LOG.debug("failed to compute hbo struct info for group {}", group.getGroupId(), e);
            return invalidStructInfo(ctx.reason);
        }
        if (!ctx.valid) {
            return invalidStructInfo(ctx.reason);
        }
        return ofPieces(out.getPieces(), out.getScans());
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

    private static void appendPlan(Plan plan, GroupExpression ge, Canonical out, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        // the dispatch is written against the algebra interfaces, so that the description of a group can
        // follow the expression which was chosen for the final plan (a physical one) as well as a logical
        // expression while the plan choice has not been made yet
        if (plan instanceof OlapScan) {
            appendScan((OlapScan) plan, out, ctx);
        } else if (plan instanceof Filter) {
            Filter filter = (Filter) plan;
            StringBuilder withLiteralHeader = new StringBuilder("F{");
            StringBuilder noLiteralHeader = new StringBuilder("F{");
            appendNormalizedSorted(filter.getConjuncts(), withLiteralHeader, noLiteralHeader);
            out.modes(withLiteralHeader.append("}(").toString(), noLiteralHeader.append("}(").toString());
            appendChild(ge, 0, out, ctx);
            out.same(")");
        } else if (plan instanceof LogicalCTEConsumer || plan instanceof PhysicalCTEConsumer
                || plan instanceof LogicalCTEProducer || plan instanceof PhysicalCTEProducer) {
            if (ge.arity() == 0 && plan instanceof PhysicalCTEConsumer) {
                // the consumer references its producer by id, not by a memo edge: there is no sub tree
                // to describe here until the producer group is resolved through that id
                invalid(ctx, "unsupported=cte consumer without a memo edge");
                return;
            }
            // a CTE consumer describes the sub tree it consumes and a producer the sub tree it
            // materializes: both are transparent, and a consumer which is reached twice composes the
            // description of that sub tree twice (see describeGroup)
            if (ge.arity() == 1) {
                appendChild(ge, 0, out, ctx);
            } else {
                invalid(ctx, "arity=" + ge.arity() + " of " + plan.getClass().getSimpleName());
            }
        } else if (plan instanceof LogicalCTEAnchor || plan instanceof PhysicalCTEAnchor) {
            // the value of an anchor is its consumer side: the producer is the first child and the plan
            // which consumes it is the second one (LogicalCTEAnchor.computeOutput is right().getOutput(),
            // and the translator executes child(0) and returns child(1))
            if (ge.arity() >= 2) {
                appendChild(ge, 1, out, ctx);
            } else {
                invalid(ctx, "arity=" + ge.arity() + " of " + plan.getClass().getSimpleName());
            }
        } else if (plan instanceof LogicalFileScan || plan instanceof PhysicalFileScan) {
            // an external table (hive / iceberg / hudi) and a file table function are catalog relations
            // whose data state the optimizer cannot read before the scan runs: they are described by the
            // table they read, so a pinned row count stays applicable and the drift guard reports that it
            // cannot compare their data state
            appendFileScan(plan, out, ctx);
        } else if (plan instanceof PhysicalStorageLayerAggregate) {
            // the optimizer answers an aggregation over a scan from the storage layer itself: the node is
            // a catalog relation without children, and its identity is the aggregation together with the
            // relation it reads. The argument expressions are referenced by expression id there, which is
            // not stable across queries, so they are left out: they do not change the number of rows this
            // node produces (one row, the query has no grouping).
            PhysicalStorageLayerAggregate storageAggregate = (PhysicalStorageLayerAggregate) plan;
            String header = "A{sl,op=" + storageAggregate.getAggOp() + "}(";
            out.modes(header, header).modes(")", ")");
            if (storageAggregate.getRelation() instanceof OlapScan) {
                appendScan((OlapScan) storageAggregate.getRelation(), out, ctx);
            }
        } else if (plan instanceof TopN) {
            // a top-n keeps its bound and its ordering out of the folding: the bound is written with the
            // same text in both modes so that NO_LITERAL cannot fold the value away and let two different
            // bounds share one entry, and the ordering is part of the identity because it decides *which*
            // rows survive - two top-ns with the same bound but a different ordering can match a
            // different number of partners in the join above them
            if (ge.arity() == 1) {
                TopN topN = (TopN) plan;
                StringBuilder withLiteral = new StringBuilder();
                StringBuilder noLiteral = new StringBuilder();
                appendTopNHeader(topN, withLiteral, noLiteral);
                out.addText(withLiteral.toString(), noLiteral.toString());
                appendChild(ge, 0, out, ctx);
            } else {
                invalid(ctx, "arity=" + ge.arity() + " of " + plan.getClass().getSimpleName());
            }
        } else if (plan instanceof Limit) {
            if (ge.arity() == 1) {
                Limit limit = (Limit) plan;
                out.addText(bound("L", limit.getLimit(), limit.getOffset()),
                        bound("L", limit.getLimit(), limit.getOffset()));
                appendChild(ge, 0, out, ctx);
            } else {
                invalid(ctx, "arity=" + ge.arity() + " of " + plan.getClass().getSimpleName());
            }
        } else if (plan instanceof Generate) {
            appendGenerate((Generate) plan, ge, out, ctx);
        } else if (plan instanceof Repeat) {
            // a repeat is an aggregate with several grouping key sets; a constant inside a grouping key
            // does not change the number of groups, so the normal folding is safe here
            appendRepeat((Repeat<?>) plan, ge, out, ctx);
        } else if (plan instanceof Union || plan instanceof Intersect || plan instanceof Except) {
            appendSetOperation(plan, (SetOperation) plan, ge, out, ctx);
        } else if (plan instanceof PhysicalDistribute || plan instanceof PhysicalAssertNumRows) {
            // an enforcer does not change the number of rows and carries no identity of its own: the
            // description of the group is the description of the plan below it
            if (ge.arity() == 1) {
                appendChild(ge, 0, out, ctx);
            } else {
                invalid(ctx, "arity=" + ge.arity() + " of " + plan.getClass().getSimpleName());
            }
        } else if (plan instanceof Window) {
            appendWindow((Window) plan, ge, out, ctx);
        } else if (plan instanceof Project || plan instanceof Sort) {
            // a project and a sort are transparent for structure matching: neither of them changes the
            // number of rows, so the nodes above them are described by the shape below and they must not
            // make the struct info of those nodes disappear
            if (ge.arity() == 1) {
                appendChild(ge, 0, out, ctx);
            } else {
                invalid(ctx, "arity=" + ge.arity() + " of " + plan.getClass().getSimpleName());
            }
        } else if (plan instanceof Join) {
            appendJoin((Join) plan, ge, out, ctx);
        } else if (plan instanceof Aggregate) {
            appendAggregate((Aggregate<?>) plan, ge, out, ctx);
        } else {
            // unsupported head operator (topn/limit/union/intersect/except/cte/tvf/generate/...)
            invalid(ctx, "unsupported=" + plan.getClass().getSimpleName());
        }
    }

    private static void appendChild(GroupExpression ge, int index, Canonical out, Ctx ctx) {
        if (!ctx.valid) {
            return;
        }
        // the child is described once per query: its memoized parts are composed here instead of
        // walking its sub tree again (the grand children are never visited by this build)
        GroupStructInfo child = describeGroup(ge.child(index), ctx);
        if (!child.isValid()) {
            // keep the reason of the child: a description which is invalid because of its child reports
            // the cause of that child (the first reason wins)
            invalid(ctx, child.getSkipReason());
            return;
        }
        out.append(child);
    }

    private static void appendJoin(Join join, GroupExpression ge, Canonical out, Ctx ctx) {
        if (isFlattenableJoinType(join.getJoinType())) {
            appendJoinChain(join, ge, out, ctx);
            return;
        }
        // Outer / semi / anti / asof joins: the two sides are semantically different and the join
        // cannot be reassociated, so the memo order of the children and the join type are kept.
        out.same("J{" + join.getJoinType() + ",c:[");
        TreeSet<String> conditions = new TreeSet<>();
        TreeSet<String> conditionsNoLiteral = new TreeSet<>();
        collectJoinConditions(join, conditions, conditionsNoLiteral);
        out.modes(String.join(SEP, conditions), String.join(SEP, conditionsNoLiteral));
        out.same("]}(");
        appendChild(ge, 0, out, ctx);
        out.same(SEP);
        appendChild(ge, 1, out, ctx);
        out.same(")");
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
    private static void appendJoinChain(Join join, GroupExpression ge, Canonical out, Ctx ctx) {
        TreeSet<String> conditions = new TreeSet<>();
        TreeSet<String> conditionsNoLiteral = new TreeSet<>();
        List<Leaf> leaves = new ArrayList<>();
        collectJoinConditions(join, conditions, conditionsNoLiteral);
        for (int i = 0; i < ge.arity(); i++) {
            ChainInfo child = chainInfoOf(ge.child(i));
            if (!child.valid) {
                invalid(ctx, child.skipReason);
                return;
            }
            conditions.addAll(child.conditions);
            conditionsNoLiteral.addAll(child.conditionsNoLiteral);
            if (!composeChain(child, leaves, ctx)) {
                return;
            }
        }
        if (!ctx.valid) {
            return;
        }
        // the leaves are ordered by their fingerprint input (their own shape, i.e. without the data
        // state of their scans): the order is part of the fingerprint, so it must not depend on a
        // data state which the fingerprint deliberately ignores
        leaves.sort(Comparator.comparing(Leaf::getShape));
        out.same("J{inner,c:[");
        out.modes(String.join(SEP, conditions), String.join(SEP, conditionsNoLiteral));
        out.same("]}(");
        for (int i = 0; i < leaves.size(); i++) {
            if (i > 0) {
                out.same(SEP);
            }
            out.append(leaves.get(i).getLeaf(), leaves.get(i).isRepeated());
        }
        out.same(")");
    }

    /**
     * The conditions and the leaves of a join chain, memoized per group: a chain of n joins used to walk
     * its own sub tree once per member (O(n^2) condition renderings for the whole query), while the
     * structure of a chain does not depend on the build which reads it - only the bookkeeping of a sub
     * tree which is reached more than once does, and that is done while composing the chain.
     */
    public static final class ChainInfo {
        private final TreeSet<String> conditions;
        private final TreeSet<String> conditionsNoLiteral;
        /** the groups the chain is made of, without its leaves: they detect a chain which is reused */
        private final List<Group> members;
        /** the groups which are not flattened into the chain, in traversal order */
        private final List<Group> leaves;
        private final boolean valid;
        private final String skipReason;

        private ChainInfo(TreeSet<String> conditions, TreeSet<String> conditionsNoLiteral,
                List<Group> members, List<Group> leaves, boolean valid, String skipReason) {
            this.conditions = conditions;
            this.conditionsNoLiteral = conditionsNoLiteral;
            this.members = members;
            this.leaves = leaves;
            this.valid = valid;
            this.skipReason = skipReason;
        }

        private static ChainInfo of(TreeSet<String> conditions, TreeSet<String> conditionsNoLiteral,
                List<Group> members, List<Group> leaves) {
            return new ChainInfo(conditions, conditionsNoLiteral, members, leaves, true, "");
        }

        private static ChainInfo failed(String skipReason) {
            return new ChainInfo(new TreeSet<>(), new TreeSet<>(), new ArrayList<>(), new ArrayList<>(),
                    false, skipReason);
        }
    }

    /** The memoized chain info of a group, computed at most once per query. */
    private static ChainInfo chainInfoOf(Group group) {
        ChainInfo memoized = group.getHboChainInfoOrNull();
        if (memoized != null) {
            return memoized;
        }
        ChainInfo computed = buildChainInfo(group);
        group.setHboChainInfo(computed);
        return computed;
    }

    /**
     * The chain structure of a group: a flattenable join contributes its own conditions and the chains
     * of its children, a transparent operator contributes the chain below it, anything else is a leaf.
     */
    private static ChainInfo buildChainInfo(Group group) {
        GroupExpression ge = getRepresentativeExpression(group);
        if (ge == null) {
            return ChainInfo.failed("no logical expression in group " + describeGroupState(group));
        }
        Plan plan = ge.getPlan();
        if (plan instanceof Join && isFlattenableJoinType(((Join) plan).getJoinType())) {
            TreeSet<String> conditions = new TreeSet<>();
            TreeSet<String> conditionsNoLiteral = new TreeSet<>();
            List<Group> members = new ArrayList<>();
            List<Group> leaves = new ArrayList<>();
            members.add(group);
            collectJoinConditions((Join) plan, conditions, conditionsNoLiteral);
            for (int i = 0; i < ge.arity(); i++) {
                ChainInfo child = chainInfoOf(ge.child(i));
                if (!child.valid) {
                    return ChainInfo.failed(child.skipReason);
                }
                conditions.addAll(child.conditions);
                conditionsNoLiteral.addAll(child.conditionsNoLiteral);
                members.addAll(child.members);
                leaves.addAll(child.leaves);
            }
            return ChainInfo.of(conditions, conditionsNoLiteral, members, leaves);
        }
        // a top-n must not be flattened into the chain although it is a sort: it keeps a bounded number
        // of rows, so it is a boundary of the chain like a limit, and the ordering it keeps them by is
        // part of the identity of every node above it. A window is a boundary as well: it keeps every
        // row, but the values of the columns it computes depend on its definition, and a node above it
        // reads those values.
        if ((plan instanceof Project || plan instanceof Sort || plan instanceof Window)
                && !(plan instanceof TopN) && !(plan instanceof Window) && ge.arity() == 1) {
            ChainInfo child = chainInfoOf(ge.child(0));
            if (!child.valid) {
                return child;
            }
            List<Group> members = new ArrayList<>();
            members.add(group);
            members.addAll(child.members);
            return ChainInfo.of(child.conditions, child.conditionsNoLiteral, members, child.leaves);
        }
        List<Group> leaves = new ArrayList<>();
        leaves.add(group);
        return ChainInfo.of(new TreeSet<>(), new TreeSet<>(), new ArrayList<>(), leaves);
    }

    /**
     * Add the leaves of a chain to the leaves of the build which is being described: a sub tree which the
     * build already reached contributes its canonical form again, but not its scans (its tables are read
     * once), and a chain which is reused makes all of its leaves repeated.
     */
    private static boolean composeChain(ChainInfo chain, List<Leaf> leaves, Ctx ctx) {
        boolean firstUse = true;
        for (Group member : chain.members) {
            if (!ctx.visited.add(member)) {
                firstUse = false;
            }
        }
        for (Group group : chain.leaves) {
            boolean repeated = !firstUse || !ctx.visited.add(group);
            GroupStructInfo leaf = describeVisited(group, ctx);
            if (!leaf.isValid()) {
                invalid(ctx, leaf.getSkipReason());
                return false;
            }
            leaves.add(new Leaf(leaf, repeated));
        }
        return true;
    }

    /** One leaf of a flattened join chain: the memoized description of that sub tree. */
    private static class Leaf {
        private final GroupStructInfo leaf;
        /** whether the sub tree of this leaf is referenced by another leaf as well */
        private final boolean repeated;

        Leaf(GroupStructInfo leaf) {
            this(leaf, false);
        }

        Leaf(GroupStructInfo leaf, boolean repeated) {
            this.leaf = leaf;
            this.repeated = repeated;
        }

        GroupStructInfo getLeaf() {
            return leaf;
        }

        boolean isRepeated() {
            return repeated;
        }

        String getShape() {
            return leaf.getShape(LiteralMode.WITH_LITERAL);
        }
    }

    /** Inner and cross joins may be reordered, reassociated and therefore flattened. */
    private static boolean isFlattenableJoinType(JoinType joinType) {
        return joinType == JoinType.INNER_JOIN || joinType == JoinType.CROSS_JOIN;
    }

    private static void appendAggregate(Aggregate<?> agg, GroupExpression ge, Canonical out, Ctx ctx) {
        // Only the grouping keys and the child take part: the output row count of an aggregation is
        // the number of groups, which the aggregate functions and the output expressions cannot
        // change, so keeping them would only split one plan pattern into several cache entries.
        StringBuilder withLiteralHeader = new StringBuilder("A{gb:");
        StringBuilder noLiteralHeader = new StringBuilder("A{gb:");
        appendNormalizedSorted(agg.getGroupByExpressions(), withLiteralHeader, noLiteralHeader);
        out.modes(withLiteralHeader.append("}(").toString(), noLiteralHeader.append("}(").toString());
        appendChild(ge, 0, out, ctx);
        out.same(")");
    }

    /** Mark the description invalid, remembering why (the first reason is the one reported). */
    private static void invalid(Ctx ctx, String reason) {
        ctx.valid = false;
        if (ctx.reason.isEmpty()) {
            ctx.reason = reason;
        }
    }

    /**
     * The header of a top-n: its bound, and the ordering which decides the rows it keeps. The ordering is
     * rendered in both literal modes like a predicate, with the direction and the null order of every key,
     * because {@code a asc} and {@code a desc} are not interchangeable for the nodes above (a literal
     * inside an ordering key changes nothing about the number of rows of the top-n itself, but it can
     * change which rows reach the node above).
     */
    private static void appendTopNHeader(TopN topN, StringBuilder withLiteral, StringBuilder noLiteral) {
        String head = "T{" + topN.getLimit() + "," + topN.getOffset() + ";ord:[";
        withLiteral.append(head);
        noLiteral.append(head);
        boolean first = true;
        for (OrderKey orderKey : topN.getOrderKeys()) {
            if (!first) {
                withLiteral.append(',');
                noLiteral.append(',');
            }
            first = false;
            appendNormalizedExpression(orderKey.getExpr(), withLiteral, noLiteral);
            String direction = (orderKey.isAsc() ? ":asc" : ":desc")
                    + (orderKey.isNullFirst() ? ":nulls_first" : ":nulls_last");
            withLiteral.append(direction);
            noLiteral.append(direction);
        }
        withLiteral.append("]}");
        noLiteral.append("]}");
    }

    /**
     * A window computes its columns from the rows of its partition, so its definition decides the values a
     * node above it reads: `row_number() OVER (PARTITION BY b ORDER BY a) AS rn` and the same expression
     * partitioned by another column produce the same column name with different values, and a predicate
     * above them (`rn % 2 = 0`) therefore keeps a different number of rows. The definition is part of the
     * identity for that reason, while the window itself adds no rows - which is also why it is not a leaf
     * of a join chain, and why a chain below it is described as its child.
     */
    private static void appendWindow(Window window, GroupExpression ge, Canonical out, Ctx ctx) {
        if (ge.arity() != 1) {
            invalid(ctx, "arity=" + ge.arity() + " of " + ge.getPlan().getClass().getSimpleName());
            return;
        }
        StringBuilder withLiteral = new StringBuilder("W{");
        StringBuilder noLiteral = new StringBuilder("W{");
        boolean firstExpression = true;
        for (NamedExpression namedExpression : window.getWindowExpressions()) {
            // the algebra interface exposes the named expressions; each of them is the alias which gives
            // the computed column its name, and the window expression is its child (the same shape
            // LogicalWindow#getActualWindowExpressions reads)
            WindowExpression windowExpression = (WindowExpression) namedExpression.child(0);
            if (!firstExpression) {
                withLiteral.append(';');
                noLiteral.append(';');
            }
            firstExpression = false;
            appendNormalizedExpression(windowExpression.getFunction(), withLiteral, noLiteral);
            withLiteral.append(";part:[");
            noLiteral.append(";part:[");
            appendNormalizedList(windowExpression.getPartitionKeys(), withLiteral, noLiteral);
            withLiteral.append("];ord:[");
            noLiteral.append("];ord:[");
            boolean firstKey = true;
            for (OrderExpression orderExpression : windowExpression.getOrderKeys()) {
                if (!firstKey) {
                    withLiteral.append(',');
                    noLiteral.append(',');
                }
                firstKey = false;
                OrderKey orderKey = orderExpression.getOrderKey();
                appendNormalizedExpression(orderKey.getExpr(), withLiteral, noLiteral);
                String direction = (orderKey.isAsc() ? ":asc" : ":desc")
                        + (orderKey.isNullFirst() ? ":nulls_first" : ":nulls_last");
                withLiteral.append(direction);
                noLiteral.append(direction);
            }
            // a frame boundary is a number of rows, so its text is the same in both modes: folding it
            // would let a bounded frame share a key with an unbounded one
            String frame = windowExpression.getWindowFrame().map(Object::toString).orElse("");
            withLiteral.append("];frame:[").append(frame).append("]}");
            noLiteral.append("];frame:[").append(frame).append("]}");
        }
        out.modes(withLiteral.append("}(").toString(), noLiteral.append("}(").toString());
        appendChild(ge, 0, out, ctx);
        out.same(")");
    }

    /** Append a comma separated list of normalized expressions (both literal modes). */
    private static void appendNormalizedList(List<? extends Expression> expressions,
            StringBuilder withLiteral, StringBuilder noLiteral) {
        boolean first = true;
        for (Expression expression : expressions) {
            if (!first) {
                withLiteral.append(',');
                noLiteral.append(',');
            }
            first = false;
            appendNormalizedExpression(expression, withLiteral, noLiteral);
        }
    }

    /** The bound of a limit / top-n: the same text in both modes, a bound is never folded. */
    private static String bound(String kind, long limit, long offset) {
        return kind + "{" + limit + "," + offset + "}";
    }

    /**
     * A generate expands a row into the rows of its table functions, so its identity is the identity of
     * those functions: the function and its arguments decide how many rows come out. A literal inside an
     * argument does change that number (`split(x, ',')` is not `split(x, ';')`), so the arguments are
     * rendered in both modes with their literal, and the `_outer` variants are distinguished by the
     * function name itself.
     */
    private static void appendGenerate(Generate generate, GroupExpression ge, Canonical out, Ctx ctx) {
        if (ge.arity() != 1) {
            invalid(ctx, "arity=" + ge.arity() + " of " + ge.getPlan().getClass().getSimpleName());
            return;
        }
        StringBuilder functions = new StringBuilder();
        for (int i = 0; i < generate.getGenerators().size(); i++) {
            if (i > 0) {
                functions.append(';');
            }
            functions.append(identityText(generate.getGenerators().get(i)));
        }
        String header = "G{tf:[" + functions + "]}";
        out.addText(header, header);
        appendChild(ge, 0, out, ctx);
    }

    /**
     * A repeat produces the rows of several grouping sets, so its identity is the list of those sets.
     * The order of the sets is part of the identity (the grouping id values depend on it) and is kept as
     * written; the expressions of one set are sorted, because their order does not change the groups.
     */
    private static void appendRepeat(Repeat<?> repeat, GroupExpression ge, Canonical out, Ctx ctx) {
        if (ge.arity() != 1) {
            invalid(ctx, "arity=" + ge.arity() + " of " + ge.getPlan().getClass().getSimpleName());
            return;
        }
        StringBuilder withLiteral = new StringBuilder("R{gs:[");
        StringBuilder noLiteral = new StringBuilder("R{gs:[");
        List<List<Expression>> groupingSets = repeat.getGroupingSets();
        for (int i = 0; i < groupingSets.size(); i++) {
            if (i > 0) {
                withLiteral.append(';');
                noLiteral.append(';');
            }
            withLiteral.append('[');
            noLiteral.append('[');
            appendNormalizedSorted(groupingSets.get(i), withLiteral, noLiteral);
            withLiteral.append(']');
            noLiteral.append(']');
        }
        out.addText(withLiteral.append("]}").toString(), noLiteral.append("]}").toString());
        appendChild(ge, 0, out, ctx);
    }

    /**
     * The identity of a set operation is its kind, its qualifier and the columns which decide whether two
     * rows are the same (the outputs): a project is transparent, so two plans which differ only in the
     * columns they compare would otherwise share an entry although their row counts differ. The branches
     * are sorted, because the memo may reorder them; an except keeps its first branch (the minuend),
     * the other ones are the subtrahends and therefore order insensitive.
     */
    private static void appendSetOperation(Plan plan, SetOperation setOperation, GroupExpression ge,
            Canonical out, Ctx ctx) {
        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < setOperation.getOutputs().size(); i++) {
            if (i > 0) {
                columns.append(';');
            }
            columns.append(identityText(setOperation.getOutputs().get(i)));
        }
        String kind = plan instanceof Union ? "U" : plan instanceof Intersect ? "I" : "E";
        // the qualifier decides whether the rows of the operation are deduplicated, which changes the
        // row count, so it is part of the identity (an all and a distinct operation never share an entry)
        String qualifier = String.valueOf(setOperation.getQualifier()).toLowerCase();
        // MergeOneRowRelationIntoUnion moves the constant SELECT branches out of the children: they are not
        // in the memo any more, and this header is the only place which can carry them. They belong to the
        // identity - two distinct unions which differ only in their constants keep a different number of
        // rows - so their values are written in both modes: folding them would let the two share a key.
        StringBuilder constants = new StringBuilder();
        if (setOperation instanceof Union) {
            List<List<NamedExpression>> constantExprsList = ((Union) setOperation).getConstantExprsList();
            if (!constantExprsList.isEmpty()) {
                constants.append(",const:[");
                for (int branch = 0; branch < constantExprsList.size(); branch++) {
                    if (branch > 0) {
                        constants.append(';');
                    }
                    List<NamedExpression> branchExpressions = constantExprsList.get(branch);
                    for (int i = 0; i < branchExpressions.size(); i++) {
                        if (i > 0) {
                            constants.append(',');
                        }
                        StringBuilder withLiteral = new StringBuilder();
                        StringBuilder noLiteral = new StringBuilder();
                        appendNormalizedExpression(branchExpressions.get(i), withLiteral, noLiteral);
                        constants.append(withLiteral);
                    }
                    constants.append(']');
                }
            }
        }
        String header = kind + "{" + qualifier + ",cols:[" + columns + "]" + constants + "}";
        List<GroupStructInfo> branches = new ArrayList<>(ge.arity());
        for (int i = 0; i < ge.arity(); i++) {
            GroupStructInfo branch = describeGroup(ge.child(i), ctx);
            if (!branch.isValid()) {
                invalid(ctx, branch.getSkipReason());
                return;
            }
            branches.add(branch);
        }
        out.addText(header, header);
        out.addText("(", "(");
        if (plan instanceof Except) {
            // the first branch is the minuend: it keeps its position, the subtrahends are sorted below
            out.append(branches.get(0));
            List<GroupStructInfo> subtrahends = new ArrayList<>(branches.subList(1, branches.size()));
            subtrahends.sort(Comparator.comparing(branch -> branch.getShape(LiteralMode.WITH_LITERAL)));
            for (GroupStructInfo subtrahend : subtrahends) {
                out.addText(";", ";");
                out.append(subtrahend);
            }
        } else {
            List<GroupStructInfo> sorted = new ArrayList<>(branches);
            sorted.sort(Comparator.comparing(branch -> branch.getShape(LiteralMode.WITH_LITERAL)));
            for (int i = 0; i < sorted.size(); i++) {
                if (i > 0) {
                    out.addText(";", ";");
                }
                out.append(sorted.get(i));
            }
        }
        out.addText(")", ")");
    }

    /**
     * Render an expression for the identity of an operator whose row count depends on its literals (a
     * table function argument, the compared columns of a set operation): the WITH_LITERAL rendering is
     * written into both modes, so that the NO_LITERAL folding cannot merge two different values into one
     * entry.
     */
    private static String identityText(Expression expression) {
        StringBuilder withLiteral = new StringBuilder();
        appendNormalizedExpression(expression, withLiteral, new StringBuilder());
        return withLiteral.toString();
    }

    /** Describe a scan of an external table: its token carries the table name and no data state. */
    private static void appendFileScan(Plan plan, Canonical out, Ctx ctx) {
        TableIf table = plan instanceof LogicalFileScan
                ? ((LogicalFileScan) plan).getTable()
                : ((PhysicalFileScan) plan).getTable();
        String name = HboScanDescriptor.qualifiedNameOf(table);
        if (name.indexOf(',') >= 0 || name.indexOf('}') >= 0) {
            // the token delimiters cannot appear in a table name: the printed struct info could not be
            // read back, and a pasted one could resolve to another table
            invalid(ctx, "delimiter in table name " + name);
            return;
        }
        out.scan(HboScanDescriptor.ofUnreadableState(table));
    }

    private static void appendScan(OlapScan scan, Canonical out, Ctx ctx) {
        if (scan instanceof LogicalOlapScan && ((LogicalOlapScan) scan).getTableSample().isPresent()) {
            // a table sample reads a random subset of the table, and a different one on every execution:
            // no row count recorded for such a scan can be reused, and a node above it inherits that
            invalid(ctx, "tablesample of " + scan.getTable().getNameWithFullQualifiers());
            return;
        }
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
                invalid(ctx, "delimiter in table name " + descriptor.getTable());
                return;
            }
            out.scan(descriptor);
        } catch (org.apache.doris.rpc.RpcException e) {
            // the data state of the table cannot be read (a cloud rpc failure, an external table whose
            // metadata is not available): the sub tree is not described, and the entry of a node above it
            // is not applied
            LOG.debug("failed to get visible version for scan {}", scan.getTable().getNameWithFullQualifiers(), e);
            invalid(ctx, "no data state for " + scan.getTable().getNameWithFullQualifiers()
                    + " (" + e.getMessage() + ")");
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

    // -----------------------------------------------------------------------------------
    // expression normalization: strip slot/expr ids, keep qualifier + column + literal value
    // -----------------------------------------------------------------------------------

    /**
     * Append the two forms of a set of conjuncts (each form sorted) while normalizing every conjunct
     * once.
     */
    private static void appendNormalizedSorted(Collection<Expression> conjuncts,
            StringBuilder withLiteral, StringBuilder noLiteral) {
        List<String> withLiteralParts = new ArrayList<>(conjuncts.size());
        List<String> noLiteralParts = new ArrayList<>(conjuncts.size());
        for (Expression conjunct : conjuncts) {
            StringBuilder partWithLiteral = new StringBuilder();
            StringBuilder partNoLiteral = new StringBuilder();
            appendNormalizedExpression(conjunct, partWithLiteral, partNoLiteral);
            withLiteralParts.add(partWithLiteral.toString());
            noLiteralParts.add(partNoLiteral.toString());
        }
        withLiteralParts.sort(String::compareTo);
        noLiteralParts.sort(String::compareTo);
        withLiteral.append(String.join(SEP, withLiteralParts));
        noLiteral.append(String.join(SEP, noLiteralParts));
    }

    /**
     * Collect both forms of the conditions of one join node (hash and other conjuncts) as sorted, de
     * duplicated sets, normalizing every conjunct once: whether a given conjunct is classified as a
     * hash conjunct or as an other conjunct depends on the grouping of the chain, so that
     * classification cannot be part of a canonical form that has to be insensitive to the grouping.
     */
    private static void collectJoinConditions(Join join,
            Set<String> conditions, Set<String> conditionsNoLiteral) {
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            addCondition(conjunct, conditions, conditionsNoLiteral);
        }
        for (Expression conjunct : join.getOtherJoinConjuncts()) {
            addCondition(conjunct, conditions, conditionsNoLiteral);
        }
        // a mark join carries its correlation condition in the mark conjuncts, and the translator turns
        // them into the hash condition of the executed null aware join: they decide which rows the node
        // keeps, so two mark joins which differ only in that condition must not share a key
        for (Expression conjunct : join.getMarkJoinConjuncts()) {
            addCondition(conjunct, conditions, conditionsNoLiteral);
        }
    }

    private static void addCondition(Expression conjunct,
            Set<String> conditions, Set<String> conditionsNoLiteral) {
        StringBuilder withLiteral = new StringBuilder();
        StringBuilder noLiteral = new StringBuilder();
        appendNormalizedExpression(conjunct, withLiteral, noLiteral);
        conditions.add(withLiteral.toString());
        conditionsNoLiteral.add(noLiteral.toString());
    }

    /**
     * Encode a literal value so that it can not be read as the structure of the canonical form. A
     * value carrying a parenthesis or a separator (e.g. {@code where s = 'a)b'}) would otherwise
     * close its own {@code lit(...)} and could forge a conjunct next to it, which would let two
     * different predicates share one fingerprint; an unbalanced parenthesis would also break the
     * rendering of the simplified struct info. The reserved characters are percent encoded, so the
     * encoding is the identity for the values which carry none of them (numbers, plain strings);
     * {@link #decodeLiteralValue} reverses it for display.
     */
    public static String encodeLiteralValue(String value) {
        StringBuilder sb = null;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (RESERVED_LITERAL_CHARS.indexOf(c) < 0) {
                if (sb != null) {
                    sb.append(c);
                }
                continue;
            }
            if (sb == null) {
                sb = new StringBuilder(value.length() + 8);
                sb.append(value, 0, i);
            }
            sb.append('%').append(HEX_DIGITS[(c >> 4) & 0xF]).append(HEX_DIGITS[c & 0xF]);
        }
        return sb == null ? value : sb.toString();
    }

    /** Reverse of {@link #encodeLiteralValue}: turn an encoded literal value back into its text. */
    public static String decodeLiteralValue(String value) {
        int start = value.indexOf('%');
        if (start < 0) {
            return value;
        }
        StringBuilder sb = new StringBuilder(value.length());
        sb.append(value, 0, start);
        for (int i = start; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '%' && i + 2 < value.length()
                    && Character.digit(value.charAt(i + 1), 16) >= 0
                    && Character.digit(value.charAt(i + 2), 16) >= 0) {
                sb.append((char) ((Character.digit(value.charAt(i + 1), 16) << 4)
                        | Character.digit(value.charAt(i + 2), 16)));
                i += 2;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** The start of the next protected header at or after {@code from}, or -1. */
    private static int nextProtectedHeaderStart(String text, int from) {
        int best = -1;
        for (int i = 0; i < PROTECTED_HEADERS.length(); i++) {
            int at = text.indexOf(PROTECTED_HEADERS.charAt(i) + "{", from);
            if (at >= 0 && (best < 0 || at < best)) {
                best = at;
            }
        }
        return best;
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
     * in a value is encoded by the traversal (see {@code encodeLiteralValue}), so it cannot be read as
     * the structure of the canonical form. The headers whose arguments the traversal writes in both
     * literal modes (a generator, a top-n, a limit, a window and a set operation) are copied verbatim,
     * because folding an argument which is part of such an identity would make the sha256 of the pasted
     * struct info differ from the fingerprint EXPLAIN printed. A text whose parenthesis never closes is
     * reported instead of being folded into a truncated string.
     */
    public static String toNoLiteral(String canonicalString) {
        StringBuilder sb = new StringBuilder(canonicalString.length());
        int index = 0;
        while (index < canonicalString.length()) {
            int start = canonicalString.indexOf("lit(", index);
            int protectedStart = nextProtectedHeaderStart(canonicalString, index);
            if (protectedStart >= 0 && (start < 0 || protectedStart < start)) {
                // the arguments of these headers are written in *both* literal modes (a generator's
                // delimiter decides how many rows a row expands into, a top-n's ordering and a window's
                // definition decide which rows reach the node above, a set-operation constant and a limit
                // bound decide the rows of the operation), so folding them here would make the sha256 of
                // the pasted struct info differ from the fingerprint EXPLAIN printed
                int end = canonicalString.indexOf('}', protectedStart);
                if (end < 0) {
                    throw new IllegalArgumentException("an unbalanced '" + canonicalString.charAt(protectedStart)
                            + "{' at position " + protectedStart);
                }
                sb.append(canonicalString, index, end + 1);
                index = end + 1;
                continue;
            }
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
            if (depth != 0) {
                // the parenthesis of `lit(` is never closed: folding would drop the rest of the text, the
                // fingerprint of a pasted struct info would match nothing, and the statement would be
                // rejected with a message about the fingerprint although the text itself is what is wrong
                throw new IllegalArgumentException("an unbalanced parenthesis after 'lit(' at position "
                        + start);
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

    /**
     * Normalize an expression into its canonical component in the given literal mode: a literal keeps
     * its value, or becomes {@code lit(*)} when the constant agnostic form is asked for.
     */
    public static String normalizeExpression(Expression expression, LiteralMode mode) {
        StringBuilder withLiteral = new StringBuilder();
        StringBuilder noLiteral = new StringBuilder();
        appendNormalizedExpression(expression, withLiteral, noLiteral);
        return mode == LiteralMode.NO_LITERAL ? noLiteral.toString() : withLiteral.toString();
    }

    /**
     * Append both forms of one expression while walking its tree once: the literal carrying form into
     * {@code withLiteral} and the constant agnostic one into {@code noLiteral}. The operands of a
     * commutative operator are sorted per form, because a literal value may order two operands
     * differently than {@code lit(*)} does.
     */
    private static void appendNormalizedExpression(Expression expression,
            StringBuilder withLiteral, StringBuilder noLiteral) {
        if (expression instanceof SlotReference) {
            SlotReference slot = (SlotReference) expression;
            // the identifier text is encoded like a literal value: a name which contains one of the
            // delimiters of the canonical form (`a,S{b}`) would otherwise be read as the start of a scan
            // token by the text based reader of HBO SET STATISTICS, which then truncates the struct info
            // it hashes. The encoding is the identity for an ordinary name, so no key of one moves.
            StringBuilder text = new StringBuilder("col(");
            for (String qualifierPart : slot.getQualifier()) {
                text.append(encodeLiteralValue(qualifierPart)).append('.');
            }
            text.append(encodeLiteralValue(slot.getName()));
            // the sub path of a variant column is part of the identity of the slot: after the variant sub
            // path pruning `v['age']` and `v['score']` are two slots which read different values, so a
            // predicate above them keeps a different number of rows, while the column name alone is the
            // same for both. The parts are encoded like a literal value, so a field name which contains a
            // delimiter cannot be read as the end of a scan token or as the end of a predicate.
            for (String subPathPart : slot.getSubPath()) {
                text.append("['").append(encodeLiteralValue(subPathPart)).append("']");
            }
            text.append(')');
            String rendered = text.toString();
            withLiteral.append(rendered);
            noLiteral.append(rendered);
        } else if (expression instanceof Literal) {
            Literal literal = (Literal) expression;
            withLiteral.append("lit(").append(encodeLiteralValue(String.valueOf(literal.getValue())))
                    .append(':').append(literal.getDataType()).append(')');
            noLiteral.append("lit(*)");
        } else if (expression.children().isEmpty()) {
            String name = expression.getClass().getSimpleName();
            withLiteral.append(name);
            noLiteral.append(name);
        } else {
            List<String> withLiteralChildren = new ArrayList<>();
            List<String> noLiteralChildren = new ArrayList<>();
            for (Expression child : expression.children()) {
                StringBuilder childWithLiteral = new StringBuilder();
                StringBuilder childNoLiteral = new StringBuilder();
                appendNormalizedExpression(child, childWithLiteral, childNoLiteral);
                withLiteralChildren.add(childWithLiteral.toString());
                noLiteralChildren.add(childNoLiteral.toString());
            }
            if (isOrderInsensitive(expression)) {
                // only order-insensitive operators (equal-to / null safe equal-to / and / or) may have
                // their operands sorted; ordered comparisons (e.g. '<') must keep the original order,
                // otherwise `col < 5` and `5 < col` would collapse to the same descriptor (review B2)
                withLiteralChildren.sort(String::compareTo);
                noLiteralChildren.sort(String::compareTo);
            }
            String name = expression.getClass().getSimpleName();
            withLiteral.append(name).append('(').append(String.join(",", withLiteralChildren)).append(')');
            noLiteral.append(name).append('(').append(String.join(",", noLiteralChildren)).append(')');
        }
    }

    /**
     * Equal-to (including its null safe form) and boolean and/or are commutative; everything else
     * keeps its operand order.
     */
    private static boolean isOrderInsensitive(Expression expression) {
        return expression instanceof EqualTo
                || expression instanceof NullSafeEqual
                || expression instanceof Or
                || expression instanceof And;
    }

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
        // the summary counts what the description renders, so it uses the same expression: for a group
        // without a logical plan (see getRepresentativeExpression) that is the physical one it holds
        GroupExpression ge = getRepresentativeExpression(group);
        if (ge == null) {
            return new HboStructSummary(0, Collections.emptyList());
        }
        Plan plan = ge.getPlan();
        List<String> relations = new ArrayList<>();
        int scanCount = 0;
        if (plan instanceof OlapScan) {
            scanCount = 1;
            relations.add(HboScanDescriptor.qualifiedNameOf(((OlapScan) plan).getTable()));
        } else if (plan instanceof LogicalFileScan || plan instanceof PhysicalFileScan) {
            // an external table is described by the token of its table (see appendFileScan), so it is a
            // relation of the summary as well: with an empty relation list the prefilter of the pinned
            // entries rejects an entry of such a filter before its fingerprint is ever looked up
            scanCount = 1;
            TableIf fileTable = plan instanceof LogicalFileScan
                    ? ((LogicalFileScan) plan).getTable()
                    : ((PhysicalFileScan) plan).getTable();
            relations.add(HboScanDescriptor.qualifiedNameOf(fileTable));
        }
        // the summary counts what the description renders: an anchor describes its consumer side, which
        // is its second child (the first one is the producer, and counting it would report scans and
        // relations the canonical form of that group does not contain)
        boolean anchor = plan instanceof LogicalCTEAnchor || plan instanceof PhysicalCTEAnchor;
        for (int i = 0; i < ge.arity(); i++) {
            if (anchor && i != 1) {
                continue;
            }
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

    /** The number of groups described since the last reset. For tests only. */
    @VisibleForTesting
    public static int getDescribedGroupCount() {
        return describedGroupCount.get();
    }

    /** Reset the description counter. For tests only. */
    @VisibleForTesting
    public static void resetDescribedGroupCount() {
        describedGroupCount.set(0);
    }

    /** The number of descriptions walked since the last reset. For tests only. */
    @VisibleForTesting
    public static int getMaterializedGroupCount() {
        return materializedGroupCount.get();
    }

    /** Reset the materialization counter. For tests only. */
    @VisibleForTesting
    public static void resetMaterializedGroupCount() {
        materializedGroupCount.set(0);
    }

    /**
     * A storage layer aggregate reads the row count of its relation directly and the read side applies a
     * pinned or a learned row count at filter, join and aggregation nodes only - never at this wrapper
     * (the translator delegates it to the relation it embeds, and the aggregation above it looks up its
     * own logical key). An advertised key of such a node is accepted and stored by
     * {@code HBO SET STATISTICS} but can never be applied, so the node is neither advertised nor
     * published: the applicable key is the one of the aggregation above it.
     */
    private static boolean hasNoHboLookupPath(AbstractPlan planNode) {
        return planNode instanceof PhysicalStorageLayerAggregate;
    }

    /**
     * Why the struct info of the group of {@code planNode} is missing, when the reason is known: the
     * group is too wide for {@code hbo_max_scans_per_group}, it contains an operator the canonical form
     * does not describe, its table name carries a struct info delimiter, the data state of one of its
     * scans cannot be read, ... Empty only when the description is valid, or when the group of the node
     * is not reachable at all.
     */
    public static Optional<String> skipReasonOfPlanNode(AbstractPlan planNode,
            Map<Integer, Group> groupsById) {
        if (hasNoHboLookupPath(planNode)) {
            return Optional.of(NO_LOOKUP_PATH_REASON);
        }
        Group group = resolveGroup(planNode, groupsById);
        if (group == null) {
            return Optional.empty();
        }
        GroupStructInfo structInfo = group.getOrComputeHboStructInfo();
        if (structInfo.isValid() || structInfo.getSkipReason().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(structInfo.getSkipReason());
    }

    /**
     * Why the group of a plan node could not be resolved, or null when it could: a node of the final
     * plan normally carries the group it was chosen for (a back reference, or the group id state a post
     * processor propagates), and a node which carries neither cannot be looked up at all.
     */
    public static String groupResolutionFailureOf(AbstractPlan planNode, Map<Integer, Group> groupsById) {
        if (planNode.getGroupExpression().isPresent()) {
            return null;
        }
        Optional<Object> groupState = planNode.getMutableState(MutableState.KEY_GROUP);
        boolean known = false;
        if (groupState.isPresent() && groupsById != null) {
            try {
                known = groupsById.containsKey(Integer.valueOf(groupState.get().toString()));
            } catch (NumberFormatException ignored) {
                // an empty id is written by a rebuild which copied it from a node without a group
                known = false;
            }
        }
        if (known) {
            return null;
        }
        return "no group for " + planNode.getClass().getSimpleName()
                + (groupState.isPresent() ? " (group id '" + groupState.get() + "' is unknown)" : "");
    }

    /** The relation key of the group of a plan node, or null when that group is not reachable. */
    public static String relationKeyOfPlanNode(AbstractPlan planNode, Map<Integer, Group> groupsById) {
        Group group = resolveGroup(planNode, groupsById);
        return group == null ? null : group.getOrComputeHboStructSummary().getRelationKey();
    }

    /** The state of one build: it describes the sub tree of the group it was started at. */
    private static class Ctx {
        private boolean valid = true;
        /** why the description was refused: the first reason wins, it is shown by EXPLAIN */
        private String reason = "";
        private final Set<Group> visited = new HashSet<>();
    }

    /**
     * The rope of one group: a text fragment (its two literal forms) or a reference to the
     * description of a child. Materializing a form walks the pieces and copies the strings, it never
     * parses them, and it takes the materialized string of a child when that child already has it.
     */
    private interface Piece {
        void appendTo(StringBuilder out, LiteralMode mode, boolean shape);
    }

    /** A fragment of text: the same structure in both forms, the literals differ per mode. */
    private static final class Text implements Piece {
        private final String withLiteral;
        private final String noLiteral;

        Text(String withLiteral, String noLiteral) {
            this.withLiteral = withLiteral;
            this.noLiteral = noLiteral;
        }

        String getWithLiteral() {
            return withLiteral;
        }

        String getNoLiteral() {
            return noLiteral;
        }

        @Override
        public void appendTo(StringBuilder out, LiteralMode mode, boolean shape) {
            out.append(mode == LiteralMode.NO_LITERAL ? noLiteral : withLiteral);
        }
    }

    /** The scan token of a scan group: the only piece which differs between the two forms. */
    private static final class ScanToken implements Piece {
        private final HboScanDescriptor descriptor;

        ScanToken(HboScanDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        HboScanDescriptor getDescriptor() {
            return descriptor;
        }

        @Override
        public void appendTo(StringBuilder out, LiteralMode mode, boolean shape) {
            // the identity of a scan is the table plus the partitions it selects: two explicit
            // partition selections of one table read different data and must not share a key
            out.append("S{").append(shape ? descriptor.getShapeIdentity() : descriptor.render()).append('}');
        }
    }

    /** A child sub tree. */
    private static final class Child implements Piece {
        private final GroupStructInfo child;
        private final boolean repeated;

        Child(GroupStructInfo child, boolean repeated) {
            this.child = child;
            this.repeated = repeated;
        }

        GroupStructInfo getChild() {
            return child;
        }

        @Override
        public void appendTo(StringBuilder out, LiteralMode mode, boolean shape) {
            String materialized = child.materialized(mode, shape);
            if (materialized != null) {
                // the child is already materialized in this form: copy it instead of walking its own
                // pieces again (and therefore without touching its grand children)
                out.append(materialized);
                return;
            }
            child.appendTo(out, mode, shape);
        }
    }

    /**
     * The rope a build assembles: the pieces of one group, plus the scans of its whole sub tree in
     * the order their tokens appear (a caller which compares them with the scans of another struct
     * info has to see the same order the printed struct info has, which is not always the build order
     * because a join chain prints its leaves sorted).
     */
    private static class Canonical {
        private final List<Piece> pieces = new ArrayList<>();
        private final List<HboScanDescriptor> scans = new ArrayList<>();

        /** A fragment which is the same in every form (structure, separator). */
        Canonical same(String text) {
            return addText(text, text);
        }

        /** A fragment which differs between the literal modes (a literal carrying expression). */
        Canonical modes(String withLiteral, String noLiteral) {
            return addText(withLiteral, noLiteral);
        }

        /** The scan token of a scan group. */
        Canonical scan(HboScanDescriptor descriptor) {
            pieces.add(new ScanToken(descriptor));
            scans.add(descriptor);
            return this;
        }

        /**
         * Compose the description of a child (or of a join chain leaf): the rope only records the
         * reference, and the scans of the child are inherited in token order.
         */
        Canonical append(GroupStructInfo child) {
            return append(child, false);
        }

        /**
         * Compose a child which is referenced more than once: its canonical form is written again (so
         * that a query which reuses a sub tree and a query which writes it twice share one entry), but
         * its tables are read once, so its scans are inherited by the first reference only.
         */
        Canonical append(GroupStructInfo child, boolean repeated) {
            if (child.isValid()) {
                pieces.add(new Child(child, repeated));
                if (!repeated) {
                    scans.addAll(child.getScans());
                }
            }
            return this;
        }

        private Canonical addText(String withLiteral, String noLiteral) {
            if (withLiteral.isEmpty() && noLiteral.isEmpty()) {
                return this;
            }
            Piece last = pieces.isEmpty() ? null : pieces.get(pieces.size() - 1);
            if (last instanceof Text) {
                // merge neighbouring text, so that a layer of structure does not become a long list
                // of one character pieces
                Text previous = (Text) last;
                pieces.set(pieces.size() - 1, new Text(previous.getWithLiteral() + withLiteral,
                        previous.getNoLiteral() + noLiteral));
                return this;
            }
            pieces.add(new Text(withLiteral, noLiteral));
            return this;
        }

        List<Piece> getPieces() {
            return pieces;
        }

        List<HboScanDescriptor> getScans() {
            return scans;
        }
    }
}
