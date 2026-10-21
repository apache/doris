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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
 *
 * <p>This description is always built by the FE from the plan it is looking at, and every string it can
 * produce is one of its renderings: {@link #getCanonical(LiteralMode)} is the text EXPLAIN prints and a
 * user may paste into {@code HBO SET STATISTICS ... STRUCT='...'}, {@link #getShape(LiteralMode)} is the
 * same text without the data state of the scans, and {@link #getFingerprint(LiteralMode)} is the sha256
 * of that shape. Nothing is ever parsed to produce a fingerprint - in particular the text a user pasted
 * is only compared with the fingerprint next to it, never taken as its source.
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

    /**
     * The prefix of the reason a node whose kind the description does not support is reported with, and
     * the two halves of the reason a node with an unsupported number of children is reported with. They
     * are constants so that the corpus can bind the frozen skip texts to the formula which produces them
     * instead of restating the prefix.
     */
    static final String UNSUPPORTED_REASON_PREFIX = "unsupported=";
    static final String ARITY_REASON_PREFIX = "arity=";
    static final String ARITY_REASON_MIDDLE = " of ";

    private static final List<HboScanDescriptor> INVALID_SCANS = Collections.emptyList();

    /**
     * Characters which structure the canonical form and therefore can not appear raw inside a
     * literal value (see {@link #encodeLiteralValue}).
     */
    private static final String RESERVED_LITERAL_CHARS = "\u0000\\%(){}[],;:|";

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
    public static final GroupStructInfo INVALID = new GroupStructInfo(false, "", null, INVALID_SCANS);

    private static final String SEP = ";";

    /**
     * The header letters whose arguments are part of the identity of their node (see
     * {@link Kind#literalsAreIdentity()}): the fold of a pasted struct info copies those headers verbatim,
     * because folding one of their literals would make the sha256 of the text differ from the fingerprint
     * EXPLAIN printed. The set is derived from the kinds which render the text, so a kind whose literals
     * stop being identity cannot be forgotten here.
     */
    private static final String PROTECTED_HEADERS = protectedHeaders();

    /**
     * The markers of the headers whose condition collection is sorted per literal mode, i.e. the ones the
     * fold of a pasted struct info has to re-sort after it folded their literals (see
     * {@code recanonicalizeFoldedConditions}): a filter, an aggregation, a join (which also de-duplicates)
     * and the grouping sets of a repeat.
     */
    private static final String[] CONDITION_HEADER_MARKERS = {"F{", "A{gb:", "J{", "R{gs:["};

    /**
     * The functions whose value depends on the session time zone although neither their arguments nor
     * their result are a date: an epoch value is read in that zone ({@code hour_from_unixtime(t.ts) = 0}
     * keeps 1000 rows in one session and 1 row in another although its argument is the same integer).
     * A function whose argument or result is a date or a datetime is covered by its type instead (see
     * {@link #usesSessionTimeZone}).
     */
    private static final List<String> EPOCH_TIME_ZONE_FUNCTIONS = Arrays.asList(
            "hour_from_unixtime", "minute_from_unixtime", "second_from_unixtime",
            "microsecond_from_unixtime", "unix_timestamp");

    private static final String NO_LOOKUP_PATH_REASON =
            "storage layer aggregate: the read side applies the row count of the aggregation above it";

    private static final String PARTIAL_STAGE_REASON =
            "local (partial) aggregation stage: the read side applies the row count of the final"
                    + " aggregation above it";

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
     * The tree of this description: the information of its own group and the descriptions of the groups
     * below it, all as objects. Nothing is rendered while it is built - {@link #render} is the one place
     * which knows the grammar - and every node remembers the forms it has rendered, so a parent which is
     * rendered later composes the text of a child instead of walking its sub tree again.
     */
    private final Node root;
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

    private GroupStructInfo(boolean valid, String skipReason, Node root,
            List<HboScanDescriptor> scans) {
        this.valid = valid;
        this.skipReason = skipReason;
        this.root = root;
        this.scans = scans;
    }

    private static GroupStructInfo ofNode(Node root) {
        List<HboScanDescriptor> scans = new ArrayList<>();
        collectScans(root, scans);
        return new GroupStructInfo(true, "", root, Collections.unmodifiableList(scans));
    }

    private static GroupStructInfo invalidStructInfo(String skipReason) {
        return skipReason.isEmpty()
                ? INVALID
                : new GroupStructInfo(false, skipReason, null, INVALID_SCANS);
    }

    private static String protectedHeaders() {
        StringBuilder letters = new StringBuilder();
        for (Kind kind : Kind.values()) {
            if (kind.literalsAreIdentity()) {
                letters.append(kind.headerLetters());
            }
        }
        return letters.toString();
    }

    /** The reason the struct info was refused by a limit, or empty. Only for diagnostics. */
    public String getSkipReason() {
        return skipReason;
    }

    public boolean isValid() {
        return valid;
    }

    /** The node this description is made of; null when the description is invalid. */
    private Node root() {
        return root;
    }

    /**
     * The row count and partition selection of every scan of the sub tree, in the order the scan tokens
     * appear in the canonical string (i.e. the order a user sees when pasting the struct info). A sub
     * tree which the canonical string writes twice - a child which is referenced more than once - keeps
     * its token in the text twice while its tables are read once, so its descriptors appear here once;
     * a caller which compares this list with the tokens of a text has to expect that difference (the
     * learned freshness comparison folds the tokens of a text per table for the same reason). Empty when
     * the struct info is invalid.
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
        materialized = root == null ? "" : render(root, mode, false);
        if (noLiteral) {
            canonicalNoLiteral = materialized;
        } else {
            canonicalWithLiteral = materialized;
        }
        return materialized;
    }

    /**
     * The fingerprint input of one literal mode: the canonical string with the data state of every scan
     * removed ({@code S{db.t}}). It is rendered from the same tree as the canonical string, with the scan
     * tokens reduced to their identity, and never parsed out of the text, so no text inside a table name
     * or a literal can be mistaken for a scan token.
     */
    public String getShape(LiteralMode mode) {
        boolean noLiteral = mode == LiteralMode.NO_LITERAL;
        String materialized = noLiteral ? shapeNoLiteral : shapeWithLiteral;
        if (materialized != null) {
            return materialized;
        }
        materialized = root == null ? "" : render(root, mode, true);
        if (noLiteral) {
            shapeNoLiteral = materialized;
        } else {
            shapeWithLiteral = materialized;
        }
        return materialized;
    }

    /**
     * The sha256 of {@link #shapeKey(LiteralMode)}: the key an hbo entry is registered under.
     *
     * <p>The preimage is the structural form of this description, not the canonical string: every field
     * is written with its length, so a table name, an alias or a literal which contains a character of
     * the printed grammar cannot change which field it belongs to. The canonical string is the readable
     * projection of the same tree ({@link #getCanonical}), and a pasted struct info is never hashed.
     */
    public String getFingerprint(LiteralMode mode) {
        boolean noLiteral = mode == LiteralMode.NO_LITERAL;
        String materialized = noLiteral ? fingerprintNoLiteral : fingerprintWithLiteral;
        if (materialized != null) {
            return materialized;
        }
        materialized = sha256(shapeKey(mode));
        if (noLiteral) {
            fingerprintNoLiteral = materialized;
        } else {
            fingerprintWithLiteral = materialized;
        }
        return materialized;
    }

    private static String sha256(byte[] value) {
        return Hashing.sha256().hashBytes(value).toString();
    }

    /** The structural identity of this description in one literal mode (see {@link #getFingerprint}). */
    private byte[] shapeKey(LiteralMode mode) {
        ShapeKeyWriter out = new ShapeKeyWriter();
        if (root == null) {
            // an invalid description has no tree: its key is a marker of its own
            out.tag(-1);
            return out.toByteArray();
        }
        out.tag(0);
        appendShapeKey(out, root, mode);
        return out.toByteArray();
    }

    /**
     * Write one node of the structural identity: the kind, the fields the canonical string prints for it
     * (each as one length prefixed text) and the children. A field is never split by the text of another
     * one, which the printed grammar cannot promise for a value which carries its characters.
     */
    private static void appendShapeKey(ShapeKeyWriter out, Node node, LiteralMode mode) {
        if (node.kind == Kind.PASSTHROUGH && node.children.size() == 1) {
            // a node which only composes its child renders no text of its own, so a plan which keeps such
            // a node and the same plan with it inlined are one description: the identity must not name
            // the node, otherwise the two forms of one plan would be keyed apart
            appendShapeKey(out, node.children.get(0).node, mode);
            return;
        }
        out.tag(node.kind.ordinal());
        switch (node.kind) {
            case SCAN:
            case FILE_SCAN:
                appendScanKey(out, node.scan);
                return;
            case STORAGE_AGGREGATE:
                out.text(String.valueOf(node.storageAggregate.getAggOp()));
                out.flag(node.scan != null);
                if (node.scan != null) {
                    appendScanKey(out, node.scan);
                }
                return;
            case FILTER:
                out.text(sortedConditions(node.conditions, mode));
                break;
            case AGGREGATE:
                out.text(sortedIdentityConditions(node.conditions));
                break;
            case JOIN:
                out.text(node.flattenable ? "inner" : String.valueOf(node.joinType));
                out.flag(node.markJoin);
                out.text(distinctIdentityConditions(node.conditions));
                break;
            case TOPN: {
                StringBuilder header = new StringBuilder();
                appendTopNHeader(header, node, mode);
                out.text(header.toString());
                break;
            }
            case LIMIT:
                out.text(bound("L", node.limit, node.offset));
                break;
            case WINDOW: {
                StringBuilder header = new StringBuilder();
                appendWindowHeader(header, node);
                out.text(header.toString());
                break;
            }
            case GENERATE: {
                StringBuilder header = new StringBuilder();
                appendGenerateHeader(header, node);
                out.text(header.toString());
                break;
            }
            case REPEAT: {
                StringBuilder header = new StringBuilder();
                appendRepeatHeader(header, node, mode);
                out.text(header.toString());
                break;
            }
            case SET_OPERATION:
                appendSetOperationKey(out, node);
                break;
            case PASSTHROUGH:
                break;
            default:
                throw new IllegalStateException("unknown node kind " + node.kind);
        }
        out.count(node.children.size());
        for (Child child : node.children) {
            // a child which is referenced more than once renders the text of the first reference, so the
            // identity names the sub tree only, not how often the plan references it
            out.count(child.mapping.size());
            for (Expression mapped : child.mapping) {
                out.text(renderExpression(mapped, mode));
            }
            appendShapeKey(out, child.node, mode);
        }
    }

    /**
     * The identity of a scan, one field at a time: a table name which carries the characters of the
     * printed token ({@code #ti:}, a comma) cannot stand for another field because each field has its own
     * length here.
     */
    private static void appendScanKey(ShapeKeyWriter out, HboScanDescriptor descriptor) {
        out.text(descriptor.getTable());
        out.number(descriptor.getTableId());
        out.text(descriptor.getRelationIdentity());
        out.text(descriptor.getPartitionIdentity());
        out.text(descriptor.getTabletIdentity());
        out.text(descriptor.getIndexIdentity());
    }

    /** The header fields of a set operation, which decide which rows its branches combine. */
    private static void appendSetOperationKey(ShapeKeyWriter out, Node node) {
        SetOperation setOperation = node.setOperation;
        out.text(node.setOperationPlan instanceof Union ? "U"
                : node.setOperationPlan instanceof Intersect ? "I" : "E");
        out.text(String.valueOf(setOperation.getQualifier()).toLowerCase(Locale.ROOT));
        out.count(setOperation.getOutputs().size());
        for (NamedExpression output : setOperation.getOutputs()) {
            out.text(identityText(output));
        }
        List<List<NamedExpression>> constantExprsList = setOperation instanceof Union
                ? ((Union) setOperation).getConstantExprsList() : Collections.emptyList();
        out.count(constantExprsList.size());
        for (List<NamedExpression> branch : constantExprsList) {
            out.count(branch.size());
            for (NamedExpression constant : branch) {
                out.text(identityText(constant));
            }
        }
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
        Group group = planNode.getGroupExpression().map(GroupExpression::getOwnerGroup).orElse(null);
        if (hasNoHboLookupPath(planNode, group)) {
            return Optional.empty();
        }
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
        Group group = resolveGroup(planNode, groupsById);
        if (hasNoHboLookupPath(planNode, group)) {
            return Optional.empty();
        }
        return fingerprintOfGroup(group, mode);
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
        Group group = resolveGroup(planNode, groupsById);
        if (hasNoHboLookupPath(planNode, group)) {
            // the annotation asks for the struct info first: without this guard the node would advertise
            // a key which no read path ever looks up (see hasNoHboLookupPath)
            return Optional.empty();
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
        Node root;
        try {
            root = buildNode(ge.getPlan(), ge, ctx);
            if (root != null) {
                applyColumnDefinitions(ge.getPlan(), root);
            }
        } catch (RuntimeException e) {
            // memo content which the simplified struct info cannot describe (an unexpected plan node, a
            // cast of a named expression which is not the alias of a window expression): treat the group as
            // invalid and fall back to the legacy behavior instead of failing the optimizer on the hot
            // path. Rendering cannot throw for a description which was built: it only concatenates.
            LOG.debug("failed to compute hbo struct info for group {}", group.getGroupId(), e);
            return invalidStructInfo(ctx.reason);
        }
        if (!ctx.valid || root == null) {
            return invalidStructInfo(ctx.reason);
        }
        return ofNode(root);
    }

    /**
     * Bind the columns the children of a node compute to the expression which produces them, and
     * substitute a reference to such a column in the expressions of this node.
     *
     * <p>A project is transparent for structure matching and an aggregation keeps only its grouping
     * keys in its header, so a column whose value they compute is <em>named</em> by every node above
     * them and <em>defined</em> by none: {@code Aggregate(group by x) -> Project(a % 2 AS x) ->
     * Scan(t)} and the same plan with {@code % 3} render one {@code A{gb:col(x)}} although their group
     * counts differ, and a pinned row count of one is then applied to the other. The definition is
     * therefore carried up: the expressions of this node are rewritten so that a column defined by a
     * child renders as the expression which computes it, and the map is handed to the nodes above.
     */
    private static void applyColumnDefinitions(Plan plan, Node node) {
        Map<ExprId, Expression> childDefinitions = childColumnDefinitions(node);
        resolveNodeExpressions(node, childDefinitions);
        if (plan instanceof Project) {
            node.columnDefinitions = projectColumnDefinitions((Project) plan, childDefinitions);
        } else if (plan instanceof Aggregate) {
            node.columnDefinitions = aggregateColumnDefinitions((Aggregate<?>) plan, childDefinitions);
        } else {
            node.columnDefinitions = childDefinitions;
        }
    }

    /**
     * The columns an aggregation computes. Its header keeps the grouping keys - the number of groups,
     * which is the row count of the node, depends on them alone - so the values its aggregate
     * functions produce are defined nowhere: {@code Filter(s = 1) -> Aggregate(group by k, MIN(v) AS
     * s)} and the same tree with {@code MAX(v) AS s} render one filter key although one k group whose
     * values are 1 and 2 keeps zero and one rows. A definition is the aggregate function under the
     * name of the alias which exposes it, resolved against the columns of the child like the header of
     * the aggregation itself.
     */
    private static Map<ExprId, Expression> aggregateColumnDefinitions(Aggregate<?> aggregate,
            Map<ExprId, Expression> childDefinitions) {
        Map<ExprId, Expression> definitions = new LinkedHashMap<>();
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            if (output instanceof Alias) {
                definitions.put(output.getExprId(),
                        resolveColumnDefinitions(((Alias) output).child(), childDefinitions));
            }
        }
        return Collections.unmodifiableMap(definitions);
    }

    /** The definitions the children of a node expose: a join forwards the columns of both sides. */
    private static Map<ExprId, Expression> childColumnDefinitions(Node node) {
        if (node.children.size() == 1) {
            return node.children.get(0).node.columnDefinitions;
        }
        if (node.children.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<ExprId, Expression> definitions = new LinkedHashMap<>();
        for (Child child : node.children) {
            definitions.putAll(child.node.columnDefinitions);
        }
        return Collections.unmodifiableMap(definitions);
    }

    /**
     * The columns a project computes: an alias names the expression it wraps, and that expression is
     * the value a node above the project reads. A plain slot of the projection is a forwarded column,
     * which keeps the definition it already had below the project (a project of plain slots over a
     * project of aliases forwards exactly those definitions).
     */
    private static Map<ExprId, Expression> projectColumnDefinitions(Project project,
            Map<ExprId, Expression> childDefinitions) {
        Map<ExprId, Expression> definitions = new LinkedHashMap<>();
        for (NamedExpression output : project.getProjects()) {
            if (output instanceof Alias) {
                definitions.put(output.getExprId(),
                        resolveColumnDefinitions(((Alias) output).child(), childDefinitions));
            } else if (childDefinitions.containsKey(output.getExprId())) {
                definitions.put(output.getExprId(), childDefinitions.get(output.getExprId()));
            }
        }
        return Collections.unmodifiableMap(definitions);
    }

    /** Substitute every reference to a defined column with the expression which computes it. */
    private static Expression resolveColumnDefinitions(Expression expression,
            Map<ExprId, Expression> definitions) {
        if (definitions.isEmpty()) {
            return expression;
        }
        return expression.rewriteUp(rewritten -> {
            if (!(rewritten instanceof Slot)) {
                return rewritten;
            }
            Expression definition = definitions.get(((Slot) rewritten).getExprId());
            return definition == null ? rewritten : definition;
        });
    }

    private static List<Expression> resolveColumnDefinitions(List<Expression> expressions,
            Map<ExprId, Expression> definitions) {
        if (definitions.isEmpty()) {
            return expressions;
        }
        List<Expression> resolved = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            resolved.add(resolveColumnDefinitions(expression, definitions));
        }
        return resolved;
    }

    /** Rewrite the expressions a node keeps, in place: they are rendered exactly once, later. */
    private static void resolveNodeExpressions(Node node, Map<ExprId, Expression> definitions) {
        if (definitions.isEmpty()) {
            return;
        }
        if (node.conditions != null) {
            node.conditions = resolveColumnDefinitions(node.conditions, definitions);
        }
        if (node.orderKeys != null) {
            List<OrderKey> orderKeys = new ArrayList<>(node.orderKeys.size());
            for (OrderKey orderKey : node.orderKeys) {
                orderKeys.add(orderKey.withExpression(
                        resolveColumnDefinitions(orderKey.getExpr(), definitions)));
            }
            node.orderKeys = orderKeys;
        }
        if (node.windowExpressions != null) {
            List<NamedExpression> windowExpressions = new ArrayList<>(node.windowExpressions.size());
            for (NamedExpression windowExpression : node.windowExpressions) {
                windowExpressions.add((NamedExpression)
                        resolveColumnDefinitions(windowExpression, definitions));
            }
            node.windowExpressions = windowExpressions;
        }
        if (node.generators != null) {
            node.generators = resolveColumnDefinitions(node.generators, definitions);
        }
        if (node.generatorConjuncts != null) {
            node.generatorConjuncts = resolveColumnDefinitions(node.generatorConjuncts, definitions);
        }
        if (node.groupingSets != null) {
            List<List<Expression>> groupingSets = new ArrayList<>(node.groupingSets.size());
            for (List<Expression> groupingSet : node.groupingSets) {
                groupingSets.add(resolveColumnDefinitions(groupingSet, definitions));
            }
            node.groupingSets = groupingSets;
        }
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

    /**
     * Step 1 of a node build: describe one child of this node. The child is described once per query -
     * its memoized description is returned - and a child which cannot be described invalidates this
     * node with the reason of the child, so that the first reason of the failing sub tree is the one
     * which is reported.
     */
    private static GroupStructInfo describeChild(GroupExpression ge, int index, Ctx ctx) {
        if (!ctx.valid) {
            return null;
        }
        GroupStructInfo child = describeGroup(ge.child(index), ctx);
        if (!child.isValid()) {
            invalid(ctx, child.getSkipReason());
            return null;
        }
        return child;
    }

    /**
     * The conditions and the leaves of a join chain, memoized per group: a chain of n joins used to walk
     * its own sub tree once per member (O(n^2) condition renderings for the whole query), while the
     * structure of a chain does not depend on the build which reads it - only the bookkeeping of a sub
     * tree which is reached more than once does, and that is done while composing the chain.
     */
    public static final class ChainInfo {
        /** the conditions of the chain, de-duplicated by their literal carrying text */
        private final List<Expression> conditions;
        /** the groups the chain is made of, without its leaves: they detect a chain which is reused */
        private final List<Group> members;
        /** the groups which are not flattened into the chain, in traversal order */
        private final List<Group> leaves;
        private final boolean valid;
        private final String skipReason;

        private ChainInfo(List<Expression> conditions, List<Group> members, List<Group> leaves,
                boolean valid, String skipReason) {
            this.conditions = conditions;
            this.members = members;
            this.leaves = leaves;
            this.valid = valid;
            this.skipReason = skipReason;
        }

        private static ChainInfo of(List<Expression> conditions, List<Group> members, List<Group> leaves) {
            return new ChainInfo(conditions, members, leaves, true, "");
        }

        private static ChainInfo failed(String skipReason) {
            return new ChainInfo(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), false, skipReason);
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
            List<Expression> conditions = joinConditions((Join) plan);
            List<Group> members = new ArrayList<>();
            List<Group> leaves = new ArrayList<>();
            members.add(group);
            for (int i = 0; i < ge.arity(); i++) {
                ChainInfo child = chainInfoOf(ge.child(i));
                if (!child.valid) {
                    return ChainInfo.failed(child.skipReason);
                }
                mergeConditions(conditions, child.conditions);
                members.addAll(child.members);
                leaves.addAll(child.leaves);
            }
            return ChainInfo.of(conditions, members, leaves);
        }
        // a top-n must not be flattened into the chain although it is a sort: it keeps a bounded number
        // of rows, so it is a boundary of the chain like a limit, and the ordering it keeps them by is
        // part of the identity of every node above it. A window is a boundary as well: it keeps every
        // row, but the values of the columns it computes depend on its definition, and a node above it
        // reads those values.
        // a top-n is a sort but must not be flattened (it keeps a bounded number of rows, so it is a
        // boundary of the chain), and a window is neither a project nor a sort, so it reaches the leaf
        // branch below without a negation which a cleanup could drop
        // a project is transparent only while it forwards its columns: one which computes a value is a
        // leaf, so that its definitions reach the conditions of the join above it (a flattened project
        // would be skipped and its definitions would be lost)
        if ((plan instanceof Sort && !(plan instanceof TopN) || isTransparentProject(plan)) && ge.arity() == 1) {
            ChainInfo child = chainInfoOf(ge.child(0));
            if (!child.valid) {
                return child;
            }
            List<Group> members = new ArrayList<>();
            members.add(group);
            members.addAll(child.members);
            return ChainInfo.of(child.conditions, members, child.leaves);
        }
        List<Group> leaves = new ArrayList<>();
        leaves.add(group);
        return ChainInfo.of(new ArrayList<>(), new ArrayList<>(), leaves);
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

        /** The key the leaves of a chain are ordered by: the identity of the sub tree, no data state. */
        String sortKey() {
            return leaf.getShape(LiteralMode.WITH_LITERAL);
        }
    }

    /**
     * The conditions of a join as expressions, de-duplicated by their literal carrying text - the key the
     * two per-mode string sets used. The rendering happens once per mode in
     * {@link #conditions(List, LiteralMode)}, which sorts and de-duplicates each mode on its own, exactly
     * like the two {@link TreeSet}s did.
     */
    private static List<Expression> joinConditions(Join join) {
        List<Expression> conditions = new ArrayList<>();
        addConditions(conditions, join.getHashJoinConjuncts());
        addConditions(conditions, join.getOtherJoinConjuncts());
        // a mark join carries its correlation condition in the mark conjuncts, and the translator turns
        // them into the hash condition of the executed null aware join: they decide which rows the node
        // keeps, so two mark joins which differ only in that condition must not share a key
        addConditions(conditions, join.getMarkJoinConjuncts());
        return conditions;
    }

    private static void addConditions(List<Expression> conditions, Collection<Expression> conjuncts) {
        mergeConditions(conditions, new ArrayList<>(conjuncts));
    }

    /** Append the conditions which are not in {@code into} yet, keyed by their literal carrying text. */
    private static void mergeConditions(List<Expression> into, List<Expression> from) {
        Set<String> seen = new HashSet<>();
        for (Expression condition : into) {
            seen.add(identityText(condition));
        }
        for (Expression condition : from) {
            if (seen.add(identityText(condition))) {
                into.add(condition);
            }
        }
    }

    /** Inner and cross joins may be reordered, reassociated and therefore flattened. */
    private static boolean isFlattenableJoinType(JoinType joinType) {
        return joinType == JoinType.INNER_JOIN || joinType == JoinType.CROSS_JOIN;
    }

    // ------------------------------------------------------------------------------------------
    // the description of one group: a tree of nodes, and the one renderer of the grammar
    // ------------------------------------------------------------------------------------------

    /** What a node of a description is; {@link #appendNode} switches on it. */
    private enum Kind {
        SCAN("", false),
        FILE_SCAN("", false),
        STORAGE_AGGREGATE("A", false),
        FILTER("F", false),
        // a grouping key can name a definition of the sub tree (the output of a computed project), and the
        // literals of that definition decide the number of groups: folding them would let the learned count
        // of `GROUP BY a % 2` (two groups) be reused for `GROUP BY a % 1000` (a thousand groups)
        AGGREGATE("A", true),
        // a join's residual conditions decide which rows the join keeps (a range bound, a function of both
        // sides), so their literals are identity: the published and learned keys of a join must not fold
        // `t.b > u.b + 1000` and `t.b > u.b + 1` into one entry
        JOIN("J", true),
        TOPN("T", true),
        LIMIT("L", true),
        WINDOW("W", true),
        GENERATE("G", true),
        REPEAT("R", false),
        SET_OPERATION("UIE", true),
        PASSTHROUGH("", false);

        /** The letters the renderer writes in front of the header of this kind (a set operation has three). */
        private final String headerLetters;
        /**
         * Whether the literals inside the header of this kind are part of its identity, which means the
         * renderer writes them in both literal modes: a generator's delimiter decides how many rows a row
         * expands into, a top-n's ordering and a window's definition decide which rows reach the node
         * above, a set operation's constant and a limit bound decide the rows of the operation itself.
         */
        private final boolean literalsAreIdentity;

        Kind(String headerLetters, boolean literalsAreIdentity) {
            this.headerLetters = headerLetters;
            this.literalsAreIdentity = literalsAreIdentity;
        }

        private String headerLetters() {
            return headerLetters;
        }

        private boolean literalsAreIdentity() {
            return literalsAreIdentity;
        }
    }

    /**
     * One node of a description: the information of the group itself, held as objects (an expression
     * list, a bound, an ordering, a scan descriptor, ...), plus the children it composes. Nothing is
     * rendered while a node is built: {@link #appendNode} is the only place which knows the grammar, and
     * it renders the literals in the mode the caller asks for, which is why a node has no "with literal"
     * and "no literal" variant of its own.
     */
    private static final class Node {
        private final Kind kind;
        /** FILTER / AGGREGATE / JOIN: the predicates, the grouping keys or the conjuncts. */
        private List<Expression> conditions;
        /** JOIN: a flattened inner / cross join chain is rendered as `inner`, like a cross join. */
        private boolean flattenable;
        /**
         * JOIN: a mark join preserves every row of its left side and produces a marker column, while the
         * same join type without the marker filters the unmatched rows: the two plans keep a different
         * number of rows with the same conditions, so the flag is part of the identity.
         */
        private boolean markJoin;
        /** JOIN: the type of a join which is not flattened. */
        private JoinType joinType;
        /** TOPN / LIMIT: the bound. */
        private long limit;
        private long offset;
        /** TOPN: the ordering which decides which rows survive. */
        private List<OrderKey> orderKeys;
        /** WINDOW: the named expressions of the window (each child is the window expression). */
        private List<NamedExpression> windowExpressions;
        /** GENERATE: the table functions, whose arguments decide how many rows come out. */
        private List<Expression> generators;
        /** GENERATE: the aliases of the columns those functions produce, in order. */
        private List<Slot> generatorOutputs;
        /** GENERATE: the lateral ON conditions, which decide which produced rows survive. */
        private List<Expression> generatorConjuncts;
        /** REPEAT: the grouping sets, in the order they were written. */
        private List<List<Expression>> groupingSets;
        /** SET_OPERATION: the plan node and the operation, which know the kind, the qualifier and more. */
        private Plan setOperationPlan;
        private SetOperation setOperation;
        /** SCAN / FILE_SCAN / STORAGE_AGGREGATE: the token of the relation which is read. */
        private HboScanDescriptor scan;
        /** STORAGE_AGGREGATE: the pushed down aggregation. */
        private PhysicalStorageLayerAggregate storageAggregate;
        /**
         * The columns this node or a node below it computes, keyed by the id of the column which
         * exposes the value: an alias of a transparent project and an output of an aggregation are
         * the two definitions a node above them cannot see in the canonical form of its children.
         */
        private Map<ExprId, Expression> columnDefinitions = Collections.emptyMap();
        /** The children, in the order the canonical string prints them. */
        private List<Child> children = new ArrayList<>();
        /** The rendered forms of this node, filled on the first request of each of them. */
        private String canonicalWithLiteral;
        private String canonicalNoLiteral;
        private String shapeWithLiteral;
        private String shapeNoLiteral;

        private Node(Kind kind) {
            this.kind = kind;
        }

        private String materialized(LiteralMode mode, boolean shape) {
            if (shape) {
                return mode == LiteralMode.NO_LITERAL ? shapeNoLiteral : shapeWithLiteral;
            }
            return mode == LiteralMode.NO_LITERAL ? canonicalNoLiteral : canonicalWithLiteral;
        }

        private void setMaterialized(LiteralMode mode, boolean shape, String text) {
            if (shape) {
                if (mode == LiteralMode.NO_LITERAL) {
                    shapeNoLiteral = text;
                } else {
                    shapeWithLiteral = text;
                }
                return;
            }
            if (mode == LiteralMode.NO_LITERAL) {
                canonicalNoLiteral = text;
            } else {
                canonicalWithLiteral = text;
            }
        }
    }

    /**
     * One child of a node: its description, and whether the sub tree is referenced by another child as
     * well - such a child writes its canonical form again (a query which reuses a sub tree and a query
     * which writes it twice share one entry) while its tables are read once, so its scan tokens are
     * inherited by the first reference only. A set operation branch also writes the mapping of its
     * columns, which is per parent and can therefore not live in the shared child node.
     */
    private static final class Child {
        private final Node node;
        private final boolean repeated;
        /** SET_OPERATION: the expressions the branch feeds its outputs with, empty for every other child. */
        private final List<Expression> mapping;

        private Child(Node node, boolean repeated) {
            this(node, repeated, Collections.emptyList());
        }

        private Child(Node node, boolean repeated, List<Expression> mapping) {
            this.node = node;
            this.repeated = repeated;
            this.mapping = mapping;
        }
    }

    // ---------------------------------------------------------------- build (memo level)

    /**
     * Build the description of one node. Every branch follows the same three steps: the node's own
     * information is collected as objects, its children are described by {@link #describeChild} (the
     * memoized description of a child group, never its sub tree again), and both are put into a
     * {@link Node} - the rendering happens later and elsewhere.
     */
    private static Node buildNode(Plan plan, GroupExpression ge, Ctx ctx) {
        if (!ctx.valid) {
            return null;
        }
        // the order of the checks matters: a top-n is a sort and a window is neither a project nor a
        // sort, so the boundaries of a join chain are recognised before the transparent operators
        if (plan instanceof OlapScan) {
            return olapScanNode((OlapScan) plan, ctx);
        }
        if (plan instanceof Filter) {
            return conditionsNode(Kind.FILTER, ((Filter) plan).getConjuncts(), ge, ctx);
        }
        if (plan instanceof LogicalCTEConsumer || plan instanceof PhysicalCTEConsumer) {
            // both consumers are leaves which reference their producer by id, and neither has a sub tree
            // which this description could follow: the producer is reachable only from its anchor, and a
            // group is described bottom-up (DeriveStatsJob, collectHboPlanInfo, the explain annotation),
            // so the consumer side is visited before the anchor and no producer is known there yet. A plan
            // which contains a consumer therefore stays undescribed - the same message for both classes,
            // because the reason is the same.
            return invalidNode("unsupported=cte consumer without a memo edge", ctx);
        }
        if (plan instanceof LogicalCTEProducer || plan instanceof PhysicalCTEProducer) {
            // a producer materializes the sub tree it was given: transparent
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            return passthroughNode(ge, 0, ctx);
        }
        if (plan instanceof LogicalCTEAnchor || plan instanceof PhysicalCTEAnchor) {
            // the value of an anchor is its consumer side: the producer is the first child and the plan
            // which consumes it is the second one (LogicalCTEAnchor.computeOutput is right().getOutput())
            if (ge.arity() < 2) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            return passthroughNode(ge, 1, ctx);
        }
        if (plan instanceof LogicalFileScan) {
            LogicalFileScan fileScan = (LogicalFileScan) plan;
            return fileScanNode(fileScan.getTable(), HboScanDescriptor.relationIdentityOf(fileScan.getTableAlias()),
                    fileScan.getTableSnapshot().isPresent() || fileScan.getScanParams().isPresent()
                            || fileScan.getTableSample().isPresent(), ctx);
        }
        if (plan instanceof PhysicalFileScan) {
            PhysicalFileScan fileScan = (PhysicalFileScan) plan;
            return fileScanNode(fileScan.getTable(), HboScanDescriptor.relationIdentityOf(fileScan.getTableAlias()),
                    fileScan.getTableSnapshot().isPresent() || fileScan.getScanParams().isPresent(), ctx);
        }
        if (plan instanceof PhysicalStorageLayerAggregate) {
            // the optimizer answers an aggregation over a scan from the storage layer itself: the node is
            // a catalog relation without children, and its identity is the aggregation together with the
            // relation it reads. The argument expressions are referenced by expression id there, which is
            // not stable across queries, so they are left out: they do not change the number of rows this
            // node produces (one row, the query has no grouping).
            PhysicalStorageLayerAggregate aggregate = (PhysicalStorageLayerAggregate) plan;
            Node node = new Node(Kind.STORAGE_AGGREGATE);
            node.storageAggregate = aggregate;
            if (aggregate.getRelation() instanceof OlapScan) {
                node.scan = scanDescriptor((OlapScan) aggregate.getRelation(), ctx);
                if (node.scan == null) {
                    return null;
                }
            }
            return node;
        }
        if (plan instanceof TopN) {
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            // a top-n keeps its bound and its ordering out of the folding: the bound is written in both
            // modes so that the constant agnostic form cannot fold two different bounds into one entry,
            // and the ordering is part of the identity because it decides *which* rows survive
            TopN topN = (TopN) plan;
            Node node = new Node(Kind.TOPN);
            node.limit = topN.getLimit();
            node.offset = topN.getOffset();
            node.orderKeys = new ArrayList<>(topN.getOrderKeys());
            node.children = children(ge, ctx, 0);
            return node.children == null ? null : node;
        }
        if (plan instanceof Limit) {
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            Limit limit = (Limit) plan;
            Node node = new Node(Kind.LIMIT);
            node.limit = limit.getLimit();
            node.offset = limit.getOffset();
            node.children = children(ge, ctx, 0);
            return node.children == null ? null : node;
        }
        if (plan instanceof Generate) {
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            // a generate expands a row into the rows of its table functions, so its identity is the
            // identity of those functions: the function and its arguments decide how many rows come out,
            // and the arguments are rendered with their literals (see identityText)
            Node node = new Node(Kind.GENERATE);
            node.generators = new ArrayList<>(((Generate) plan).getGenerators());
            node.generatorOutputs = generatorOutputsOf(plan);
            node.generatorConjuncts = generatorConjunctsOf(plan);
            node.children = children(ge, ctx, 0);
            return node.children == null ? null : node;
        }
        if (plan instanceof Repeat) {
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            // a repeat produces the rows of several grouping sets, so its identity is the list of those
            // sets; the order of the sets is part of the identity (the grouping id values depend on it)
            // and is kept as written, while the expressions of one set are sorted (their order does not
            // change the groups)
            Node node = new Node(Kind.REPEAT);
            node.groupingSets = new ArrayList<>();
            for (List<Expression> set : ((Repeat<?>) plan).getGroupingSets()) {
                node.groupingSets.add(new ArrayList<>(set));
            }
            node.children = children(ge, ctx, 0);
            return node.children == null ? null : node;
        }
        if (plan instanceof Union || plan instanceof Intersect || plan instanceof Except) {
            return setOperationNode(plan, (SetOperation) plan, ge, ctx);
        }
        if (plan instanceof PhysicalDistribute || plan instanceof PhysicalAssertNumRows
                || plan instanceof Project || plan instanceof Sort) {
            // an enforcer, a project and a sort are transparent for structure matching: none of them
            // changes the number of rows, so the nodes above them are described by the shape below and
            // they must not make the struct info of those nodes disappear
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            return passthroughNode(ge, 0, ctx);
        }
        if (plan instanceof Window) {
            if (ge.arity() != 1) {
                return invalidNode(ARITY_REASON_PREFIX + ge.arity() + ARITY_REASON_MIDDLE
                        + plan.getClass().getSimpleName(), ctx);
            }
            // a window computes its columns from the rows of its partition, so its definition decides the
            // values a node above it reads and is part of the identity; the window itself adds no rows
            Node node = new Node(Kind.WINDOW);
            node.windowExpressions = new ArrayList<>(((Window) plan).getWindowExpressions());
            node.children = children(ge, ctx, 0);
            return node.children == null ? null : node;
        }
        if (plan instanceof Join) {
            return joinNode((Join) plan, ge, ctx);
        }
        if (plan instanceof Aggregate) {
            // only the grouping keys take part: the output row count of an aggregation is the number of
            // groups, which the aggregate functions and the output expressions cannot change, so keeping
            // them would only split one plan pattern into several cache entries
            return conditionsNode(Kind.AGGREGATE, ((Aggregate<?>) plan).getGroupByExpressions(), ge, ctx);
        }
        return invalidNode(UNSUPPORTED_REASON_PREFIX + plan.getClass().getSimpleName(), ctx);
    }

    /** A filter or an aggregation: a header made of its conditions, then its single child. */
    private static Node conditionsNode(Kind kind, Collection<Expression> conditions, GroupExpression ge,
            Ctx ctx) {
        if (ge.arity() != 1) {
            return invalidNode("arity=" + ge.arity() + " of " + ge.getPlan().getClass().getSimpleName(), ctx);
        }
        Node node = new Node(kind);
        node.conditions = new ArrayList<>(conditions);
        node.children = children(ge, ctx, 0);
        return node.children == null ? null : node;
    }

    /**
     * A join. An inner or cross join is flattened together with every flattenable join below it into one
     * node whose conditions are the merged conditions of the chain and whose children are the leaves of
     * the chain (the first sub tree which is not part of it), while an outer / semi / anti / asof join
     * keeps its type and the memo order of its two sides, because it cannot be reassociated.
     */
    private static Node joinNode(Join join, GroupExpression ge, Ctx ctx) {
        if (!isFlattenableJoinType(join.getJoinType())) {
            Node node = new Node(Kind.JOIN);
            node.joinType = join.getJoinType();
            node.markJoin = join.isMarkJoin();
            node.conditions = joinConditions(join);
            node.children = children(ge, ctx, 0);
            if (node.children == null) {
                return null;
            }
            for (int i = 1; i < ge.arity(); i++) {
                List<Child> more = children(ge, ctx, i);
                if (more == null) {
                    return null;
                }
                node.children.addAll(more);
            }
            return node;
        }
        return joinChainNode(join, ge, ctx);
    }

    /**
     * A whole inner / cross join chain as one node: inner and cross joins are associative and
     * commutative, so the grouping of the chain is an artifact of the explored expression and must not be
     * part of the fingerprint. The conditions of every join of the chain are merged (de-duplicated by
     * their literal carrying text) and the leaves are sorted by their own identity - an order which is
     * part of the fingerprint and must therefore not depend on a data state.
     */
    private static Node joinChainNode(Join join, GroupExpression ge, Ctx ctx) {
        List<Expression> conditions = joinConditions(join);
        List<Leaf> leaves = new ArrayList<>();
        for (int i = 0; i < ge.arity(); i++) {
            ChainInfo child = chainInfoOf(ge.child(i));
            if (!child.valid) {
                return invalidNode(child.skipReason, ctx);
            }
            mergeConditions(conditions, child.conditions);
            if (!composeChain(child, leaves, ctx)) {
                return null;
            }
        }
        if (!ctx.valid) {
            return null;
        }
        leaves.sort(Comparator.comparing(Leaf::sortKey));
        Node node = new Node(Kind.JOIN);
        node.flattenable = true;
        node.conditions = conditions;
        for (Leaf leaf : leaves) {
            node.children.add(new Child(leaf.getLeaf().root(), leaf.isRepeated()));
        }
        return node;
    }

    /**
     * A set operation: its header carries the kind, the qualifier and the compared columns, and every
     * branch writes the mapping of the columns it feeds the outputs with (see {@link #branchMapping}).
     * The branches are sorted - the memo may reorder them - together with their mapping, because the
     * mapping belongs to one branch and two branches of the same shape can still feed different columns.
     */
    private static Node setOperationNode(Plan plan, SetOperation setOperation, GroupExpression ge, Ctx ctx) {
        Node node = new Node(Kind.SET_OPERATION);
        node.setOperationPlan = plan;
        node.setOperation = setOperation;
        List<GroupStructInfo> branches = describeBranches(ge, ctx);
        if (branches == null) {
            return null;
        }
        List<List<SlotReference>> mappings = setOperation.getRegularChildrenOutputs();
        if (plan instanceof Except) {
            // the first branch is the minuend: it keeps its position, the subtrahends are sorted below
            node.children.add(branchChild(branches.get(0), mappings, 0));
            List<Integer> subtrahends = new ArrayList<>();
            for (int i = 1; i < branches.size(); i++) {
                subtrahends.add(i);
            }
            subtrahends.sort(Comparator.comparing(
                    index -> branchSortKey(branches.get(index), mappings, index)));
            for (int index : subtrahends) {
                node.children.add(branchChild(branches.get(index), mappings, index));
            }
            return node;
        }
        // the branches are sorted because the memo may reorder them
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < branches.size(); i++) {
            order.add(i);
        }
        order.sort(Comparator.comparing(index -> branchSortKey(branches.get(index), mappings, index)));
        for (int index : order) {
            node.children.add(branchChild(branches.get(index), mappings, index));
        }
        return node;
    }

    private static Child branchChild(GroupStructInfo branch, List<List<SlotReference>> mappings, int index) {
        return new Child(branch.root(), false, branchMapping(mappings, index, branch.root()));
    }

    /** The mapping of one branch followed by its shape: the key the branches of a node are sorted by. */
    private static String branchSortKey(GroupStructInfo branch, List<List<SlotReference>> mappings, int index) {
        return branchMappingText(branchMapping(mappings, index, branch.root()), LiteralMode.WITH_LITERAL)
                + branch.getShape(LiteralMode.WITH_LITERAL);
    }

    /**
     * The columns one branch of a set operation feeds its outputs with. The branches are children of the
     * node, but the projects which select those columns are transparent and the output slots of the
     * operation are new ones, so this mapping is the only place where an output column is tied to a
     * column of the branch: {@code SELECT a FROM t UNION SELECT a FROM u} and the same query selecting
     * {@code u.b} read the same two tables and produce a different number of distinct rows.
     */
    private static List<Expression> branchMapping(List<List<SlotReference>> mappings, int index, Node branchNode) {
        if (index >= mappings.size() || mappings.get(index).isEmpty()) {
            return Collections.emptyList();
        }
        Map<ExprId, Expression> definitions = branchNode == null
                ? Collections.emptyMap() : branchNode.columnDefinitions;
        List<Expression> mapping = new ArrayList<>(mappings.get(index).size());
        for (SlotReference mapped : mappings.get(index)) {
            // the mapped slot may be the output of a computed project inside the branch: the value the
            // branch feeds the output with is that definition, not the name of the slot
            mapping.add(resolveColumnDefinitions(mapped, definitions));
        }
        return mapping;
    }

    /** The mapping of one branch, rendered in the literal mode of the node above it. */
    private static String branchMappingText(List<Expression> mapping, LiteralMode mode) {
        if (mapping.isEmpty()) {
            return "";
        }
        StringBuilder text = new StringBuilder("[");
        for (int i = 0; i < mapping.size(); i++) {
            if (i > 0) {
                text.append(',');
            }
            text.append(renderExpression(mapping.get(i), mode));
        }
        return text.append(']').toString();
    }

    /** Whether a project only forwards the columns of its child (no output of it is an alias). */
    private static boolean isTransparentProject(Plan plan) {
        if (!(plan instanceof Project)) {
            return false;
        }
        for (NamedExpression expression : ((Project) plan).getProjects()) {
            if (expression instanceof Alias) {
                return false;
            }
        }
        return true;
    }

    /** A node which only composes the child at {@code index} (an enforcer, a project, a CTE edge). */
    private static Node passthroughNode(GroupExpression ge, int index, Ctx ctx) {
        Node node = new Node(Kind.PASSTHROUGH);
        node.children = children(ge, ctx, index);
        return node.children == null ? null : node;
    }

    /**
     * A scan of an internal table: its descriptor carries what the read side needs to compare the data
     * state, so a scan whose state cannot be read leaves the whole group undescribed.
     */
    private static Node olapScanNode(OlapScan scan, Ctx ctx) {
        HboScanDescriptor descriptor = scanDescriptor(scan, ctx);
        return descriptor == null ? null : scanRecord(Kind.SCAN, descriptor);
    }

    /**
     * A scan of an external table. Its identity is the table name only, which is why a scan which reads an
     * explicit snapshot or a sampled part of one would share it although it holds different rows: such a
     * scan is not described until the selector is part of the identity.
     */
    private static Node fileScanNode(TableIf table, String relationIdentity, boolean explicitlyVersioned,
            Ctx ctx) {
        String name = HboScanDescriptor.qualifiedNameOf(table);
        if (name.indexOf(',') >= 0 || name.indexOf('}') >= 0) {
            // the token delimiters cannot appear in a table name: the printed struct info could not be
            // read back, and a pasted one could resolve to another table
            return invalidNode("delimiter in table name " + name, ctx);
        }
        if (explicitlyVersioned) {
            return invalidNode("selected file scan " + name, ctx);
        }
        return scanRecord(Kind.FILE_SCAN, HboScanDescriptor.ofUnreadableState(table, relationIdentity));
    }

    /** Both scan kinds carry exactly one descriptor and nothing else. */
    private static Node scanRecord(Kind kind, HboScanDescriptor descriptor) {
        Node node = new Node(kind);
        node.scan = descriptor;
        return node;
    }

    /** The token of an olap scan, or null when its data state cannot be read. */
    private static HboScanDescriptor scanDescriptor(OlapScan scan, Ctx ctx) {
        if (scan instanceof LogicalOlapScan && ((LogicalOlapScan) scan).getTableSample().isPresent()) {
            // a table sample reads a random subset of the table, and a different one on every execution:
            // no row count recorded for such a scan can be reused, and a node above it inherits that
            invalid(ctx, "tablesample of " + scan.getTable().getNameWithFullQualifiers());
            return null;
        }
        try {
            HboScanDescriptor descriptor = HboScanDescriptor.of(scan);
            if (descriptor.getTable().indexOf(',') >= 0 || descriptor.getTable().indexOf('}') >= 0) {
                LOG.debug("scan table {} contains a struct info delimiter, hbo is disabled for it",
                        descriptor.getTable());
                invalid(ctx, "delimiter in table name " + descriptor.getTable());
                return null;
            }
            return descriptor;
        } catch (org.apache.doris.rpc.RpcException e) {
            // the data state of the table cannot be read (a cloud rpc failure, an external table whose
            // metadata is not available): the sub tree is not described
            LOG.debug("failed to get visible version for scan {}", scan.getTable().getNameWithFullQualifiers(), e);
            invalid(ctx, "no data state for " + scan.getTable().getNameWithFullQualifiers()
                    + " (" + e.getMessage() + ")");
            return null;
        }
    }

    /** The children at the given indexes, or null when one of them invalidates this node. */
    private static List<Child> children(GroupExpression ge, Ctx ctx, int... indexes) {
        List<Child> children = new ArrayList<>(indexes.length);
        for (int index : indexes) {
            GroupStructInfo child = describeChild(ge, index, ctx);
            if (child == null) {
                return null;
            }
            children.add(new Child(child.root(), false));
        }
        return children;
    }

    private static Node invalidNode(String reason, Ctx ctx) {
        invalid(ctx, reason);
        return null;
    }

    // ---------------------------------------------------------------- render

    /**
     * Render one form of a description: the canonical string, or its fingerprint input. A node which was
     * rendered in that form before returns its materialized text, so the sub tree of such a node is not
     * walked again - a form of a whole query is therefore assembled in time linear in its nodes.
     */
    private static String render(Node node, LiteralMode mode, boolean shape) {
        String materialized = node.materialized(mode, shape);
        if (materialized != null) {
            return materialized;
        }
        materializedGroupCount.incrementAndGet();
        StringBuilder out = new StringBuilder(256);
        appendNode(out, node, mode, shape);
        String rendered = out.toString();
        node.setMaterialized(mode, shape, rendered);
        return rendered;
    }

    /** The one place which knows the grammar of the canonical string. */
    private static void appendNode(StringBuilder out, Node node, LiteralMode mode, boolean shape) {
        switch (node.kind) {
            case SCAN:
            case FILE_SCAN:
                appendScanToken(out, node.scan, shape);
                return;
            case STORAGE_AGGREGATE:
                out.append("A{sl,op=").append(node.storageAggregate.getAggOp()).append("}(");
                if (node.scan != null) {
                    appendScanToken(out, node.scan, shape);
                }
                out.append(')');
                return;
            case FILTER:
                out.append("F{").append(sortedConditions(node.conditions, mode)).append("}(");
                appendChildren(out, node, mode, shape);
                out.append(')');
                return;
            case AGGREGATE:
                out.append("A{gb:").append(sortedIdentityConditions(node.conditions)).append("}(");
                appendChildren(out, node, mode, shape);
                out.append(')');
                return;
            case JOIN:
                out.append("J{").append(node.flattenable ? "inner" : String.valueOf(node.joinType))
                        .append(node.markJoin ? ",mark" : "")
                        .append(",c:[").append(distinctIdentityConditions(node.conditions)).append("]}(");
                appendChildren(out, node, mode, shape);
                out.append(')');
                return;
            case TOPN:
                appendTopNHeader(out, node, mode);
                appendChildren(out, node, mode, shape);
                return;
            case LIMIT:
                out.append(bound("L", node.limit, node.offset));
                appendChildren(out, node, mode, shape);
                return;
            case WINDOW:
                appendWindowHeader(out, node);
                appendChildren(out, node, mode, shape);
                out.append(')');
                return;
            case GENERATE:
                appendGenerateHeader(out, node);
                appendChildren(out, node, mode, shape);
                return;
            case REPEAT:
                appendRepeatHeader(out, node, mode);
                appendChildren(out, node, mode, shape);
                return;
            case SET_OPERATION:
                appendSetOperation(out, node, mode, shape);
                return;
            case PASSTHROUGH:
                appendChildren(out, node, mode, shape);
                return;
            default:
                throw new IllegalStateException("unknown node kind " + node.kind);
        }
    }

    private static void appendChildren(StringBuilder out, Node node, LiteralMode mode, boolean shape) {
        for (int i = 0; i < node.children.size(); i++) {
            if (i > 0) {
                out.append(SEP);
            }
            // a child which was rendered in this form before contributes its materialized text
            out.append(render(node.children.get(i).node, mode, shape));
        }
    }

    private static void appendScanToken(StringBuilder out, HboScanDescriptor descriptor, boolean shape) {
        out.append("S{").append(shape ? descriptor.getShapeIdentity() : descriptor.render()).append('}');
    }

    /**
     * The conditions of a filter or an aggregation in one mode: every condition is rendered in that mode
     * and the rendered texts are sorted, but a text which occurs twice stays twice. Two conjuncts can fold
     * to the same constant agnostic text ({@code a like '%x%' and a like '%y%'}), and they still describe
     * two predicates whose plans keep a different number of rows: de-duplicating them would give the two
     * plans one key, and a pinned count measured on one of them would be applied to the other.
     */
    private static String sortedConditions(List<Expression> conditions, LiteralMode mode) {
        List<String> rendered = new ArrayList<>(conditions.size());
        for (Expression condition : conditions) {
            rendered.add(renderExpression(condition, mode));
        }
        rendered.sort(String::compareTo);
        return String.join(SEP, rendered);
    }

    /**
     * The conditions of a join in one mode: rendered, sorted and de-duplicated. A flattened chain can
     * contribute the same condition from several of its joins, and the two string sets of the previous
     * implementation de-duplicated by the rendered text for that reason.
     */
    private static String distinctConditions(List<Expression> conditions, LiteralMode mode) {
        TreeSet<String> rendered = new TreeSet<>();
        for (Expression condition : conditions) {
            rendered.add(renderExpression(condition, mode));
        }
        return String.join(SEP, rendered);
    }

    /**
     * The grouping keys of an aggregation, rendered with their literals in both modes (see
     * {@link Kind#AGGREGATE}): a key which names a definition of the sub tree carries the literals of that
     * definition, and they decide the number of groups. Sorted like every other condition collection,
     * duplicates kept (a key repeated in the group by is a different plan).
     */
    private static String sortedIdentityConditions(List<Expression> conditions) {
        List<String> rendered = new ArrayList<>(conditions.size());
        for (Expression condition : conditions) {
            rendered.add(identityText(condition));
        }
        rendered.sort(String::compareTo);
        return String.join(SEP, rendered);
    }

    /**
     * The conditions of a join, rendered with their literals in both modes (see {@link Kind#JOIN}), sorted
     * and de-duplicated like every other join condition collection.
     */
    private static String distinctIdentityConditions(List<Expression> conditions) {
        TreeSet<String> rendered = new TreeSet<>();
        for (Expression condition : conditions) {
            rendered.add(identityText(condition));
        }
        return String.join(SEP, rendered);
    }

    /** One condition, rendered in one literal mode. */
    private static String renderExpression(Expression expression, LiteralMode mode) {
        StringBuilder sb = new StringBuilder();
        if (mode == LiteralMode.NO_LITERAL) {
            appendNormalizedExpression(expression, new StringBuilder(), sb);
        } else {
            appendNormalizedExpression(expression, sb, new StringBuilder());
        }
        return sb.toString();
    }

    private static void appendTopNHeader(StringBuilder out, Node node, LiteralMode mode) {
        out.append("T{").append(node.limit).append(',').append(node.offset).append(";ord:[");
        boolean first = true;
        for (OrderKey orderKey : node.orderKeys) {
            if (!first) {
                out.append(',');
            }
            first = false;
            // a literal inside an ordering key decides which rows the top-n keeps, so it is part of the
            // identity and is written in both modes (see Kind.literalsAreIdentity): folding it would give
            // `order by b * 1` and `order by b * -1` one key in the constant agnostic mode
            out.append(identityText(orderKey.getExpr()));
            out.append(orderKey.isAsc() ? ":asc" : ":desc")
                    .append(orderKey.isNullFirst() ? ":nulls_first" : ":nulls_last");
        }
        out.append("]}");
    }

    private static void appendWindowHeader(StringBuilder out, Node node) {
        // one `{...}` group for the whole header, not one per expression: the fold protects such a group
        // by finding its closing brace, so a group per expression would leave the later expressions
        // unprotected and fold the identity literals of their definitions
        out.append("W{");
        boolean firstExpression = true;
        for (NamedExpression namedExpression : node.windowExpressions) {
            // the algebra interface exposes the named expressions; each of them is the alias which gives
            // the computed column its name, and the window expression is its child
            WindowExpression windowExpression = (WindowExpression) namedExpression.child(0);
            if (!firstExpression) {
                out.append(';');
            }
            firstExpression = false;
            // the output a definition computes is part of the identity: `row_number() AS x` with the
            // first definition and `rank() AS x` with it are the same definition set, but a predicate
            // above the window keeps a different number of rows from x in the two plans. The name is
            // encoded like every other value, so a name carrying a delimiter can not split the entry
            if (namedExpression instanceof Alias) {
                // only an explicit alias names the output in a way the user chose; the generated name of an
                // unaliased window is the source text of its definition, which would make the key depend on
                // the spelling of the statement
                out.append(encodeLiteralValue(namedExpression.toSlot().getName()).replace("=", "%3D"))
                        .append('=');
            }
            // the definition of a window decides the values a node above it reads, so its literals are
            // part of the identity and are written in both modes (see Kind.literalsAreIdentity)
            out.append(identityText(windowExpression.getFunction()));
            out.append(";part:[");
            out.append(identityText(windowExpression.getPartitionKeys()));
            out.append("];ord:[");
            boolean firstKey = true;
            for (OrderExpression orderExpression : windowExpression.getOrderKeys()) {
                if (!firstKey) {
                    out.append(',');
                }
                firstKey = false;
                OrderKey orderKey = orderExpression.getOrderKey();
                out.append(identityText(orderKey.getExpr()));
                out.append(orderKey.isAsc() ? ":asc" : ":desc")
                        .append(orderKey.isNullFirst() ? ":nulls_first" : ":nulls_last");
            }
            // a frame boundary is a number of rows, so its text does not depend on the mode: folding it
            // would let a bounded frame share a key with an unbounded one
            out.append("];frame:[").append(windowExpression.getWindowFrame().map(Object::toString).orElse(""))
                    .append(']');
        }
        out.append("}(");
    }

    private static void appendGenerateHeader(StringBuilder out, Node node) {
        out.append("G{tf:[");
        for (int i = 0; i < node.generators.size(); i++) {
            if (i > 0) {
                out.append(';');
            }
            out.append(identityText(node.generators.get(i)));
        }
        // the outputs are part of the identity: the same functions bound to a different column reach a
        // predicate above the generate differently (a single output function keeps the alias it was
        // given), so each position is named next to the function which fills it
        out.append("];out:[");
        for (int i = 0; i < node.generatorOutputs.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(encodeLiteralValue(node.generatorOutputs.get(i).getName()));
        }
        // the lateral ON conditions decide which produced rows survive: `ON x > 0` and `ON x > 1000`
        // keep a different number of rows from the same array
        out.append("];on:[");
        List<String> onConditions = new ArrayList<>(node.generatorConjuncts.size());
        for (Expression conjunct : node.generatorConjuncts) {
            onConditions.add(identityText(conjunct));
        }
        onConditions.sort(String::compareTo);
        out.append(String.join(SEP, onConditions));
        out.append("]}");
    }

    /** The lateral ON conditions of a generate node, empty when the plan does not expose them. */
    private static List<Expression> generatorConjunctsOf(Plan plan) {
        if (plan instanceof LogicalGenerate) {
            return new ArrayList<>(((LogicalGenerate<?>) plan).getConjuncts());
        }
        if (plan instanceof PhysicalGenerate) {
            return new ArrayList<>(((PhysicalGenerate<?>) plan).getConjuncts());
        }
        return Collections.emptyList();
    }

    /** The ordered output slots of a generate node, empty when the plan does not expose them. */
    private static List<Slot> generatorOutputsOf(Plan plan) {
        if (plan instanceof LogicalGenerate) {
            return new ArrayList<>(((LogicalGenerate<?>) plan).getGeneratorOutput());
        }
        if (plan instanceof PhysicalGenerate) {
            return new ArrayList<>(((PhysicalGenerate<?>) plan).getGeneratorOutput());
        }
        return Collections.emptyList();
    }

    private static void appendRepeatHeader(StringBuilder out, Node node, LiteralMode mode) {
        out.append("R{gs:[");
        for (int i = 0; i < node.groupingSets.size(); i++) {
            if (i > 0) {
                out.append(';');
            }
            out.append('[');
            appendNormalizedSorted(node.groupingSets.get(i), mode, out);
            out.append(']');
        }
        out.append("]}");
    }

    /**
     * A set operation: its kind, its qualifier and the columns which decide whether two rows are the
     * same. The qualifier decides whether the rows are de-duplicated, which changes the row count, so it
     * is part of the identity; the constant branches the memo moved out of the children are carried by
     * the header as well, with their literals, because two distinct unions which differ only in their
     * constants keep a different number of rows.
     */
    private static void appendSetOperation(StringBuilder out, Node node, LiteralMode mode, boolean shape) {
        SetOperation setOperation = node.setOperation;
        Plan plan = node.setOperationPlan;
        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < setOperation.getOutputs().size(); i++) {
            if (i > 0) {
                columns.append(';');
            }
            columns.append(identityText(setOperation.getOutputs().get(i)));
        }
        String kind = plan instanceof Union ? "U" : plan instanceof Intersect ? "I" : "E";
        String qualifier = String.valueOf(setOperation.getQualifier()).toLowerCase();
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
                        constants.append(identityText(branchExpressions.get(i)));
                    }
                    constants.append(']');
                }
            }
        }
        out.append(kind).append('{').append(qualifier).append(",cols:[").append(columns).append(']')
                .append(constants).append("}(");
        appendBranches(out, node, mode, shape);
        out.append(')');
    }

    /** The branches of a set operation: each writes the mapping of its columns before its own text. */
    private static void appendBranches(StringBuilder out, Node node, LiteralMode mode, boolean shape) {
        for (int i = 0; i < node.children.size(); i++) {
            if (i > 0) {
                out.append(SEP);
            }
            Child child = node.children.get(i);
            out.append(branchMappingText(child.mapping, mode));
            out.append(render(child.node, mode, shape));
        }
    }

    /** The scan tokens of a sub tree, in the order their tokens appear in the canonical string. */
    private static void collectScans(Node node, List<HboScanDescriptor> scans) {
        if (node.kind == Kind.SCAN || node.kind == Kind.FILE_SCAN) {
            scans.add(node.scan);
            return;
        }
        if (node.kind == Kind.STORAGE_AGGREGATE) {
            if (node.scan != null) {
                scans.add(node.scan);
            }
            return;
        }
        for (Child child : node.children) {
            if (!child.repeated) {
                // a child which is referenced more than once writes its text again but its tables are
                // read once, so only the first reference contributes scan tokens
                collectScans(child.node, scans);
            }
        }
    }

    /** Mark the description invalid, remembering why (the first reason is the one reported). */
    private static void invalid(Ctx ctx, String reason) {
        ctx.valid = false;
        if (ctx.reason.isEmpty()) {
            ctx.reason = reason;
        }
    }

    /** The same list rendering, for the one mode a node of a description is rendered in. */
    private static void appendNormalizedList(List<? extends Expression> expressions, LiteralMode mode,
            StringBuilder out) {
        if (mode == LiteralMode.NO_LITERAL) {
            appendNormalizedList(expressions, new StringBuilder(), out);
        } else {
            appendNormalizedList(expressions, out, new StringBuilder());
        }
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

    /**
     * Step 1 for a node with an arbitrary number of children (a set operation): describe every branch in
     * memo order, or report the reason of the first branch which cannot be described.
     */
    private static List<GroupStructInfo> describeBranches(GroupExpression ge, Ctx ctx) {
        List<GroupStructInfo> branches = new ArrayList<>(ge.arity());
        for (int i = 0; i < ge.arity(); i++) {
            GroupStructInfo branch = describeGroup(ge.child(i), ctx);
            if (!branch.isValid()) {
                invalid(ctx, branch.getSkipReason());
                return null;
            }
            branches.add(branch);
        }
        return branches;
    }

    /** The bound of a limit / top-n: the same text in both modes, a bound is never folded. */
    private static String bound(String kind, long limit, long offset) {
        return kind + "{" + limit + "," + offset + "}";
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

    /** The identity text of a list of expressions, comma separated (the form the header prints). */
    private static String identityText(List<? extends Expression> expressions) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < expressions.size(); i++) {
            if (i > 0) {
                text.append(',');
            }
            text.append(identityText(expressions.get(i)));
        }
        return text.toString();
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

    /** The same sorted rendering, for the one mode a node of a description is rendered in. */
    private static void appendNormalizedSorted(Collection<Expression> conjuncts, LiteralMode mode,
            StringBuilder out) {
        if (mode == LiteralMode.NO_LITERAL) {
            appendNormalizedSorted(conjuncts, new StringBuilder(), out);
        } else {
            appendNormalizedSorted(conjuncts, out, new StringBuilder());
        }
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

    /** The closing brace which matches the opening brace at {@code openingBrace}. */
    private static int closingBraceOf(String text, int openingBrace) {
        int depth = 0;
        for (int i = openingBrace; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("an unbalanced '" + text.charAt(openingBrace) + "{' at position "
                + openingBrace);
    }

    /**
     * The start of the next protected header at or after {@code from}, or -1. The rule is the one every
     * reader of the canonical string uses ({@link HboScanDescriptor#nextTokenStart}), so a text which
     * merely looks like a header - an escaped name, a literal value - is not taken for one.
     */

    private static int nextProtectedHeaderStart(String text, int from) {
        int best = -1;
        for (int i = 0; i < PROTECTED_HEADERS.length(); i++) {
            int at = HboScanDescriptor.nextTokenStart(text, PROTECTED_HEADERS.charAt(i) + "{", from);
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
            // a scan token is copied verbatim as well: it carries no foldable literal (its name is encoded),
            // and a table whose name contains `lit(` would otherwise be folded into a token which names a
            // different table
            int scanStart = HboScanDescriptor.nextScanTokenStart(canonicalString, index);
            if (scanStart >= 0 && (start < 0 || scanStart < start)
                    && (protectedStart < 0 || scanStart < protectedStart)) {
                int scanEnd = closingBraceOf(canonicalString, scanStart + 1);
                sb.append(canonicalString, index, scanEnd + 1);
                index = scanEnd + 1;
                continue;
            }
            if (protectedStart >= 0 && (start < 0 || protectedStart < start)) {
                // the arguments of these headers are written in *both* literal modes (a generator's
                // delimiter decides how many rows a row expands into, a top-n's ordering and a window's
                // definition decide which rows reach the node above, a set-operation constant and a limit
                // bound decide the rows of the operation), so folding them here would make the sha256 of
                // the pasted struct info differ from the fingerprint EXPLAIN printed
                // the header is one balanced `{...}` group: count the braces instead of taking the first
                // closing one, so a future header with a nested group is still protected as a whole
                int end = closingBraceOf(canonicalString, protectedStart);
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
        return recanonicalizeFoldedConditions(sb.toString());
    }

    /**
     * Re-sort the condition collections of a folded text. The structured rendering renders a condition in
     * the requested mode and then sorts it into its collection, while the substitution above only
     * rewrites the literals of the text it was given: folding the literal carrying form of a node whose
     * conditions order differently in the two modes would therefore not reproduce its constant agnostic
     * text, and the statement would reject a pair the user copied correctly. The collections are the
     * conditions of a filter and of an aggregation (sorted), the conjuncts of a join (sorted and
     * de-duplicated) and the expressions of every grouping set of a repeat (each set sorted).
     */
    private static String recanonicalizeFoldedConditions(String folded) {
        StringBuilder out = new StringBuilder(folded.length());
        int index = 0;
        while (index < folded.length()) {
            int next = nextConditionHeaderStart(folded, index);
            if (next < 0) {
                out.append(folded, index, folded.length());
                return out.toString();
            }
            out.append(folded, index, next);
            int close;
            try {
                close = closingBraceOf(folded, next + 1);
            } catch (IllegalArgumentException e) {
                // the text is not a well formed canonical string: leave it as the substitution produced it,
                // this pass does not add a rejection of its own
                out.append(folded, next, folded.length());
                return out.toString();
            }
            out.append(canonicalConditionHeader(folded.substring(next, close))).append('}');
            index = close + 1;
        }
        return out.toString();
    }

    /** The start of the next header with a mode dependent condition order at or after {@code from}. */
    private static int nextConditionHeaderStart(String folded, int from) {
        int best = -1;
        for (String marker : CONDITION_HEADER_MARKERS) {
            int at = HboScanDescriptor.nextTokenStart(folded, marker, from);
            if (at >= 0 && (best < 0 || at < best)) {
                best = at;
            }
        }
        return best;
    }

    /** Rewrite the condition collection of one header (which it is known by its marker to have). */
    private static String canonicalConditionHeader(String header) {
        if (header.startsWith("F{")) {
            return "F{" + sortedConditionText(header.substring(2));
        }
        if (header.startsWith("A{gb:")) {
            return "A{gb:" + sortedConditionText(header.substring(5));
        }
        if (header.startsWith("J{")) {
            return sortedJoinHeader(header);
        }
        return sortedRepeatHeader(header);
    }

    /**
     * The conjuncts of a join: the renderer collects them into a set, so they are sorted and
     * de-duplicated - folding can map two different literals onto one text (two inequalities with
     * different bounds, for example), which this pass therefore also de-duplicates, and the order of the
     * folded texts is the order of the constant agnostic canonical form.
     */
    private static String sortedJoinHeader(String header) {
        int listStart = header.indexOf(",c:[");
        if (listStart < 0) {
            return header;
        }
        int listOpen = listStart + 3;
        int listClose = closingBracketOf(header, listOpen);
        if (listClose < 0) {
            return header;
        }
        TreeSet<String> conditions = new TreeSet<>(splitConditionRegion(header, listOpen + 1, listClose));
        return header.substring(0, listOpen + 1) + String.join(SEP, conditions)
                + header.substring(listClose);
    }

    /**
     * The expressions of every grouping set of a repeat: each set is sorted on its own and duplicates
     * stay, exactly like the renderer writes them ({@code appendNormalizedSorted}).
     */
    private static String sortedRepeatHeader(String header) {
        int marker = header.indexOf("gs:[");
        if (marker < 0) {
            return header;
        }
        int listOpen = marker + 3;
        int listClose = closingBracketOf(header, listOpen);
        if (listClose < 0) {
            return header;
        }
        StringBuilder sets = new StringBuilder();
        int cursor = listOpen + 1;
        while (cursor < listClose) {
            int setClose = closingBracketOf(header, cursor);
            if (sets.length() > 0) {
                sets.append(SEP);
            }
            sets.append('[').append(sortedConditionText(header.substring(cursor + 1, setClose))).append(']');
            cursor = setClose + 1;
            if (cursor < listClose && header.charAt(cursor) == ';') {
                cursor++;
            }
        }
        return header.substring(0, listOpen + 1) + sets + header.substring(listClose);
    }

    /** The closing bracket which matches the opening bracket at {@code openingBracket}, or -1. */
    private static int closingBracketOf(String text, int openingBracket) {
        int depth = 0;
        for (int i = openingBracket; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Split a {@code ;} separated condition list. A variant sub path prints its own brackets
     * ({@code col(v['age'])}), so only a separator outside a bracket pair ends a condition; a literal
     * value escapes its brackets, so it cannot open one.
     */
    private static List<String> splitConditionRegion(String text, int from, int to) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = from;
        for (int i = from; i < to; i++) {
            char c = text.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (c == ';' && depth == 0) {
                parts.add(text.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(text.substring(start, to));
        return parts;
    }

    /** A {@code ;} separated condition list in sorted order (the order the structured rendering writes). */
    private static String sortedConditionText(String conditions) {
        if (conditions.isEmpty()) {
            return conditions;
        }
        List<String> parts = new ArrayList<>(Arrays.asList(conditions.split(";", -1)));
        parts.sort(String::compareTo);
        return String.join(SEP, parts);
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
     * One component of a slot name. A dot inside the component is encoded, so a column named {@code a.b}
     * of the table {@code t} and a column {@code b} of the aliased table {@code t.a} cannot render one
     * identity (both would read as {@code col(internal.db.t.a.b)}). The encoding is the identity for an
     * ordinary component, so no key of a slot without a dot moves.
     */
    private static String encodeSlotComponent(String component) {
        return encodeLiteralValue(component).replace(".", "%2E");
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
                text.append(encodeSlotComponent(qualifierPart)).append('.');
            }
            text.append(encodeSlotComponent(slot.getName()));
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
            withLiteral.append("lit(").append(encodeLiteralValue(literalText(literal)))
                    .append(':').append(literal.getDataType()).append(')');
            noLiteral.append("lit(*)");
        } else if (expression.children().isEmpty()) {
            String name = expressionName(expression);
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
            String name = expressionName(expression);
            withLiteral.append(name).append('(').append(String.join(",", withLiteralChildren)).append(')');
            noLiteral.append(name).append('(').append(String.join(",", noLiteralChildren)).append(')');
        }
    }

    /** The same rendering, for the one mode a node of a description is rendered in. */
    private static void appendNormalizedExpression(Expression expression, LiteralMode mode, StringBuilder out) {
        if (mode == LiteralMode.NO_LITERAL) {
            appendNormalizedExpression(expression, new StringBuilder(), out);
        } else {
            appendNormalizedExpression(expression, out, new StringBuilder());
        }
    }

    /**
     * The value of a literal as the canonical form prints it. A temporal literal prints its full text:
     * {@code DateLiteral#getValue} is only yyyyMMdd (and a datetime only yyyyMMddHHmmss), so
     * {@code 2020-01-01 00:00:00.000001} and {@code 2020-01-01 00:00:00.000002} would render one literal
     * although execution compares the microseconds, i.e. two predicates which keep a different number of
     * rows would share a key.
     */
    private static String literalText(Literal literal) {
        if (literal instanceof NullLiteral) {
            // the text of every other literal is the value it holds, which for a NULL would be the word a
            // string literal may hold: a reserved character in front of it keeps a NULL and the string
            // `null` apart, and the encoder turns it into an escape no other value produces
            return "\u0000null";
        }
        if (literal instanceof DateLiteral) {
            return ((DateLiteral) literal).getStringValue();
        }
        if (literal instanceof ArrayLiteral) {
            // the elements of a composite literal are literals themselves, so they are rendered by the
            // same rule: the text of the value a list holds separates its elements with an apostrophe
            // which an element may carry, and `['a','b']` would be one text with `['a', 'b']` as a
            // single element - the boundary each element is written with removes that ambiguity
            StringBuilder text = new StringBuilder("[");
            List<Literal> elements = ((ArrayLiteral) literal).getValue();
            for (int i = 0; i < elements.size(); i++) {
                if (i > 0) {
                    text.append(',');
                }
                Literal element = elements.get(i);
                text.append("lit(").append(literalText(element)).append(':')
                        .append(element.getDataType()).append(')');
            }
            return text.append(']').toString();
        }
        return String.valueOf(literal.getValue());
    }

    /**
     * The name one expression contributes to a canonical. For a built-in operator the class name is the
     * identity, but a few expressions carry a part of it in a field rather than in {@code children()}:
     * every Java and Python udf keeps the function it calls, the database which holds it and the
     * catalog id of that overload in fields, and a match predicate keeps the analyzer which tokenizes
     * its argument there. Two of them would render alike although a predicate above them keeps a
     * different number of rows. The field is encoded like a slot name, so a value which contains one of
     * the delimiters of the canonical form cannot be read as the start of another token.
     */
    private static String expressionName(Expression expression) {
        String className = expression.getClass().getSimpleName();
        if (expression instanceof Lambda) {
            // the names of the arguments bind the parameters of the body to the expressions which follow
            // the lambda: `(x,y) -> x > y` and `(y,x) -> x > y` render the same body, while the first
            // compares the first array with the second and the second compares the second with the first
            StringBuilder names = new StringBuilder();
            for (String argumentName : ((Lambda) expression).getLambdaArgumentNames()) {
                if (names.length() > 0) {
                    names.append(',');
                }
                names.append(encodeSlotComponent(argumentName));
            }
            return className + '<' + names + '>';
        }
        if (expression instanceof ArrayItemReference) {
            // the name is how the body of the lambda refers to the array this reference binds
            return className + '<' + encodeSlotComponent(((ArrayItemReference) expression).getName()) + '>';
        }
        if (expression instanceof Match) {
            // `MATCH 'foo-bar' USING ANALYZER a1` and the same predicate with a2 have equal children
            // but select a different analyzer index, so they tokenize the string differently
            return ((Match) expression).getAnalyzer()
                    .map(analyzer -> className + '<' + encodeSlotComponent(analyzer) + '>')
                    .orElse(className);
        }
        if (expression instanceof SearchExpression) {
            // the slot children tell which column is searched, while the terms, operators and options
            // of the search live in the dsl string the expression keeps for execution
            return className + '<'
                    + encodeSlotComponent(((SearchExpression) expression).getDslString()) + '>';
        }
        if (expression instanceof Udf) {
            Udf udf = (Udf) expression;
            // a function of the global function manager has no database (it is registered with a null
            // name): its scope is encoded explicitly, so that a database with the same name as the
            // function cannot be mistaken for it - the two scopes never render one text
            String qualifiedName = udf.getDbName() == null
                    ? "global:" + encodeSlotComponent(udf.getName())
                    : encodeSlotComponent(udf.getDbName()) + '.' + encodeSlotComponent(udf.getName());
            return className + '<' + qualifiedName + '#' + udf.getFunctionId() + '>';
        }
        if (expression instanceof AggregateFunction && ((AggregateFunction) expression).isDistinct()) {
            // `COUNT(DISTINCT v)` and `COUNT(v)` have equal children; the flag is outside them, and it
            // decides which values are counted, i.e. how many rows a predicate above the aggregation keeps
            return className + "<distinct>";
        }
        if (expression instanceof Function && usesSessionTimeZone((Function) expression)) {
            // the session which evaluates the expression decides how an epoch value is read: the same
            // arguments keep a different number of rows in another time zone
            return className + '<' + encodeSlotComponent(sessionTimeZone()) + '>';
        }
        return className;
    }

    /**
     * The time zone of the session which evaluates the expression (the one the planner and the backends
     * read), or the default of this FE when there is no session.
     */
    private static String sessionTimeZone() {
        ConnectContext context = ConnectContext.get();
        if (context == null || context.getSessionVariable() == null) {
            return ZoneId.systemDefault().getId();
        }
        return context.getSessionVariable().getTimeZone();
    }

    /** Whether the value of a function depends on the session time zone (see the two lists above). */
    private static boolean usesSessionTimeZone(Function function) {
        if (EPOCH_TIME_ZONE_FUNCTIONS.contains(function.getName())) {
            return true;
        }
        try {
            if (function.getDataType().isDateLikeType()) {
                return true;
            }
            for (Expression child : function.children()) {
                if (child.getDataType().isDateLikeType()) {
                    return true;
                }
            }
        } catch (RuntimeException e) {
            // a function whose type can only be computed once it is bound (a table function, or a call
            // whose arguments need a cast the analysis inserts) has no date argument the renderer could
            // see: it contributes its class name only, as before
            return false;
        }
        return false;
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
    private static boolean hasNoHboLookupPath(AbstractPlan planNode, Group group) {
        if (planNode instanceof PhysicalStorageLayerAggregate) {
            return true;
        }
        // a group without a logical expression which holds a hash aggregation is the local (partial)
        // stage SplitAggWithoutDistinct inserted into the plan: it describes the same grouping keys over
        // the same sub tree as the aggregation above it, so both run under one key - and publishing the
        // partial run (many rows, one per instance) would become the learned output estimate of the final
        // aggregation (one row). Like the storage layer aggregate it is neither advertised nor published.
        return group != null && group.getLogicalExpressions().isEmpty()
                && planNode instanceof PhysicalHashAggregate;
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
        Group group = resolveGroup(planNode, groupsById);
        if (planNode instanceof PhysicalStorageLayerAggregate) {
            return Optional.of(NO_LOOKUP_PATH_REASON);
        }
        if (hasNoHboLookupPath(planNode, group)) {
            return Optional.of(PARTIAL_STAGE_REASON);
        }
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

}
