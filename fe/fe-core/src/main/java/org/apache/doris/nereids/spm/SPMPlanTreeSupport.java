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

package org.apache.doris.nereids.spm;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundInlineTable;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.properties.SelectHintUseMv;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.spm.matcher.MatchAttempt;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Database;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncryptKeyRef;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SessionUser;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * SPMPlanTreeSupport - whole-query plan-tree SPM engine.
 *
 * SPM binds and rewrites a whole parsed (still unbound) SELECT plan tree, not only the
 * per-query-block WHERE predicates. The three lifecycle phases run over the whole tree:
 *
 * - transform: applies an expression transform to every literal-carrying
 *   expression of every plan node (filter/having predicates, projections, aggregate
 *   group-by/output, ...) and rebuilds the touched nodes. Used both for parameterization
 *   (SPMPlaceholderBuilder) and for substitution (SPMPlaceholderReplacer).
 * - check: structurally compares a parameterized bind tree against the user's
 *   original tree node by node and expression by expression, extracting the actual user
 *   values for every placeholder id (Level 3).
 * - containsPlaceholder: whether a tree still carries an unbound placeholder
 *   (safety net before a rewritten tree is handed to the analyzer).
 *
 * A Literal that lives inside a SubqueryExpr's own query plan (e.g. the constant of a
 * scalar subquery in the SELECT list) is reached through the expression transform: the
 * expression visitors recurse into the subquery plan with transform. Nested
 * query blocks that appear as plain plan nodes (derived tables, CTE bodies) are reached
 * by the normal children / CTE-alias traversal below.
 */
public final class SPMPlanTreeSupport {

    /**
     * Marker of the per-table NULLABILITY section inside one fingerprint entry (see
     * describeTableForFingerprint). An entry persisted before the section
     * existed simply lacks it and is still accepted (legacyEntryOf), so the
     * section can be introduced without invalidating already persisted baselines.
     */
    private static final String NULLABILITY_SECTION = "nullable:";

    /**
     * The marker appended by expandedOutputLabels behind the derivable prefix of
     * a PARTLY derivable star expansion (see the star-expansion helper returned by
     * alignRootOutputLabels). Kept in the constants block: a static field may
     * not sit between methods (checkstyle DeclarationOrder).
     */
    private static final String OPEN_TAIL_LABEL = "\u0000spm-open-star-tail";

    /** Expression transform used by transform. */
    public interface ExprTransform {
        Expression apply(Expression expr);

        /**
         * Called while the rebuild ENTERS a nested query block (a derived table / CTE body /
         * subquery alias), with exitQueryBlock() in a finally block. The placeholder
         * builder gives every block its own identity so two literals of DIFFERENT blocks can
         * never share one placeholder id: block 0 was previously shared by the outer query
         * and every derived table, so equal filter literals there (e.g. an outer
         * x = 1 and a derived-table x = 1 whose parent signatures coincide)
         * merged into one id and a variant changing only one of the values could never
         * match.
         */
        default void enterQueryBlock() {
        }

        /** Matching exit of enterQueryBlock(). */
        default void exitQueryBlock() {
        }

        /**
         * Called while the rebuild enters ONE projection item (SELECT-list slot) of a
         * projection-like list, with exitProjectionItem() in a finally block. The
         * item POSITION is part of a literal's placeholder identity: the literals of
         * SELECT 1 AS x, 1 AS y have the same value, the same child position inside
         * their Alias and (before the position existed) the same parent signature, so they
         * shared one placeholder and a variant with different values per column could never
         * match.
         */
        default void enterProjectionItem(int index) {
        }

        /** Matching exit of enterProjectionItem(int). */
        default void exitProjectionItem() {
        }
    }

    /**
     * An ExprTransform that also changes what its children see while the tree is rebuilt.
     * Only namespace qualification is scope-sensitive: the set of CTE aliases visible in
     * the query text grows when the rebuild enters a CTE body or a CTE's main query, so
     * those two entry points receive a different transform than the surrounding tree.
     * Plain transforms (parameterize / substitute) are not scope-aware and are passed
     * through unchanged.
     */
    private interface ScopedTransform extends ExprTransform {
        /** The transform to use for the main query of a CTE (sees every alias of it). */
        ExprTransform enterCteMain(LogicalCTE<? extends Plan> cte);

        /** The transform to use for the body of alias #aliasIndex (sees the earlier ones). */
        ExprTransform enterCteAlias(LogicalCTE<? extends Plan> cte, int aliasIndex);
    }

    /** Re-descends into expression subquery plans so their hints are stripped as well. */
    private static final ExprTransform HINT_STRIP_EXPR = expr -> {
        if (expr instanceof SubqueryExpr) {
            LogicalPlan subPlan = ((SubqueryExpr) expr).getQueryPlan();
            LogicalPlan stripped = stripSelectHints(subPlan);
            if (stripped != subPlan) {
                return ((SubqueryExpr) expr).withSubquery(stripped);
            }
        }
        return expr;
    };

    /**
     * Expression transform of stripCheckPolicy: strips the policy markers from
     * every plan an expression owns (IN / EXISTS / scalar subquery plans, the payloads of
     * a * REPLACE(...), the ASOF MATCH_CONDITION).
     */
    private static final ExprTransform STRIP_POLICY_IN_EXPRESSION =
            SPMPlanTreeSupport::stripPolicyOfExpression;

    private SPMPlanTreeSupport() {
    }

    /**
     * The top-level LIMIT / OFFSET a query tree exposes to its CALLER ({limit,
     * offset}) or null when it has none. The descent follows the wrapper chain (result
     * sink, sort, ...) and STOPS at a projection: a limit BELOW a projection is not the
     * caller-visible top-level limit - and mergeLimitNode cannot transfer the
     * caller's values there either, which is exactly the situation the replay guard in
     * SPMPlanner checks with this helper.
     */
    public static long[] topLevelLimitOf(Plan plan) {
        Plan node = plan;
        while (node != null && !(node instanceof LogicalLimit) && !(node instanceof LogicalTopN)
                && !(node instanceof LogicalProject) && node.children().size() == 1) {
            node = node.child(0);
        }
        if (node instanceof LogicalLimit) {
            return new long[] {((LogicalLimit<?>) node).getLimit(),
                    ((LogicalLimit<?>) node).getOffset()};
        }
        if (node instanceof LogicalTopN) {
            return new long[] {((LogicalTopN<?>) node).getLimit(),
                    ((LogicalTopN<?>) node).getOffset()};
        }
        return null;
    }

    /**
     * Whether every row-limiting node of replayed is justified by the caller's own
     * tree userPlan: the MULTISET of (limit, offset, INPUT-IDENTITY) keys the
     * replay exposes must be contained in the caller's own - a LIMIT VARIANT replay
     * transfers the caller's top-level value POSITIONALLY, so any other cap it still
     * exposes was inherited from the CAPTURED plan (or a manual plan) and would silently
     * truncate the result of the variant (the frozen
     * SELECT DISTINCT k FROM (SELECT k FROM t ORDER BY k LIMIT 1) s ORDER BY k
     * LIMIT 2 kept the inner cap at 1 while the caller asked for two keys).
     *
     * Comparing INPUT IDENTITIES rather than node paths is deliberate: the replay is a
     * MANUAL frozen plan whose structure may legitimately differ from the caller's, so
     * only what the caller itself asked for (values AND the input they truncate) can
     * justify a cap. The input identity carries the OCCURRENCE of the relation it names
     * two caps over the same table are only equivalent when they truncate
     * the SAME occurrence, otherwise a cap moved between same-table occurrences passed
     * the multiset check and truncated the wrong side of e.g. a self join. The occurrence
     * is the relation's per-name ORDINAL in walk order - NOT its alias, because the
     * frozen text of a baseline is the DECOMPILED plan and the decompiler renames every
     * derived alias (an identical-text baseline must keep hitting its own query: the
     * alias-based form rejected it because the caller's `x` became `t_1`).
     *
     * @param replayed the replayed tree AFTER the limit merge
     * @param userPlan the caller's own tree
     * @return whether every replayed cap is one the caller's tree also has
     */
    public static boolean rowLimitsWithin(Plan replayed, Plan userPlan) {
        Map<String, Integer> allowed = new HashMap<>();
        collectRowLimits(userPlan, allowed);
        Map<String, Integer> used = new HashMap<>();
        collectRowLimits(replayed, used);
        for (Map.Entry<String, Integer> entry : used.entrySet()) {
            Integer available = allowed.get(entry.getKey());
            if (available == null || available < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * The REVERSE direction of rowLimitsWithin: every cap the
     * CALLER's tree asks for must survive in the replayed tree with the SAME values,
     * ordered slice and input placement. The one-directional check only rejected caps the
     * replay ADDED; a replay that DROPPED a caller cap passed (bind
     * SELECT k FROM (SELECT k FROM t ORDER BY k LIMIT 1) s ORDER BY k LIMIT 2 vs
     * manual plan SELECT k FROM t ORDER BY k LIMIT 2: the replay's sole outer cap
     * is contained in the caller's {inner, outer} set, yet for t={1,2,3} the caller
     * returns ONE row and the replay two). Positive logic: a cap can only be honored when
     * the replay demonstrably carries it.
     *
     * @param replayed the replayed tree AFTER the limit merge
     * @param userPlan the caller's own tree
     * @return whether every caller cap exists in the replayed tree
     */
    public static boolean rowLimitsSurviveReplay(Plan replayed, Plan userPlan) {
        Map<String, Integer> required = new HashMap<>();
        collectRowLimits(userPlan, required);
        Map<String, Integer> exposed = new HashMap<>();
        collectRowLimits(replayed, exposed);
        for (Map.Entry<String, Integer> entry : required.entrySet()) {
            Integer present = exposed.get(entry.getKey());
            if (present == null || present < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * The equal-top-limit acceptance for the OPTIMIZER-REDUCED caller tree: the RAW
     * caller can contain MORE occurrences of one table than the frozen optimal plan,
     * which collapses them (common-subexpression elimination folds three date_dim
     * scans into one, tpcds q76), so the occurrence-tagged keys of the TOP-LEVEL cap
     * differ although the cap truncates the same (ordered) result. Accept when the
     * caller's and the replay's top-level cap agree on (limit, offset, ordered-slice)
     * and on the occurrence-ordinal-FREE relation NAME set, while every OTHER cap on
     * both sides keeps the strict two-way containment. The caller must already have
     * established that the top-level (limit, offset) is unchanged from the stored
     * bind tree (see SPMPlanner#limitContractPreserved): the value cannot come from a
     * variant's positional transfer, and only the top cap - whose slice is the whole
     * result, not one occurrence of a self-join - may ignore the ordinals.
     *
     * @param replayed the replayed tree AFTER the limit merge
     * @param userPlan the caller's own tree
     * @return whether the top caps agree modulo occurrence ordinals and everything
     *         else stays strictly contained
     */
    @VisibleForTesting
    static boolean topCapContractPreserved(Plan replayed, Plan userPlan) {
        long[] top = topLevelLimitOf(userPlan);
        if (top == null || !Arrays.equals(top, topLevelLimitOf(replayed))) {
            return false;
        }
        Map<String, Integer> userCaps = new HashMap<>();
        collectRowLimits(userPlan, userCaps);
        Map<String, Integer> replayCaps = new HashMap<>();
        collectRowLimits(replayed, replayCaps);
        String prefix = top[0] + ":" + top[1] + ":";
        String userTopKey = singleTopCapKey(userCaps, prefix);
        String replayTopKey = singleTopCapKey(replayCaps, prefix);
        if (userTopKey == null || replayTopKey == null) {
            // more than one cap shares the top (limit, offset): keep the strict checks
            return false;
        }
        if (!stripOccurrenceOrdinals(userTopKey).equals(stripOccurrenceOrdinals(replayTopKey))) {
            return false;
        }
        consumeOne(userCaps, userTopKey);
        consumeOne(replayCaps, replayTopKey);
        for (Map.Entry<String, Integer> entry : replayCaps.entrySet()) {
            Integer available = userCaps.get(entry.getKey());
            if (available == null || available < entry.getValue()) {
                return false;
            }
        }
        for (Map.Entry<String, Integer> entry : userCaps.entrySet()) {
            Integer present = replayCaps.get(entry.getKey());
            if (present == null || present < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /** The single cap key of the given (limit, offset) prefix, or null when ambiguous. */
    private static String singleTopCapKey(Map<String, Integer> caps, String prefix) {
        String found = null;
        for (Map.Entry<String, Integer> entry : caps.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                if (found != null) {
                    return null;
                }
                found = entry.getKey();
            }
        }
        return found;
    }

    /**
     * Whether the tree's ONLY row cap is its top-level one: a LIMIT VARIANT (the caller's
     * top value differs from the stored bind tree's) whose replay carries no OTHER cap
     * cannot truncate a different slice - the positionally merged top cap governs the
     * whole result (tpcds q76's folded plan has one cap for the entire tree, so the
     * similar query raising LIMIT 100 to 101 replays it unchanged except for the value;
     * see SPMPlanner#limitContractPreserved).
     */
    public static boolean onlyTopCap(Plan plan) {
        long[] top = topLevelLimitOf(plan);
        if (top == null) {
            return false;
        }
        Map<String, Integer> caps = new HashMap<>();
        collectRowLimits(plan, caps);
        return caps.size() == 1
                && caps.keySet().iterator().next().startsWith(top[0] + ":" + top[1] + ":");
    }

    /** Consumes one instance of the key's multiplicity (multiset bookkeeping). */
    private static void consumeOne(Map<String, Integer> caps, String key) {
        Integer count = caps.get(key);
        if (count == null) {
            return;
        }
        if (count <= 1) {
            caps.remove(key);
        } else {
            caps.put(key, count - 1);
        }
    }

    /**
     * One cap key with the per-name occurrence ordinals dropped from its input section:
     * "limit:offset:slice:name#N,name#M" becomes "limit:offset:slice:name1,name2"
     * (sorted names, duplicates collapsed). The head - the limit, the offset and the
     * ordered-slice fingerprint (order-key count and directions) - stays comparable.
     */
    private static String stripOccurrenceOrdinals(String capKey) {
        int first = capKey.indexOf(':');
        int second = first < 0 ? -1 : capKey.indexOf(':', first + 1);
        int third = second < 0 ? -1 : capKey.indexOf(':', second + 1);
        if (third < 0) {
            return capKey;
        }
        TreeSet<String> names = new TreeSet<>();
        String inputs = capKey.substring(third + 1);
        for (String input : inputs.split(",")) {
            if (input.isEmpty()) {
                continue;
            }
            int hash = input.lastIndexOf('#');
            names.add(hash < 0 ? input : input.substring(0, hash));
        }
        return capKey.substring(0, third + 1) + String.join(",", names);
    }

    /** The collected cap keys of one tree (sorted) - diagnostics and tests. */
    @VisibleForTesting
    static String rowLimitKeysForTest(Plan plan) {
        Map<String, Integer> caps = new HashMap<>();
        collectRowLimits(plan, caps);
        List<String> keys = new ArrayList<>(caps.keySet());
        Collections.sort(keys);
        return keys.toString();
    }

    /**
     * Whether every cap of the replayed tree is either one the CALLER's own tree carries
     * (same value, offset and input identity, see rowLimitsWithin) or a pushdown OF THE
     * CALLER's own top-level cap: the optimizer folds a top LIMIT n into the plan it
     * plans (the tpcds q28 frozen plan carries one 100:0 cap per store_sales scan),
     * so the frozen plan of the IDENTICAL query legitimately exposes caps the RAW
     * caller tree has no node for. Only caps with EXACTLY the caller's top-level
     * (limit, offset) qualify, and the caller must ask for NO order contract of its
     * own: an equal-valued inner cap below an ordering-dependent slice truncates a
     * DIFFERENT slice (see SPMPlanner#limitContractPreserved).
     */
    @VisibleForTesting
    static boolean replayCapsJustifiedByCallerTopLimit(Plan replayed, Plan userPlan) {
        long[] top = topLevelLimitOf(userPlan);
        if (top == null) {
            return false;
        }
        boolean ordered = !rootOrderContractForTest(userPlan).isEmpty();
        // With a caller ORDER BY the folded cap must not cut a different slice. That is
        // guaranteed when the replay still exposes the caller's own TOP-LEVEL ordering
        // AND the folded caps sit on the BRANCHES of a set operation: the merge of the
        // per-branch orders selects the caller's slice among each branch's own top rows
        // (tpcds q49's union branches carry one 100 cap each), while a cap on the input
        // of an ordinary sort would truncate before the ordering exists.
        if (ordered && !orderContractPreserved(replayed, userPlan)) {
            return false;
        }
        Map<String, Integer> allowed = new HashMap<>();
        collectRowLimits(userPlan, allowed);
        Map<String, Integer> used = new HashMap<>();
        collectRowLimits(replayed, used);
        String prefix = top[0] + ":" + top[1] + ":";
        Set<String> topScope = null;
        List<Set<String>> extraScopes = new ArrayList<>();
        if (ordered) {
            // the first set operation below the order/limit wrappers: the class-exact
            // helper cannot be used here (the concrete Union / Intersect / Except class is
            // what the caller-side comparison keys on), so descend on the base interface
            Plan probe = replayed;
            while (probe != null && !(probe instanceof LogicalSetOperation)
                    && probe.children().size() == 1) {
                probe = probe.child(0);
            }
            if (!(probe instanceof LogicalSetOperation)) {
                return false;
            }
            topScope = topCapScopeOf(allowed, prefix);
        }
        for (Map.Entry<String, Integer> entry : used.entrySet()) {
            if (allowed.containsKey(entry.getKey())) {
                continue;
            }
            if (!entry.getKey().startsWith(prefix)) {
                return false;
            }
            if (ordered) {
                Set<String> scope = capKeyRelations(entry.getKey());
                if (topScope == null || !topScope.containsAll(scope)) {
                    return false;
                }
                extraScopes.add(scope);
            }
        }
        if (ordered) {
            // one cap per branch: overlapping branch scopes would let two folded caps
            // truncate the same input
            for (int i = 0; i < extraScopes.size(); i++) {
                for (int j = i + 1; j < extraScopes.size(); j++) {
                    for (String relation : extraScopes.get(i)) {
                        if (extraScopes.get(j).contains(relation)) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /** The relation set of the WIDEST caller cap carrying the given (limit, offset) prefix. */
    private static Set<String> topCapScopeOf(Map<String, Integer> allowed, String prefix) {
        Set<String> widest = null;
        for (String key : allowed.keySet()) {
            if (key.startsWith(prefix)) {
                Set<String> relations = capKeyRelations(key);
                if (widest == null || relations.size() > widest.size()) {
                    widest = relations;
                }
            }
        }
        return widest;
    }

    /** The relation names of one cap key ("limit:offset:slice:r1,r2,..."). */
    private static Set<String> capKeyRelations(String key) {
        int first = key.indexOf(':');
        int second = first < 0 ? -1 : key.indexOf(':', first + 1);
        int third = second < 0 ? -1 : key.indexOf(':', second + 1);
        if (third < 0 || third + 1 >= key.length()) {
            return Set.of();
        }
        return new HashSet<>(Arrays.asList(key.substring(third + 1).split(",")));
    }

    /**
     * Collects every row-limiting node of the tree as (limit, offset, INPUT IDENTITIES):
     * the (limit, offset) pair ALONE is not enough - a cap of the same value can sit on a
     * different input than the caller's own cap of that value, and the positional merge
     * leaves it there (a manual plan capped t2 while the caller's own cap
     * sat on t1; with one matching t1 row and two matching t2 rows the variant returned
     * ONE row where the caller's own plan returns two). The key therefore includes every
     * relation beneath the cap, so a replayed cap only counts as one the caller also has
     * when it truncates the same input.
     *
     * The input identities alone are not enough either when the SAME table occurs more
     * than once: a manual plan can move ORDER BY k LIMIT 1 from the first table
     * instance to a second instance of the SAME table, and a caller raising only the
     * outer LIMIT from 1 to 2 then compared 1:0:t against 1:0:t and
     * accepted the replay - although t={1,2} yields (1,1),(2,1) instead of (1,1),(1,2)
     * . Each input therefore carries its OCCURRENCE ORDINAL among the
     * relations of the same name, assigned in walk order: a cap moved to another
     * occurrence of the same input gets a different key and the variant is skipped (its
     * placement cannot be proven equivalent), which is always safe - the caller's own
     * query then simply plans normally. The ordinal (rather than the alias name) is what
     * keeps an IDENTICAL-text baseline matchable: its frozen text is the DECOMPILED plan,
     * whose derived aliases are regenerated, while the relative order of the relation
     * occurrences is preserved.
     *
     * The walk covers the plans OUTSIDE children() as well - CTE bodies
     * (LogicalCTE.extraPlans()) and expression-owned subquery plans (IN / EXISTS / scalar,
     * plus * REPLACE payloads) - a cap inside a WITH body was invisible to a
     * children-only walk, and the caller raising only the OUTER limit kept the frozen body
     * cap for the variant. The identity set keeps a plan reachable through
     * two paths (a filter's extraPlans() and its predicate expression) from counting the
     * same cap twice.
     */
    private static void collectRowLimits(Plan plan, Map<String, Integer> out) {
        // pass 1: assign every relation its per-name occurrence ordinal in walk order;
        // pass 2: collect the caps with those ordinals (the two walks are the same
        // pre-order traversal - children, extraPlans, expressions - so an ordinal is
        // identical on both sides whenever the two trees render the occurrences in the
        // same relative order)
        IdentityHashMap<Plan, Integer> relationOrdinals = new IdentityHashMap<>();
        // CTE references are named DIFFERENTLY on the two sides of a legitimate replay:
        // the frozen planSql is the DECOMPILED plan and the decompiler regenerates
        // every WITH alias (the caller's `ws_wh` becomes `t_4`), so a CTE reference is
        // identified by its CTE's DEFINITION ORDER index instead of the alias - the
        // same trick the occurrence ordinal plays for table aliases (see
        // SPMPlanTreeSupport#relationNameOf).
        Map<String, Integer> cteOrder = cteDefinitionOrder(plan);
        assignRelationOrdinals(plan, new HashMap<>(), relationOrdinals, cteOrder,
                Collections.newSetFromMap(new IdentityHashMap<>()));
        collectRowLimits(plan, out, relationOrdinals, cteOrder,
                Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static void collectRowLimits(Plan plan, Map<String, Integer> out,
            IdentityHashMap<Plan, Integer> relationOrdinals,
            Map<String, Integer> cteOrder, Set<Plan> visited) {
        if (plan == null || !visited.add(plan)) {
            return;
        }
        if (plan instanceof LogicalLimit) {
            out.merge(rowLimitKey(((LogicalLimit<?>) plan).getLimit(),
                    ((LogicalLimit<?>) plan).getOffset(), plan, relationOrdinals, cteOrder),
                    1, Integer::sum);
        } else if (plan instanceof LogicalTopN) {
            out.merge(rowLimitKey(((LogicalTopN<?>) plan).getLimit(),
                    ((LogicalTopN<?>) plan).getOffset(), plan, relationOrdinals, cteOrder),
                    1, Integer::sum);
        }
        for (Plan child : plan.children()) {
            collectRowLimits(child, out, relationOrdinals, cteOrder, visited);
        }
        for (Plan extra : plan.extraPlans()) {
            collectRowLimits(extra, out, relationOrdinals, cteOrder, visited);
        }
        for (Expression expression : plan.getExpressions()) {
            collectRowLimits(expression, out, relationOrdinals, cteOrder, visited);
        }
    }

    /** Recurses one expression tree looking for subquery plans (see walkSubqueryPlans). */
    private static void collectRowLimits(Expression expression, Map<String, Integer> out,
            IdentityHashMap<Plan, Integer> relationOrdinals,
            Map<String, Integer> cteOrder, Set<Plan> visited) {
        if (expression instanceof SubqueryExpr) {
            collectRowLimits(((SubqueryExpr) expression).getQueryPlan(), out,
                    relationOrdinals, cteOrder, visited);
        }
        if (expression instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                collectRowLimits(replaced, out, relationOrdinals, cteOrder, visited);
            }
        }
        for (Expression child : expression.children()) {
            collectRowLimits(child, out, relationOrdinals, cteOrder, visited);
        }
    }

    /**
     * Maps every CTE alias DEFINED in the tree (normalized, see normalizeCteName) to
     * its definition-order index, assigned in the walk order of walkPlansScoped. The
     * two sides of one replay - the caller's raw tree and the parsed frozen planSql -
     * spell the same CTE with different aliases (the decompiler regenerates every
     * derived alias), but the WITH structure and its order are preserved, so the
     * INDEX is the name-independent identity the row-limit keys compare. Distinct
     * CTEs keep distinct indices, so a cap moved between two CTEs still changes its
     * key.
     */
    private static Map<String, Integer> cteDefinitionOrder(Plan root) {
        Map<String, Integer> order = new HashMap<>();
        int[] next = {0};
        walkPlansScoped(root, Collections.emptySet(), (Plan node, Set<String> visibleCtes) -> {
            if (node instanceof LogicalCTE) {
                for (LogicalSubQueryAlias<Plan> alias
                        : ((LogicalCTE<? extends Plan>) node).getAliasQueries()) {
                    String name = normalizeCteName(alias.getAlias());
                    order.putIfAbsent(name, next[0]++);
                }
            }
        });
        return order;
    }

    /** Pre-order walk assigning each relation node its per-name occurrence ordinal. */
    private static void assignRelationOrdinals(Plan plan, Map<String, Integer> counters,
            IdentityHashMap<Plan, Integer> ordinals, Map<String, Integer> cteOrder,
            Set<Plan> visited) {
        if (plan == null || !visited.add(plan)) {
            return;
        }
        String name = relationNameOf(plan, cteOrder);
        if (name != null) {
            ordinals.put(plan, counters.merge(name, 1, Integer::sum));
        }
        for (Plan child : plan.children()) {
            assignRelationOrdinals(child, counters, ordinals, cteOrder, visited);
        }
        for (Plan extra : plan.extraPlans()) {
            assignRelationOrdinals(extra, counters, ordinals, cteOrder, visited);
        }
        for (Expression expression : plan.getExpressions()) {
            assignRelationOrdinals(expression, counters, ordinals, cteOrder, visited);
        }
    }

    /** Recurses one expression tree for relation ordinals (see assignRelationOrdinals). */
    private static void assignRelationOrdinals(Expression expression,
            Map<String, Integer> counters, IdentityHashMap<Plan, Integer> ordinals,
            Map<String, Integer> cteOrder, Set<Plan> visited) {
        if (expression instanceof SubqueryExpr) {
            assignRelationOrdinals(((SubqueryExpr) expression).getQueryPlan(), counters,
                    ordinals, cteOrder, visited);
        }
        if (expression instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                assignRelationOrdinals(replaced, counters, ordinals, cteOrder, visited);
            }
        }
        for (Expression child : expression.children()) {
            assignRelationOrdinals(child, counters, ordinals, cteOrder, visited);
        }
    }

    /**
     * The identity of one row-limiting node: its values, its ORDERED SLICE and the
     * occurrence-tagged relations it truncates (see the collectRowLimits javadoc).
     *
     * The slice is part of the identity: equal (limit, offset,
     * inputs) is NOT enough when the cap truncates a DIFFERENT slice of the same input -
     * an ASC/DESC change of the TopN picks other rows, and a manual plan may keep the cap
     * at a different position of the same query block. The key therefore carries the
     * cap's sort ARITY with each key's direction and NULL placement ("-" for an
     * order-free LogicalLimit, which truncates an arbitrary slice either way).
     *
     * Two further candidate dimensions are deliberately NOT part of the key because
     * they are not comparable ACROSS trees: the order keys' EXPRESSION TEXT (the caller
     * and the frozen replay are different trees whose sort keys reference per-tree slots
     * - the TPCH q02 cap sorts by c_16 in the frozen text and by
     * s_acctbal for the caller), and whether an AGGREGATE sits below the cap (a
     * scalar subquery exists as a materialized JOIN + aggregate in the decompiled frozen
     * text while the caller's own tree still carries it as an expression node - TPCH q02
     * was rejected by exactly that mismatch although both plans truncate the same slice).
     */
    private static String rowLimitKey(long limit, long offset, Plan limitNode,
            IdentityHashMap<Plan, Integer> relationOrdinals,
            Map<String, Integer> cteOrder) {
        Set<String> inputs = new TreeSet<>();
        collectRelationInputs(limitNode, relationOrdinals, cteOrder, inputs,
                Collections.newSetFromMap(new IdentityHashMap<>()));
        String slice = "-";
        if (limitNode instanceof LogicalTopN) {
            List<OrderKey> orderKeys = ((LogicalTopN<?>) limitNode).getOrderKeys();
            StringBuilder keys = new StringBuilder(String.valueOf(orderKeys.size()));
            for (OrderKey key : orderKeys) {
                keys.append(key.isAsc() ? '+' : '-')
                        .append(key.isNullFirst() ? 'n' : 'l').append(';');
            }
            slice = keys.toString();
        }
        return limit + ":" + offset + ":" + slice + ":" + String.join(",", inputs);
    }

    /**
     * The OCCURRENCE-TAGGED name of every relation beneath a row-limiting node (children,
     * CTE bodies and expression-owned subquery plans included): db.t#N with N the
     * node's per-name ordinal in the tree's walk order. A subtree without a relation (a
     * FROM-less child) contributes nothing - the key then identifies the cap by its
     * values alone, exactly as the caller's equivalent cap does.
     */
    private static void collectRelationInputs(Plan plan,
            IdentityHashMap<Plan, Integer> relationOrdinals,
            Map<String, Integer> cteOrder, Set<String> out,
            Set<Plan> visited) {
        if (plan == null || !visited.add(plan)) {
            return;
        }
        String name = relationNameOf(plan, cteOrder);
        if (name != null) {
            Integer ordinal = relationOrdinals.get(plan);
            out.add(ordinal == null ? name : name + "#" + ordinal);
        }
        for (Plan child : plan.children()) {
            collectRelationInputs(child, relationOrdinals, cteOrder, out, visited);
        }
        for (Plan extra : plan.extraPlans()) {
            collectRelationInputs(extra, relationOrdinals, cteOrder, out, visited);
        }
        for (Expression expression : plan.getExpressions()) {
            collectRelationInputs(expression, relationOrdinals, cteOrder, out, visited);
        }
    }

    /** Recurses one expression tree for relation inputs (see collectRelationInputs). */
    private static void collectRelationInputs(Expression expression,
            IdentityHashMap<Plan, Integer> relationOrdinals,
            Map<String, Integer> cteOrder, Set<String> out,
            Set<Plan> visited) {
        if (expression instanceof SubqueryExpr) {
            collectRelationInputs(((SubqueryExpr) expression).getQueryPlan(),
                    relationOrdinals, cteOrder, out, visited);
        }
        if (expression instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                collectRelationInputs(replaced, relationOrdinals, cteOrder, out, visited);
            }
        }
        for (Expression child : expression.children()) {
            collectRelationInputs(child, relationOrdinals, cteOrder, out, visited);
        }
    }

    /**
     * The identity name of one relation node used by the row-limit keys, or null when
     * the node is not a relation.
     *
     * A one-part relation bound by a CTE alias is named by its CTE's DEFINITION ORDER
     * index ("cte#N"), not by the alias: the frozen planSql replayed for an
     * identical-text baseline is the DECOMPILED plan, and the decompiler regenerates
     * every WITH alias (the caller's `ws_wh` becomes `t_4`) while preserving the WITH
     * structure and its order (see SPMPlanTreeSupport#cteDefinitionOrder). Comparing
     * the alias verbatim made the identical query fail its own cap contract. The
     * index keeps distinct CTEs distinct, so a cap moved between two CTEs still
     * changes its key.
     */
    private static String relationNameOf(Plan plan, Map<String, Integer> cteOrder) {
        if (plan instanceof UnboundRelation) {
            List<String> parts = ((UnboundRelation) plan).getNameParts();
            if (parts == null || parts.isEmpty()) {
                return null;
            }
            if (parts.size() == 1 && cteOrder != null) {
                Integer cteIndex = cteOrder.get(normalizeCteName(parts.get(0)));
                if (cteIndex != null) {
                    return "cte#" + cteIndex;
                }
            }
            return String.join(".", parts);
        }
        if (plan instanceof LogicalCatalogRelation) {
            return ((LogicalCatalogRelation) plan).getTable().getNameWithFullQualifiers();
        }
        if (plan instanceof UnboundTVFRelation) {
            return ((UnboundTVFRelation) plan).getFunctionName();
        }
        return null;
    }

    // ==================== whole-tree transform (parameterize / substitute) ====================

    /**
     * Rebuilds the whole plan tree applying transform to every literal-carrying
     * expression of every plan node (in bottom-up order). Nodes whose type is not
     * explicitly handled are rebuilt through withChildren only (their
     * expressions are kept untouched, which only narrows what can be parameterized /
     * substituted - never breaks the tree).
     *
     * @param plan      the parsed (unbound) plan tree
     * @param transform the expression transform (parameterize or substitute)
     * @return the rebuilt tree (the original instance when nothing changed)
     */
    public static LogicalPlan transform(LogicalPlan plan, ExprTransform transform) {
        Plan result = plan.accept(new TreeTransformer(), transform);
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /** Plan visitor that rebuilds every node, transforming its expressions. */
    private static class TreeTransformer extends PlanVisitor<Plan, ExprTransform> {

        /**
         * GROUP BY mirror records (see mirrorProjectItem): a group-by expression's
         * pre-parameterization text mapped to its transformed self. A select item spelling
         * the SAME expression must reuse those placeholder ids, or the parameterized tree
         * fails NormalizeAggregate ("PROJECT expression ... must appear in the GROUP BY
         * clause"; tpcds q23_1 groups by substr(i_item_desc, 1, 30) and selects the same
         * expression, whose literals otherwise receive their own ids and make the two
         * copies structurally different). Records stay consumable for the projection
         * directly above the aggregate and are dropped once its items were processed.
         */
        private final java.util.List<GroupByMirror> groupByMirrors = new java.util.ArrayList<>();

        /** One recorded group-by mirror (pre-text + the transformed expression). */
        private static final class GroupByMirror {
            private final String text;
            private final Expression expression;

            private GroupByMirror(String text, Expression expression) {
                this.text = text;
                this.expression = expression;
            }
        }

        @Override
        public Plan visit(Plan plan, ExprTransform transform) {
            // LogicalUsingJoin.accept() dispatches to this generic visit (PlanVisitor has no
            // using-join overload), so the ASOF MATCH_CONDITION is handled here: it is
            // stored OUTSIDE children() and getExpressions() and would otherwise keep its
            // literal concrete.
            if (plan instanceof LogicalUsingJoin) {
                return visitUsingJoin((LogicalUsingJoin<?, ?>) plan, transform);
            }
            List<Plan> children = plan.children();
            boolean changed = false;
            List<Plan> newChildren = new ArrayList<>(children.size());
            for (Plan child : children) {
                Plan newChild = child.accept(this, transform);
                newChildren.add(newChild);
                if (newChild != child) {
                    changed = true;
                }
            }
            if (!changed) {
                return plan;
            }
            try {
                return plan.withChildren(newChildren);
            } catch (RuntimeException e) {
                // plan type without withChildren -> keep the original children
                return plan;
            }
        }

        @Override
        public Plan visitLogicalCTE(LogicalCTE<? extends Plan> cte, ExprTransform transform) {
            // the main query subtree, then the CTE bodies (kept in aliasQueries, not in
            // children()) - a WHERE inside a CTE definition is transformed too.
            // A ScopedTransform needs per-scope contexts here: the main query sees every
            // alias of this WITH, alias body i only the aliases defined before it.
            ExprTransform mainContext = transform instanceof ScopedTransform
                    ? ((ScopedTransform) transform).enterCteMain(cte) : transform;
            Plan newChild = cte.child(0) == null ? null : cte.child(0).accept(this, mainContext);
            boolean childChanged = newChild != cte.child(0);
            List<LogicalSubQueryAlias<Plan>> newAliasQueries =
                    new ArrayList<>(cte.getAliasQueries().size());
            boolean aliasChanged = false;
            for (int i = 0; i < cte.getAliasQueries().size(); i++) {
                LogicalSubQueryAlias<Plan> aliasQuery = cte.getAliasQueries().get(i);
                ExprTransform aliasContext = transform instanceof ScopedTransform
                        ? ((ScopedTransform) transform).enterCteAlias(cte, i) : transform;
                Plan newAliasQuery = aliasQuery.accept(this, aliasContext);
                newAliasQueries.add((LogicalSubQueryAlias<Plan>) newAliasQuery);
                if (newAliasQuery != aliasQuery) {
                    aliasChanged = true;
                }
            }
            if (!childChanged && !aliasChanged) {
                return cte;
            }
            try {
                return new LogicalCTE<Plan>(cte.isRecursive(), newAliasQueries, newChild);
            } catch (RuntimeException e) {
                return cte;
            }
        }

        @Override
        public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias,
                ExprTransform transform) {
            // A derived table / CTE body opens a nested QUERY BLOCK (see
            // ExprTransform#enterQueryBlock): its literals must be able to distinguish
            // themselves from equally shaped literals of the outer block or of a sibling.
            // The numbering is per rebuild and both trees of a baseline traverse the same
            // structure, so corresponding blocks keep corresponding ids.
            transform.enterQueryBlock();
            try {
                Plan child = alias.child() == null ? null : alias.child().accept(this, transform);
                if (child == alias.child()) {
                    return alias;
                }
                return alias.withChildren(java.util.List.of(child));
            } finally {
                transform.exitQueryBlock();
            }
        }

        @Override
        public Plan visitUnboundRelation(UnboundRelation relation, ExprTransform transform) {
            // of the whole-tree transforms, only namespace qualification rewrites relation
            // references; parameterization / substitution leave them untouched
            return transform instanceof QualifyTransform
                    ? ((QualifyTransform) transform).qualify(relation) : relation;
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project,
                ExprTransform transform) {
            int mirrorMark = groupByMirrors.size();
            Plan child = project.child().accept(this, transform);
            boolean changed = child != project.child();
            List<NamedExpression> newProjects = transformProjectItems(project.getProjects(),
                    transform, mirrorMark);
            dropGroupByMirrorsFrom(mirrorMark);
            if (newProjects != project.getProjects()) {
                changed = true;
            }
            if (!changed) {
                return project;
            }
            return project.withProjectsAndChild(newProjects, child);
        }

        /**
         * transformNamed with the GROUP BY mirror: an item whose pre-parameterization value
         * text equals a group-by expression recorded by the aggregate below inherits that
         * expression WITH its placeholder ids, keeping the two structurally equal (the
         * analyzer requires a select expression to literally reappear in the GROUP BY: the
         * ids allocated for one copy cannot be re-derived for the other, see
         * mirrorProjectItem).
         */
        private List<NamedExpression> transformProjectItems(List<NamedExpression> expressions,
                ExprTransform transform, int mirrorMark) {
            boolean changed = false;
            List<NamedExpression> newExpressions = new ArrayList<>(expressions.size());
            for (int i = 0; i < expressions.size(); i++) {
                NamedExpression expression = expressions.get(i);
                NamedExpression mirrored = mirrorProjectItem(expression, mirrorMark);
                if (mirrored != null) {
                    newExpressions.add(mirrored);
                    changed = true;
                    continue;
                }
                // the item's POSITION in the list is part of the placeholder identity (see
                // ExprTransform#enterProjectionItem)
                transform.enterProjectionItem(i);
                Expression transformed;
                try {
                    transformed = transform.apply(expression);
                } finally {
                    transform.exitProjectionItem();
                }
                if (transformed instanceof NamedExpression && transformed != expression) {
                    newExpressions.add((NamedExpression) transformed);
                    changed = true;
                } else {
                    newExpressions.add(expression);
                }
            }
            return changed ? newExpressions : expressions;
        }

        /** One item rewritten to the recorded group-by expression, or null when none matches. */
        private NamedExpression mirrorProjectItem(NamedExpression item, int mirrorMark) {
            // the raw parse tree wraps select items in UnboundAlias, the analyzed tree in
            // Alias: both expose the aliased expression as their only child
            Expression value = item instanceof Alias || item instanceof UnboundAlias
                    ? item.child(0) : item;
            String text = value.toString();
            Expression mirror = null;
            for (int i = groupByMirrors.size() - 1; i >= mirrorMark; i--) {
                if (groupByMirrors.get(i).text.equals(text)) {
                    mirror = groupByMirrors.get(i).expression;
                    break;
                }
            }
            if (mirror == null) {
                return null;
            }
            if (item instanceof Alias || item instanceof UnboundAlias) {
                return (NamedExpression) item.withChildren(java.util.List.of(mirror));
            }
            return mirror instanceof NamedExpression ? (NamedExpression) mirror : null;
        }

        private void dropGroupByMirrorsFrom(int mirrorMark) {
            while (groupByMirrors.size() > mirrorMark) {
                groupByMirrors.remove(groupByMirrors.size() - 1);
            }
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter,
                ExprTransform transform) {
            Plan child = filter.child().accept(this, transform);
            boolean changed = child != filter.child();
            Set<Expression> newConjuncts = transformConjuncts(filter.getConjuncts(), transform);
            if (newConjuncts != filter.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return filter;
            }
            return filter.withConjunctsAndChild(newConjuncts, child);
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                ExprTransform transform) {
            // A literal can live directly in a JOIN's ON clause (e.g.
            // "a JOIN b ON a.k = b.k AND a.cat = 'x'"); it is NOT pushed into a Filter
            // on the unbound (raw) tree that parameterization runs over. Without handling
            // the join's own conjuncts here those literals would stay concrete in the
            // parameterized bind tree, so a structurally identical query with a different
            // literal would fail the Level 3 structural match and never hit the baseline.
            Plan left = join.left().accept(this, transform);
            Plan right = join.right().accept(this, transform);
            List<Expression> hash = transformExprs(join.getHashJoinConjuncts(), transform);
            List<Expression> other = transformExprs(join.getOtherJoinConjuncts(), transform);
            List<Expression> mark = transformExprs(join.getMarkJoinConjuncts(), transform);
            boolean changed = left != join.left() || right != join.right()
                    || hash != join.getHashJoinConjuncts()
                    || other != join.getOtherJoinConjuncts()
                    || mark != join.getMarkJoinConjuncts();
            if (!changed) {
                return join;
            }
            return join.withConjunctsChildren(hash, other, mark, left, right,
                    new JoinReorderContext());
        }

        /**
         * Rebuilds an ASOF / USING join, transforming the MATCH_CONDITION. The condition is
         * stored in matchCondition, OUTSIDE both children() and
         * getExpressions() (which returns the USING slots), so the generic pass would
         * leave its literal concrete on the bind side - the Level 3 match would then
         * compare only the USING slots and accept a user variant with a different
         * temporal boundary, replaying the captured one.
         */
        private Plan visitUsingJoin(LogicalUsingJoin<? extends Plan, ? extends Plan> join,
                ExprTransform transform) {
            Plan left = join.left().accept(this, transform);
            Plan right = join.right().accept(this, transform);
            Optional<Expression> match = join.getMatchCondition();
            Optional<Expression> newMatch = match.isPresent()
                    ? Optional.of(transform.apply(match.get())) : match;
            if (left == join.left() && right == join.right() && newMatch.equals(match)) {
                return join;
            }
            return new LogicalUsingJoin<>(join.getJoinType(), left, right,
                    join.getUsingSlots(), newMatch, join.getDistributeHint());
        }

        @Override
        public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, ExprTransform transform) {
            Plan child = having.child().accept(this, transform);
            boolean changed = child != having.child();
            Set<Expression> newConjuncts = transformConjuncts(having.getConjuncts(), transform);
            if (newConjuncts != having.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return having;
            }
            return having.withConjunctsAndChild(newConjuncts, child);
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                ExprTransform transform) {
            Plan child = aggregate.child().accept(this, transform);
            boolean changed = child != aggregate.child();
            int mirrorMark = groupByMirrors.size();
            List<Expression> newGroupBy = new ArrayList<>(aggregate.getGroupByExpressions().size());
            for (Expression groupByExpr : aggregate.getGroupByExpressions()) {
                Expression newExpr = transform.apply(groupByExpr);
                newGroupBy.add(newExpr);
                if (newExpr != groupByExpr) {
                    changed = true;
                    // a select item spelling the same expression must reuse these ids (see
                    // mirrorProjectItem): the analyzer requires the select expression to
                    // literally reappear in the GROUP BY of the parameterized tree
                    groupByMirrors.add(new GroupByMirror(groupByExpr.toString(), newExpr));
                }
            }
            // The SELECT list of an aggregate query lives in the aggregate's OWN output
            // expressions for aggregate-rooted shapes, and in the PROJECT above it for
            // project-rooted shapes: the records stay alive for BOTH consumers and are
            // dropped by the enclosing projection's own cleanup after its items ran.
            List<NamedExpression> newOutput = transformProjectItems(
                    aggregate.getOutputExpressions(), transform, mirrorMark);
            if (newOutput != aggregate.getOutputExpressions()) {
                changed = true;
            }
            if (!changed) {
                return aggregate;
            }
            return aggregate.withChildGroupByAndOutput(newGroupBy, newOutput, child);
        }

        @Override
        public Plan visitUnboundOneRowRelation(UnboundOneRowRelation oneRow,
                ExprTransform transform) {
            List<NamedExpression> newProjects = transformNamed(oneRow.getProjects(), transform);
            if (newProjects == oneRow.getProjects()) {
                return oneRow;
            }
            return new UnboundOneRowRelation(oneRow.getRelationId(), newProjects);
        }

        @Override
        public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRow,
                ExprTransform transform) {
            List<NamedExpression> newProjects = transformNamed(oneRow.getProjects(), transform);
            if (newProjects == oneRow.getProjects()) {
                return oneRow;
            }
            return new LogicalOneRowRelation(oneRow.getRelationId(), newProjects);
        }

        @Override
        public Plan visitLogicalSort(LogicalSort<? extends Plan> sort,
                ExprTransform transform) {
            // ORDER BY keys can carry literals (e.g. substr(w_warehouse_name, 1, 20) in a
            // TPCDS query): they must be parameterized / substituted like any other
            // expression, otherwise the rewritten tree would keep the bind-side order key
            // while group-by / project got the user's value -> analyze error / wrong order.
            //
            // EXCEPTION: an ORDER BY whose key is a BARE integer literal is an ORDINAL
            // (position reference, e.g. ORDER BY 1, 2), resolved by BindExpression during
            // analyze. It must NOT be parameterized: parameterizing would turn the ordinal
            // into a placeholder function and the optimizer would then sort by the
            // constant value (projecting it as an extra column) instead of by the
            // referenced output column - silently changing the query's row order.
            Plan child = sort.child().accept(this, transform);
            boolean changed = child != sort.child();
            List<OrderKey> orderKeys = sort.getOrderKeys();
            List<OrderKey> newOrderKeys = new ArrayList<>(orderKeys.size());
            for (OrderKey orderKey : orderKeys) {
                Expression keyExpr = orderKey.getExpr();
                Expression newExpr = keyExpr instanceof IntegerLikeLiteral
                        ? keyExpr : transform.apply(keyExpr);
                if (newExpr != keyExpr) {
                    changed = true;
                }
                newOrderKeys.add(orderKey.withExpression(newExpr));
            }
            if (!changed) {
                return sort;
            }
            return sort.withOrderKeysAndChild(newOrderKeys, child);
        }

        @Override
        public Plan visitLogicalQualify(LogicalQualify<? extends Plan> qualify,
                ExprTransform transform) {
            // QUALIFY <window-predicate>: the predicates live on the LogicalQualify node
            // itself (e.g. "row_number() over (...) = 1") and would otherwise keep their
            // literals concrete in the parameterized tree, so a user query with a
            // different QUALIFY threshold never matches the baseline.
            Plan child = qualify.child().accept(this, transform);
            boolean changed = child != qualify.child();
            Set<Expression> newConjuncts = transformConjuncts(qualify.getConjuncts(), transform);
            if (newConjuncts != qualify.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return qualify;
            }
            return new LogicalQualify<Plan>(newConjuncts, child);
        }

        @Override
        public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate,
                ExprTransform transform) {
            // LATERAL VIEW / UNNEST: the generator arguments (e.g.
            // explode(split('a,b,c', ','))) decide which rows/values the generator emits -
            // they are NOT predicates (no WHERE-like semantics), so a different argument
            // means a different query. They are therefore intentionally left concrete and
            // never parameterized: SPM only matches a user query carrying the exact same
            // generator argument. The conjuncts field is the only WHERE-like part of this
            // node (empty at parse time), so it is still transformed for future-proofing.
            Plan child = generate.child().accept(this, transform);
            boolean changed = child != generate.child();
            List<Expression> newConjuncts = transformExprs(generate.getConjuncts(), transform);
            if (newConjuncts != generate.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return generate;
            }
            return new LogicalGenerate<Plan>(generate.getGenerators(),
                    generate.getGeneratorOutput(), generate.getExpandColumnAlias(),
                    newConjuncts, child);
        }

        @Override
        public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat,
                ExprTransform transform) {
            // GROUPING SETS / ROLLUP / CUBE.
            // The outputExpressions are the SELECT-list items of the query block (constants
            // like "SELECT 5 AS tag" live here) and need parameterizes select items
            // like any other expression -> parameterize them so a user value change can
            // match and be substituted. The groupingSets are the grouping keys (GROUP BY
            // columns); we does not traverse them -> they stay concrete.
            Plan child = repeat.child().accept(this, transform);
            boolean changed = child != repeat.child();
            List<NamedExpression> newOutput = transformNamed(repeat.getOutputExpressions(),
                    transform);
            if (newOutput != repeat.getOutputExpressions()) {
                changed = true;
            }
            if (!changed) {
                return repeat;
            }
            // NOTE: the overload that takes the grouping-id VALUES rebuilds through a
            // constructor that forces withInProjection=true, which flips toDigest() from
            // "SELECT <outputs> FROM <child>" to "<child>". A rebuild must keep the
            // parse-time rendering state: only the qualification below changes a digest,
            // not the fact that a node was rebuilt on the way. The 4-arg overload reuses
            // the existing grouping-id values and keeps withInProjection as-is.
            return repeat.withGroupingIdValues(repeat.getGroupingSets(), newOutput,
                    repeat.getGroupingId().orElse(null), child);
        }
    }

    /**
     * Transforms an ordinary (non-Named) expression list in place-free way; returns the
     * original list when no element changed.
     *
     * Used for expression LISTS that are not SELECT items and carry no output name/alias
     * of their own, e.g. a join's conjuncts or a LogicalGenerate's conjuncts. Example -
     * the ON / residual predicates of
     *
     *     SELECT ... FROM t1 JOIN t2 ON t1.k = t2.k WHERE t2.a > 100 AND t2.b = 'x'
     *
     * are parameterized one conjunct at a time, so a user query whose literal differs
     * (e.g. t2.a > 200) still structurally matches the baseline. The list keeps its
     * order (no Set normalization - join conjunct order is meaningful).
     *
     * @param expressions the expression list to transform
     * @param transform   the per-expression transform (parameterize or substitute)
     * @return the transformed list, or the original list instance when nothing changed
     */
    private static List<Expression> transformExprs(List<Expression> expressions,
            ExprTransform transform) {
        boolean changed = false;
        List<Expression> newExpressions = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            Expression transformed = transform.apply(expression);
            newExpressions.add(transformed);
            if (transformed != expression) {
                changed = true;
            }
        }
        return changed ? newExpressions : expressions;
    }

    /**
     * Transforms a NamedExpression list (SELECT items / aggregate outputs) in place-free
     * way; returns the original list when no element changed.
     *
     * A NamedExpression is an expression paired with an output name/alias (e.g.
     * Alias(expr, name)). The transform runs over the WHOLE NamedExpression - including
     * the alias name itself when it is derived from the expression text - so its inner
     * literals are parameterized while the output column name is kept consistent with
     * the (also parameterized) bind side. Example - the SELECT list of
     *
     *     SELECT l_returnflag, sum(l_quantity * 2) AS total FROM lineitem
     *     GROUP BY l_returnflag
     *
     * is transformed per item: the plain column l_returnflag is unchanged, while the
     * aggregate item's constant 2 becomes a placeholder
     * (sum(l_quantity * SpmConstVar(1, 2)) AS total), so a similar user query with a
     * different multiplier still matches. Only a transform result that stays a
     * NamedExpression is adopted (an Alias must not collapse into a bare expression,
     * which would drop the output name).
     *
     * @param expressions the NamedExpression list (projects / aggregate outputs) to
     *                    transform
     * @param transform   the per-expression transform (parameterize or substitute)
     * @return the transformed list, or the original list instance when nothing changed
     */
    private static List<NamedExpression> transformNamed(List<NamedExpression> expressions,
            ExprTransform transform) {
        boolean changed = false;
        List<NamedExpression> newExpressions = new ArrayList<>(expressions.size());
        for (int i = 0; i < expressions.size(); i++) {
            NamedExpression expression = expressions.get(i);
            // the item's POSITION in the list is part of the placeholder identity (see
            // ExprTransform#enterProjectionItem)
            transform.enterProjectionItem(i);
            Expression transformed;
            try {
                transformed = transform.apply(expression);
            } finally {
                transform.exitProjectionItem();
            }
            if (transformed instanceof NamedExpression && transformed != expression) {
                newExpressions.add((NamedExpression) transformed);
                changed = true;
            } else {
                newExpressions.add(expression);
            }
        }
        return changed ? newExpressions : expressions;
    }

    /**
     * Transforms a conjunct set in a deterministic order; returns the original set when
     * no element changed. The conjuncts of a filter / having are a Set whose iteration
     * order is not stable across separately parsed trees, so they are ordered by SQL
     * text before transforming: the bind tree and the (separately parsed) plan tree of
     * one baseline then parameterize their conjuncts in the same order and the
     * placeholder ids stay aligned (no cross-tree id skew for identical conjuncts).
     */
    private static Set<Expression> transformConjuncts(Set<Expression> conjuncts,
            ExprTransform transform) {
        List<Expression> ordered = new ArrayList<>(conjuncts);
        ordered.sort(Comparator.comparing(Expression::toSql));
        boolean changed = false;
        Set<Expression> newConjuncts = new LinkedHashSet<>(conjuncts.size());
        for (Expression conjunct : ordered) {
            Expression transformed = transform.apply(conjunct);
            newConjuncts.add(transformed);
            if (transformed != conjunct) {
                changed = true;
            }
        }
        return changed ? newConjuncts : conjuncts;
    }

    // ==================== namespace qualification (creation catalog / database) ====================

    /**
     * Returns a copy of the tree where every UNQUALIFIED relation reference (nameParts.size()
     * == 1) that binds to a BASE TABLE is prefixed with the given catalog / database, so the
     * SPM digest / hash key of "FROM t" depends on the query's effective namespace. The copy
     * is only used to compute the matching key / compare against a baseline - it is never
     * analyzed, optimized or executed.
     *
     * Without this, "SELECT ... FROM t" has the same key under db1 and db2: a baseline
     * captured under db1 would silently match the same text executed under db2 and - because
     * the frozen planSql is fully qualified - keep executing against db1.t.
     *
     * A reference to a CTE alias is NOT prefixed: its binding comes from the WITH clause of
     * the query itself and therefore means the same thing in every database (prefixing it
     * made "WITH c AS (...) SELECT * FROM c" match only in the database it was created in).
     * Whether a single-part name is a CTE reference is decided with the analyzer's scoping
     * rules (AnalyzeCTE): an alias body sees the aliases defined before it, plus itself when
     * it is a real recursive CTE (WITH RECURSIVE plus a self-reference in its body); a CTE's
     * main query sees every alias of that WITH; nested WITH nodes extend the enclosing
     * scope; expression subqueries inherit the scope of the point they appear in
     * (SubExprAnalyzer). A name that is NOT a visible alias - including a forward reference
     * to a later alias and a self reference under a plain (non-RECURSIVE) WITH, both of which
     * bind as base tables - is always prefixed, so this can only narrow the match key, never
     * make a base-table reference namespace-independent.
     *
     * @param plan    the parsed (unbound) tree
     * @param catalog the effective catalog, may be null / empty (then only the db is prefixed)
     * @param db      the effective database; when null / empty the tree is returned as-is
     * @return the qualified tree (a rebuilt copy; the argument is not modified)
     */
    public static LogicalPlan namespaceQualified(LogicalPlan plan, String catalog, String db) {
        boolean hasCatalog = catalog != null && !catalog.isEmpty();
        boolean hasDb = db != null && !db.isEmpty();
        // A TWO-part name (db.t) is relative to the current CATALOG, not to the current
        // database, so it must be prefixed whenever the catalog is known - a session can
        // switch to cat1 WITHOUT a USE db, and an unprefixed db.t would key the same text
        // under cat2 to the same digest while the frozen SQL still reads cat1.db.t. Only
        // when NEITHER the catalog nor the db is known can nothing be made
        // namespace-independent.
        if (plan == null || (!hasCatalog && !hasDb)) {
            return plan;
        }
        Plan result = plan.accept(new TreeTransformer(), new QualifyTransform(catalog, db));
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /**
     * Namespace-qualification scope: prefixes single-part base-table references with the
     * effective [catalog, db] and keeps references to the CTE aliases visible at the current
     * point verbatim. Immutable: entering a CTE scope returns a new instance whose visible
     * set is the enclosing one plus the aliases visible in that scope.
     */
    private static final class QualifyTransform implements ScopedTransform {
        private final String catalog;
        private final String db;
        /** normalized names of the CTE aliases visible at the current point */
        private final Set<String> visibleCtes;

        QualifyTransform(String catalog, String db) {
            this(catalog, db, Collections.emptySet());
        }

        private QualifyTransform(String catalog, String db, Set<String> visibleCtes) {
            this.catalog = catalog;
            this.db = db;
            this.visibleCtes = visibleCtes;
        }

        @Override
        public Expression apply(Expression expr) {
            return qualifyExpression(expr, this);
        }

        @Override
        public ExprTransform enterCteMain(LogicalCTE<? extends Plan> cte) {
            Set<String> extended = new LinkedHashSet<>(visibleCtes);
            for (LogicalSubQueryAlias<Plan> alias : cte.getAliasQueries()) {
                extended.add(normalizeCteName(alias.getAlias()));
            }
            return new QualifyTransform(catalog, db, Collections.unmodifiableSet(extended));
        }

        @Override
        public ExprTransform enterCteAlias(LogicalCTE<? extends Plan> cte, int aliasIndex) {
            List<LogicalSubQueryAlias<Plan>> aliases = cte.getAliasQueries();
            Set<String> extended = new LinkedHashSet<>(visibleCtes);
            for (int i = 0; i < aliasIndex; i++) {
                extended.add(normalizeCteName(aliases.get(i).getAlias()));
            }
            // a self reference binds to the WITH clause only in a real recursive CTE;
            // under a plain WITH it is an ordinary base-table reference
            LogicalSubQueryAlias<Plan> alias = aliases.get(aliasIndex);
            if (cte.isRecursive() && alias.isRecursiveCte()) {
                extended.add(normalizeCteName(alias.getAlias()));
            }
            return new QualifyTransform(catalog, db, Collections.unmodifiableSet(extended));
        }

        /** Prefixes a one- or two-part relation with the effective namespace. */
        Plan qualify(UnboundRelation relation) {
            List<String> parts = relation.getNameParts();
            if (parts.size() > 2) {
                return relation; // fully qualified (catalog.db.table): nothing to add
            }
            if (parts.size() == 1 && isVisibleCte(parts.get(0))) {
                return relation; // bound by a WITH clause, not a base-table reference
            }
            // A one-part name is relative to the current db, but a TWO-part name is only
            // relative to the current CATALOG ("db.t" means current_catalog.db.t):
            // leaving "db.t" verbatim would let a baseline created in cat1 match the
            // same text executed in cat2, after which the frozen fully-qualified replay
            // keeps reading cat1.db.t. Only three-part names are complete.
            List<String> qualified = new ArrayList<>(3);
            if (parts.size() == 1) {
                if (db == null || db.isEmpty()) {
                    // A one-part name needs the DATABASE to become absolute; prefixing only
                    // the catalog would turn the table name into a database name.
                    return relation;
                }
                if (catalog != null && !catalog.isEmpty()) {
                    qualified.add(catalog);
                }
                qualified.add(db);
            } else {
                // two-part name: the catalog completes it
                if (catalog == null || catalog.isEmpty()) {
                    return relation;
                }
                qualified.add(catalog);
            }
            qualified.addAll(parts);
            try {
                return copyWithNameParts(relation, qualified);
            } catch (RuntimeException e) {
                return relation;
            }
        }

        /**
         * Rebuilds the relation with new name parts while preserving EVERY scan modifier
         * (partition list, tablet ids, hints, sample, index, scan params, snapshot):
         * dropping them here would hide a PARTITION(...) / TABLESAMPLE / FOR VERSION
         * selection from the Level 3 comparison and allow a baseline captured under a
         * different selection to match.
         */
        private static UnboundRelation copyWithNameParts(UnboundRelation relation,
                List<String> nameParts) {
            return new UnboundRelation(relation.getRelationId(), nameParts,
                    relation.getPartNames(), relation.isTempPart(), relation.getTabletIds(),
                    relation.getHints(), relation.getTableSample(), relation.getIndexName(),
                    relation.getScanParams(), relation.getIndexInSqlString(),
                    relation.getTableSnapshot());
        }

        private boolean isVisibleCte(String name) {
            return !visibleCtes.isEmpty() && visibleCtes.contains(normalizeCteName(name));
        }
    }

    /** Mirrors the analyzer's CTE name comparison (CTEContext.findCTEContext). */
    private static String normalizeCteName(String name) {
        int lowerCaseTableNames = GlobalVariable.lowerCaseTableNames;
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getCurrentCatalog() != null) {
            lowerCaseTableNames = ctx.getCurrentCatalog().getLowerCaseTableNames();
        }
        return lowerCaseTableNames != 0 ? name.toLowerCase(Locale.ROOT) : name;
    }

    /**
     * Qualifies the relations of every subquery plan owned by an expression, recursively
     * (IN / scalar / EXISTS subqueries anywhere in the tree). The subquery inherits the
     * qualification scope of the point it appears in, i.e. the CTE aliases visible there.
     */
    private static Expression qualifyExpression(Expression expr, QualifyTransform transform) {
        if (expr instanceof SubqueryExpr) {
            LogicalPlan subPlan = ((SubqueryExpr) expr).getQueryPlan();
            Plan qualified = subPlan.accept(new TreeTransformer(), transform);
            if (qualified instanceof LogicalPlan && qualified != subPlan) {
                return ((SubqueryExpr) expr).withSubquery((LogicalPlan) qualified);
            }
            return expr;
        }
        if (expr instanceof UnboundStar) {
            // SELECT * REPLACE((SELECT max(v) FROM u) AS k): the replacement payloads sit
            // OUTSIDE children(), so the generic recursion below never reached the
            // subquery plan they own. Creating a baseline in db1 and running the same
            // text under db2 kept the raw "u": digest and the Level-3 check matched (the
            // outer table was fixed) while the frozen SQL still read db1.u, so replay
            // returned db1's value. Rebuild the payloads under the CURRENT namespace and
            // CTE scope.
            UnboundStar star = (UnboundStar) expr;
            List<NamedExpression> replaced = star.getReplacedAlias();
            if (replaced.isEmpty()) {
                return expr;
            }
            boolean changed = false;
            List<NamedExpression> newReplaced = new ArrayList<>(replaced.size());
            for (NamedExpression replacement : replaced) {
                Expression newReplacement = qualifyExpression(replacement, transform);
                newReplaced.add(newReplacement instanceof NamedExpression
                        ? (NamedExpression) newReplacement : replacement);
                changed |= newReplacement != replacement;
            }
            return changed ? new UnboundStar(star.getQualifier(), star.getExceptedSlots(),
                    newReplaced, star.getIndexInSqlString()) : expr;
        }
        if (expr.children().isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(expr.children().size());
        for (Expression child : expr.children()) {
            Expression newChild = qualifyExpression(child, transform);
            newChildren.add(newChild);
            if (newChild != child) {
                changed = true;
            }
        }
        return changed ? expr.withChildren(newChildren) : expr;
    }

    // ==================== hint stripping (in-memory fallback tree) ====================

    /**
     * Strips the CAPTURED SET_VAR payloads from a plan tree: the root block, nested query
     * blocks, CTE bodies and expression subqueries. The frozen-text replay path re-parses
     * its planSql INCLUDING the hints deliberately; the in-memory fallback tree must not
     * re-apply the BASELINE's captured SET_VAR on top of a user query that came with
     * different session variables. A root-only peel let an INNER hint survive (e.g. a
     * plan-side SET_VAR(time_zone='+08:00') inside a scalar subquery) and the hint was
     * then applied during ordinary replay analysis, although the matching user query is
     * hint-free and runs under -08:00 - from_unixtime returned different values.
     *
     * PLAN-SELECTION hints are the opposite case and are KEPT: the baseline exists to
     * pin the authored plan, and the non-frozen fallback replays the parameterized TREE -
     * dropping an ORDERED or LEADING(...) hint here let the replay
     * choose a different join order than the plan the baseline was created to enforce
     * (the decompiler falls back to the authored SQL whenever the physical plan cannot
     * be rendered, e.g. a PhysicalAssertNumRows produced by a scalar subquery). SET_VAR
     * is the only hint class that carries the CREATOR's session state.
     *
     * @param plan the parameterized fallback tree
     * @return the tree without any SET_VAR payload (the original instance when there was none)
     */
    public static LogicalPlan stripSelectHints(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }
        Plan result = plan.accept(new HintStripper(), HINT_STRIP_EXPR);
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /**
     * TreeTransformer that removes the SET_VAR payloads from every LogicalSelectHint
     * wrapper (whilst descending into nested blocks and keeping the other hints). A
     * wrapper left without hints is dropped entirely, so a hint-free subtree keeps its
     * original shape.
     */
    private static class HintStripper extends TreeTransformer {
        @Override
        public Plan visit(Plan plan, ExprTransform transform) {
            if (!(plan instanceof LogicalSelectHint)) {
                return super.visit(plan, transform);
            }
            LogicalSelectHint<?> selectHint = (LogicalSelectHint<?>) plan;
            Plan child = selectHint.child(0);
            if (child == null) {
                return plan;
            }
            Plan strippedChild = child.accept(this, transform);
            List<SelectHint> kept = new ArrayList<>(selectHint.getHints().size());
            for (SelectHint hint : selectHint.getHints()) {
                if (!(hint instanceof SelectHintSetVar)) {
                    kept.add(hint);
                }
            }
            if (kept.isEmpty()) {
                return strippedChild;
            }
            return new LogicalSelectHint<>(ImmutableList.copyOf(kept), strippedChild);
        }
    }

    // ==================== replay-time context expression detection ====================

    /**
     * Whether the tree contains an expression whose value belongs to the CREATOR's
     * replay-time context: a session / user variable (@v) or current_user(),
     * session_user(), database(), connection_id(). Such leaves survive
     * parameterization, the creator-context optimization then resolves them to LITERALS,
     * and the frozen SQL persists the CREATOR's value - while matching still compares the
     * ORIGINAL unbound bind tree, so a global baseline (e.g.
     * SELECT current_user(), k FROM t WHERE k = 1) could match another user's
     * query and return the creator's identity.
     *
     * @param plan the parsed (unbound) tree
     * @return true when such an expression is found anywhere - every subquery plan
     *         (IN / EXISTS / scalar, coercions included), CTE bodies and an ASOF join's
     *         out-of-band MATCH_CONDITION are scanned
     */
    public static boolean containsReplayContextExpression(LogicalPlan plan) {
        if (plan == null) {
            return false;
        }
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (found[0]) {
                return;
            }
            if (node instanceof LogicalUsingJoin) {
                // matchCondition lives outside children() and getExpressions()
                Optional<Expression> matchCondition =
                        ((LogicalUsingJoin<?, ?>) node).getMatchCondition();
                if (matchCondition.isPresent()
                        && containsReplayContextExpression(matchCondition.get())) {
                    found[0] = true;
                    return;
                }
            }
            for (Expression expr : node.getExpressions()) {
                if (containsReplayContextExpression(expr)) {
                    found[0] = true;
                    return;
                }
            }
        });
        return found[0];
    }

    /** Whether an expression tree contains a replay-time context expression. */
    public static boolean containsReplayContextExpression(Expression expr) {
        if (expr instanceof Variable || expr instanceof UnboundVariable
                || expr instanceof CurrentUser || expr instanceof SessionUser
                || expr instanceof Database || expr instanceof ConnectionId
                || expr instanceof CurrentDate || expr instanceof CurrentTime
                || expr instanceof Now) {
            // a PARSED "@v" is an UnboundVariable, not the analyzed Variable: the guard
            // only knew the analyzed shape, so `WHERE k = @v` slipped through and the
            // creator-context optimization froze the CREATOR's value into the planSql
            // of a GLOBAL baseline. The BARE clock keywords
            // (CURRENT_DATE / CURRENT_TIME / CURRENT_TIMESTAMP / LOCALTIME /
            // LOCALTIMESTAMP) are not calls at all: the parser builds the bound
            // CurrentDate / CurrentTime / Now leaves directly
            // (LogicalPlanBuilder#visitCurrentDate etc.), so the name-based UnboundFunction
            // check below never saw them and a frozen baseline served the CREATE date for
            // every later match while the parenthesized forms were rejected.
            return true;
        }
        if (expr instanceof UnboundFunction) {
            // the same functions can reach the unbound tree as a plain function call
            // (e.g. "current_user()" with parentheses parses as UnboundFunction)
            String name = ((UnboundFunction) expr).getName();
            if (name != null) {
                switch (name.toLowerCase(Locale.ROOT)) {
                    case "current_user":
                    case "session_user":
                    case "database":
                    case "connection_id":
                    case "user":
                    case "current_catalog":
                    case "last_query_id":
                    case "version":
                        // user()/current_catalog()/last_query_id()/version() are the same
                        // creator / environment values: freezing them served every other
                        // session the creator's identity or an id that never repeats.
                        return true;
                    case "now":
                    case "current_timestamp":
                    case "utc_timestamp":
                    case "localtime":
                    case "localtimestamp":
                    case "current_date":
                    case "curdate":
                    case "current_time":
                    case "curtime":
                        // Every clock function is evaluated from the STATEMENT start time
                        // (see DateTimeAcquire: ConnectContext#getStartTimeInstant) and the
                        // optimizer folds it into a literal, which the decompiler stores in
                        // planFrozen SQL: a baseline for `SELECT now() AS ts FROM t` returned
                        // the CREATE timestamp on every later matching query, and
                        // `WHERE event_time < now()` replayed with a stale cutoff.
                        return true;
                    case "unix_timestamp":
                        // unix_timestamp() without arguments is the same statement clock;
                        // WITH an argument it is a pure function of that argument and stays
                        // freezable.
                        return expr.arity() == 0;
                    default:
                        break;
                }
            }
        }
        if (expr instanceof UnboundStar) {
            // SELECT * REPLACE(f(@v) AS v): the replaced-alias payloads are NOT expression
            // children of the star, so the recursive walk below never reached them and the
            // statement passed the guard. Scan the payloads explicitly.
            for (NamedExpression replaced : ((UnboundStar) expr).getReplacedAlias()) {
                if (containsReplayContextExpression(replaced)) {
                    return true;
                }
            }
        }
        if (expr instanceof SubqueryExpr) {
            if (containsReplayContextExpression(((SubqueryExpr) expr).getQueryPlan())) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (containsReplayContextExpression(child)) {
                return true;
            }
        }
        return false;
    }

    // ==================== whole-tree placeholder detection ====================

    /**
     * Whether the plan tree (including filters/having/projections and every subquery
     * plan reachable from an expression) still contains an unbound placeholder.
     *
     * @param plan the plan tree to scan
     * @return true when a SpmConstVar / SpmConstList is found anywhere
     */
    public static boolean containsPlaceholder(LogicalPlan plan) {
        return plan.accept(new PlaceholderScanVisitor(), null);
    }

    /** Plan visitor that scans every node's expressions (and subquery plans) for placeholders. */
    private static class PlaceholderScanVisitor extends PlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan instanceof LogicalUsingJoin) {
                // The USING join's matchCondition is OUT OF BAND: it is neither a child
                // nor in getExpressions(). A bind/plan pair whose ASOF boundary differs
                // only in an interval literal leaves a plan-only id in this field; the
                // in-memory fallback substitutes the original LogicalUsingJoin, so a
                // visitor that only walks USING slots + children would return an invalid
                // tree (an unresolved placeholder id) to analysis.
                Optional<Expression> matchCondition =
                        ((LogicalUsingJoin<?, ?>) plan).getMatchCondition();
                if (matchCondition.isPresent() && containsPlaceholder(matchCondition.get())) {
                    return true;
                }
            }
            for (Expression expr : plan.getExpressions()) {
                if (containsPlaceholder(expr)) {
                    return true;
                }
            }
            for (Plan child : plan.children()) {
                if (child.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitLogicalCTE(LogicalCTE<? extends Plan> cte, Void context) {
            if (cte.child(0) != null && cte.child(0).accept(this, context)) {
                return true;
            }
            for (LogicalSubQueryAlias<Plan> aliasQuery : cte.getAliasQueries()) {
                if (aliasQuery.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Whether an expression tree (recursing into subquery plans) contains a placeholder.
     */
    public static boolean containsPlaceholder(Expression expr) {
        if (expr instanceof SpmConstVar || expr instanceof SpmConstList) {
            return true;
        }
        if (expr instanceof SubqueryExpr) {
            if (containsPlaceholder(((SubqueryExpr) expr).getQueryPlan())) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (containsPlaceholder(child)) {
                return true;
            }
        }
        return false;
    }

    // ==================== frozen-tree placeholder detection (M3) ====================

    /**
     * Whether a plan tree (re-parsed from the frozen planSql) still carries an
     * unsubstituted placeholder call (_spm_const_var(id) / _spm_const_list(id) as a raw
     * UnboundFunction). Such a call would reach the analyzer as an unregistered function
     * and fail, so the rewrite must reject the tree instead of returning it.
     *
     * @param plan the frozen (re-parsed) plan tree to scan
     * @return true when an unsubstituted placeholder call is found anywhere
     */
    public static boolean containsFrozenPlaceholder(Plan plan) {
        return plan.accept(new FrozenPlaceholderScanVisitor(), null);
    }

    /** Plan visitor that scans every node's expressions (and subquery plans) for
     * unsubstituted frozen-tree placeholder calls. */
    private static class FrozenPlaceholderScanVisitor extends PlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan instanceof LogicalUsingJoin) {
                // Same out-of-band field as PlaceholderScanVisitor: an unresolved
                // placeholder id inside the ASOF boundary must reject the fallback tree.
                Optional<Expression> matchCondition =
                        ((LogicalUsingJoin<?, ?>) plan).getMatchCondition();
                if (matchCondition.isPresent() && containsFrozenPlaceholder(matchCondition.get())) {
                    return true;
                }
            }
            for (Expression expr : plan.getExpressions()) {
                if (containsFrozenPlaceholder(expr)) {
                    return true;
                }
            }
            for (Plan child : plan.children()) {
                if (child.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitLogicalCTE(LogicalCTE<? extends Plan> cte, Void context) {
            if (cte.child(0) != null && cte.child(0).accept(this, context)) {
                return true;
            }
            for (LogicalSubQueryAlias<Plan> aliasQuery : cte.getAliasQueries()) {
                if (aliasQuery.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }

    /** Whether an expression tree contains an unsubstituted frozen-tree placeholder call. */
    public static boolean containsFrozenPlaceholder(Expression expr) {
        if (SPMFrozenTreeReplacer.isUnsubstitutedPlaceholder(expr)) {
            return true;
        }
        if (expr instanceof SubqueryExpr) {
            if (containsFrozenPlaceholder(((SubqueryExpr) expr).getQueryPlan())) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (containsFrozenPlaceholder(child)) {
                return true;
            }
        }
        return false;
    }

    // ==================== whole-tree structural check (Level 3) ====================

    /**
     * Compares a parameterized bind tree with the user's original tree node by node and
     * expression by expression. Placeholder values are extracted from the user side into
     * placeholderValues (a repeated id must resolve to the same user value).
     *
     * @param bindPlan          the parameterized bind tree (contains placeholders)
     * @param userPlan          the user's original tree (contains real literals)
     * @param placeholderValues placeholder id -> user value (filled in here)
     * @return whether the two trees match
     */
    public static boolean check(LogicalPlan bindPlan, LogicalPlan userPlan,
            Map<Long, Expression> placeholderValues) {
        MatchAttempt attempt = MatchAttempt.begin();
        if (attempt == null) {
            // a NESTED match (a subquery plan reached from the visitor) rides the
            // enclosing attempt: its choices and the outer tree's share one placeholder
            // map, so only the OUTERMOST driver may retry
            return checkPlan(bindPlan, userPlan, placeholderValues, false);
        }
        try {
            Map<Long, Expression> entryState = new HashMap<>(placeholderValues);
            for (int i = 0; i < MatchAttempt.MAX_ATTEMPTS; i++) {
                attempt.resetPass();
                if (checkPlan(bindPlan, userPlan, placeholderValues, false)) {
                    return true;
                }
                placeholderValues.clear();
                placeholderValues.putAll(entryState);
                if (!attempt.advance()) {
                    return false;
                }
            }
            placeholderValues.clear();
            return false;
        } finally {
            attempt.end();
        }
    }

    /**
     * Level 3 check for one subquery-EXPRESSION plan pair. Inside a subquery's plan the
     * LIMIT / OFFSET fields are compared EXACTLY (see checkPlan): they are plain long
     * fields that mergeLimits cannot merge (the subquery plan does not sit on a
     * plan-child path of the main tree), so a "subquery LIMIT 2" query must not match a
     * baseline captured with a different subquery limit and replay the captured value.
     */
    public static boolean checkSubqueryPlan(LogicalPlan bindPlan, LogicalPlan userPlan,
            Map<Long, Expression> placeholderValues) {
        return checkPlan(bindPlan, userPlan, placeholderValues, true);
    }

    /** Node-by-node recursive structural check. */
    private static boolean checkPlan(Plan bind, Plan user, Map<Long, Expression> placeholderValues,
            boolean insideSubquery) {
        if (bind.getClass() != user.getClass()) {
            return false;
        }
        // LIMIT / OFFSET are plain long fields, not expressions, so the generic node check
        // cannot see them. A top-level LIMIT is adopted from the user query later
        // (mergeLimits); a LIMIT inside a subquery expression's plan is NOT reachable by
        // that merge, so it must take part in the match exactly - otherwise a query with
        // "subquery LIMIT 2" would replay a baseline captured with "subquery LIMIT 1"
        // and return a truncated result.
        if (insideSubquery && bind instanceof LogicalLimit && user instanceof LogicalLimit) {
            if (((LogicalLimit<?>) bind).getLimit() != ((LogicalLimit<?>) user).getLimit()
                    || ((LogicalLimit<?>) bind).getOffset() != ((LogicalLimit<?>) user).getOffset()) {
                return false;
            }
        }
        // LogicalTopN carries its ORDER BY ... LIMIT / OFFSET pair as a node of its own (it
        // does not extend LogicalLimit). Inside a nested query block the positional merge
        // gives up as soon as the frozen join order differs from the user's: the limited
        // derived table then pairs with the OTHER relation, the class-mismatch guard
        // leaves the captured TopN untouched and the replay returns the captured slice.
        // Comparing the pair exactly keeps such a query from matching at all.
        if (insideSubquery && bind instanceof LogicalTopN && user instanceof LogicalTopN) {
            if (((LogicalTopN<?>) bind).getLimit() != ((LogicalTopN<?>) user).getLimit()
                    || ((LogicalTopN<?>) bind).getOffset() != ((LogicalTopN<?>) user).getOffset()) {
                return false;
            }
        }
        // compare this node's expressions first (bind side is parameterized)
        if (!checkNodeExpressions(bind, user, placeholderValues)) {
            return false;
        }
        List<Plan> bindChildren = bind.children();
        List<Plan> userChildren = user.children();
        if (bindChildren.size() != userChildren.size()) {
            return false;
        }
        for (int i = 0; i < bindChildren.size(); i++) {
            // A derived table (LogicalSubQueryAlias) opens a nested query block: its own
            // LIMIT / OFFSET cannot rely on the positional merge (mergeLimits gives up on
            // a class mismatch - e.g. a frozen join order that differs from the user's -
            // and would silently keep the captured slice), so nested limits are part of
            // the exact match (see the LIMIT check above). A SET OPERAND is a query block
            // of its own too: (SELECT k FROM t1 LIMIT 1) UNION ALL SELECT k FROM t2
            // parses as UnionAll(Limit(1, t1), t2) with no SubQueryAlias around that
            // limit, and toDigest() masks the limit value - without this the frozen set
            // (wrapped as a derived table at replay) kept the captured LIMIT 1 while the
            // user asked LIMIT 2, silently losing a t1 row.
            boolean childInsideSubquery = insideSubquery || bind instanceof LogicalSubQueryAlias
                    || bind instanceof LogicalSetOperation;
            if (!checkPlan(bindChildren.get(i), userChildren.get(i), placeholderValues,
                    childInsideSubquery)) {
                return false;
            }
        }
        // CTE bodies are not children() - compare them explicitly
        if (bind instanceof LogicalCTE && user instanceof LogicalCTE) {
            LogicalCTE<?> bindCte = (LogicalCTE<?>) bind;
            LogicalCTE<?> userCte = (LogicalCTE<?>) user;
            // WITH RECURSIVE changes how a self-reference binds (work table vs base
            // table): the same syntax against a same-named real table means different
            // queries, so the recursion flag is part of the identity
            if (bindCte.isRecursive() != userCte.isRecursive()) {
                return false;
            }
            List<LogicalSubQueryAlias<Plan>> bindAliases = bindCte.getAliasQueries();
            List<LogicalSubQueryAlias<Plan>> userAliases = userCte.getAliasQueries();
            if (bindAliases.size() != userAliases.size()) {
                return false;
            }
            for (int i = 0; i < bindAliases.size(); i++) {
                // CTE bodies are nested query blocks: their LIMIT / OFFSET are compared
                // exactly (the positional LIMIT merge cannot reach them reliably)
                if (!checkPlan(bindAliases.get(i), userAliases.get(i), placeholderValues, true)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares the expressions of one node pair. Set-typed conjuncts (filter / having)
     * are ordered deterministically before pairing; list-typed expressions (projections,
     * group-by, ...) keep their parse order.
     */
    private static boolean checkNodeExpressions(Plan bind, Plan user,
            Map<Long, Expression> placeholderValues) {
        // Base-table relation: UnboundRelation.toDigest() omits partition names / sample /
        // hints and normalizes tablet / snapshot / scan-parameter values, and the
        // namespace is only covered by the L1/L2 digest pre-filter. Compare every
        // non-parameterizable scan identity field exactly here, so "t PARTITION(p1)" can
        // never match "PARTITION(p2)" (the frozen replay would keep reading p1). The
        // relation NAMES are deliberately not compared at this level: the stored bind
        // tree is the raw parse while the user side is namespace-qualified; name
        // equality is enforced by the digest pre-filter.
        if (bind instanceof UnboundRelation && user instanceof UnboundRelation) {
            return sameScanIdentity((UnboundRelation) bind, (UnboundRelation) user);
        }
        // Table-valued function: UnboundTVFRelation.toDigest() reduces every call to
        // "fn(?)" and the node has no expressions/children, so the generic comparison
        // cannot distinguish the properties. numbers('number'='100') must never match a
        // baseline captured with numbers('number'='10'): the frozen replay would run the
        // captured properties. Compare the function name and the property map exactly.
        if (bind instanceof UnboundTVFRelation && user instanceof UnboundTVFRelation) {
            UnboundTVFRelation bindTvf = (UnboundTVFRelation) bind;
            UnboundTVFRelation userTvf = (UnboundTVFRelation) user;
            return bindTvf.getFunctionName().equals(userTvf.getFunctionName())
                    && Objects.equals(bindTvf.getProperties(), userTvf.getProperties());
        }
        // (VALUES ...) relations: the digest masks every shape to the same VALUES text
        // and the generic expression comparison flattens the cell list, so two 2-cell
        // rows and four 1-cell rows expose the same literals in the same order - a
        // caller's count(*) over the frozen 2-row relation then returns 2 where the
        // user query would return 4. Compare the ROW structure: count, arity and the
        // cells positionally.
        if (bind instanceof InlineTable && user instanceof InlineTable) {
            List<List<NamedExpression>> bindRows = ((InlineTable) bind).getConstantExprsList();
            List<List<NamedExpression>> userRows = ((InlineTable) user).getConstantExprsList();
            if (bindRows.size() != userRows.size()) {
                return false;
            }
            for (int i = 0; i < bindRows.size(); i++) {
                List<NamedExpression> bindCells = bindRows.get(i);
                List<NamedExpression> userCells = userRows.get(i);
                if (bindCells.size() != userCells.size()) {
                    return false;
                }
                for (int j = 0; j < bindCells.size(); j++) {
                    if (!checkExpression(bindCells.get(j), userCells.get(j), placeholderValues)) {
                        return false;
                    }
                }
            }
            return true;
        }
        // SELECT hints: LogicalSelectHint.getExpressions() is empty and its toDigest()
        // drops the hint list, so two query blocks that differ ONLY in a hint payload have
        // the same digest and the same "no expressions, same child" result. A SET_VAR hint
        // however changes how the query is analyzed / planned (time_zone and sql_mode
        // change the RESULT, LEADING / ORDERED change the join order): accepting a
        // variant with a different setting would replay the creator's frozen expression /
        // plan under the caller's setting - neither side's semantics. Compare the complete
        // hint list of every block (nested blocks carry their own LogicalSelectHint and
        // are reached through the child / subquery recursion).
        if (bind instanceof LogicalSelectHint && user instanceof LogicalSelectHint) {
            return sameSelectHints(((LogicalSelectHint<?>) bind).getHints(),
                    ((LogicalSelectHint<?>) user).getHints());
        }
        // Positional subquery / CTE column aliases: LogicalSubQueryAlias does not expose
        // them through getExpressions() (toDigest merely computes the joined alias list
        // without appending it), so s(x, y) and s(y, x) over the same derived table would
        // otherwise match - replaying the captured column mapping and returning the wrong
        // column. Compare the alias lists explicitly.
        if (bind instanceof LogicalSubQueryAlias && user instanceof LogicalSubQueryAlias) {
            Optional<List<String>> bindAliases =
                    ((LogicalSubQueryAlias<?>) bind).getColumnAliases();
            Optional<List<String>> userAliases =
                    ((LogicalSubQueryAlias<?>) user).getColumnAliases();
            if (!Objects.equals(bindAliases, userAliases)) {
                return false;
            }
        }
        // MARK join: LogicalJoin uses one class for both plain and MARK joins; neither
        // the mark flag nor the mark conjuncts are reachable through the generic
        // expression comparison (toDigest omits markJoinSlotReference). CROSS MARK JOIN
        // must never match a plain CROSS JOIN: the frozen replay would keep one row per
        // left row instead of the cartesian multiplicity. Compare the mark state and the
        // mark conjuncts explicitly.
        if (bind instanceof LogicalJoin && user instanceof LogicalJoin) {
            LogicalJoin<?, ?> bindJoin = (LogicalJoin<?, ?>) bind;
            LogicalJoin<?, ?> userJoin = (LogicalJoin<?, ?>) user;
            if (bindJoin.isMarkJoin() != userJoin.isMarkJoin()) {
                return false;
            }
            // The MARK_SLOT name is the user-visible result header of a "SELECT *" over a
            // mark join: neither toDigest nor getExpressions exposes it, so without this
            // comparison MARK_SLOT m1 and MARK_SLOT m2 match and the frozen replay would
            // restore the captured header instead of the requested one. Identifiers are
            // matched case-insensitively (the same rule the analyzer applies).
            Optional<MarkJoinSlotReference> bindMarkSlot = bindJoin.getMarkJoinSlotReference();
            Optional<MarkJoinSlotReference> userMarkSlot = userJoin.getMarkJoinSlotReference();
            if (bindMarkSlot.isPresent() != userMarkSlot.isPresent()) {
                return false;
            }
            if (bindMarkSlot.isPresent() && !bindMarkSlot.get().getName()
                    .equalsIgnoreCase(userMarkSlot.get().getName())) {
                return false;
            }
            List<Expression> bindMark = bindJoin.getMarkJoinConjuncts();
            List<Expression> userMark = userJoin.getMarkJoinConjuncts();
            if (bindMark.size() != userMark.size()) {
                return false;
            }
            for (int i = 0; i < bindMark.size(); i++) {
                if (!checkExpression(bindMark.get(i), userMark.get(i), placeholderValues)) {
                    return false;
                }
            }
        }
        // USING / ASOF join: getExpressions() returns the USING slots only, so the
        // MATCH_CONDITION (a temporal boundary on ASOF joins) would never take part in the
        // match - a user variant with a different boundary would replay the captured one
        // and select another right-side row. Compare the optional condition explicitly.
        if (bind instanceof LogicalUsingJoin && user instanceof LogicalUsingJoin) {
            Optional<Expression> bindMatch = ((LogicalUsingJoin<?, ?>) bind).getMatchCondition();
            Optional<Expression> userMatch = ((LogicalUsingJoin<?, ?>) user).getMatchCondition();
            if (bindMatch.isPresent() != userMatch.isPresent()) {
                return false;
            }
            if (bindMatch.isPresent()
                    && !checkExpression(bindMatch.get(), userMatch.get(), placeholderValues)) {
                return false;
            }
        }
        if (bind instanceof LogicalFilter || bind instanceof LogicalHaving) {
            Set<Expression> bindConjuncts;
            Set<Expression> userConjuncts;
            if (bind instanceof LogicalFilter) {
                bindConjuncts = ((LogicalFilter<?>) bind).getConjuncts();
                userConjuncts = ((LogicalFilter<?>) user).getConjuncts();
            } else {
                bindConjuncts = ((LogicalHaving<?>) bind).getConjuncts();
                userConjuncts = ((LogicalHaving<?>) user).getConjuncts();
            }
            if (bindConjuncts.size() != userConjuncts.size()) {
                return false;
            }
            // The conjunct SETS are unordered, and the two sides render differently: the
            // bind side carries "_spm_const_var(id)" placeholders while the user side
            // carries the concrete literal text, so a lexical (toSql) sort can pair the
            // WRONG expressions whenever the literal ordering reverses the structural
            // order - rejecting a valid baseline. Match the two conjunct multisets
            // structurally instead: every tentative pairing extracts placeholder values
            // transactionally and rolls them back on failure, so a pairing that only
            // looked compatible cannot poison the rest of the match.
            List<Expression> remaining = new ArrayList<>(userConjuncts);
            for (Expression bindConjunct : bindConjuncts) {
                int matched = -1;
                // the pairing is an UNORDERED search: when a greedy pairing breaks a LATER
                // use of the same placeholder id, the retry driver re-runs the check with
                // a different starting point (see MatchAttempt)
                int offset = MatchAttempt.offset(remaining.size());
                for (int i = 0; i < remaining.size(); i++) {
                    int candidate = (offset + i) % remaining.size();
                    Map<Long, Expression> snapshot = new HashMap<>(placeholderValues);
                    if (checkExpression(bindConjunct, remaining.get(candidate), placeholderValues)) {
                        matched = candidate;
                        break;
                    }
                    placeholderValues.clear();
                    placeholderValues.putAll(snapshot);
                }
                if (matched < 0) {
                    return false;
                }
                remaining.remove(matched);
            }
            return true;
        }

        List<? extends Expression> bindExprs = bind.getExpressions();
        List<? extends Expression> userExprs = user.getExpressions();
        if (bindExprs.size() != userExprs.size()) {
            return false;
        }
        for (int i = 0; i < bindExprs.size(); i++) {
            if (!checkExpression(bindExprs.get(i), userExprs.get(i), placeholderValues)) {
                return false;
            }
        }
        // LATERAL VIEW / UNNEST: generator arguments are intentionally kept concrete
        // (see TreeTransformer.visitLogicalGenerate), so a baseline only matches a user
        // query carrying EXACTLY the same generator arguments and output aliases.
        // LogicalGenerate does not expose them through getExpressions(), compare them
        // explicitly - otherwise a query with a different array/generator argument would
        // match and be replayed with the baseline's old arguments.
        if (bind instanceof LogicalGenerate && user instanceof LogicalGenerate) {
            LogicalGenerate<?> bindGenerate = (LogicalGenerate<?>) bind;
            LogicalGenerate<?> userGenerate = (LogicalGenerate<?>) user;
            List<Function> bindGenerators = bindGenerate.getGenerators();
            List<Function> userGenerators = userGenerate.getGenerators();
            if (bindGenerators.size() != userGenerators.size()) {
                return false;
            }
            for (int i = 0; i < bindGenerators.size(); i++) {
                if (!checkExpression(bindGenerators.get(i), userGenerators.get(i), placeholderValues)) {
                    return false;
                }
            }
            // UNNEST under LEFT JOIN differs from the inner form only by its mode
            // flags: both are the same class with the same rendered text, and a
            // frozen OUTER unnest keeps each unmatched row with NULL where the inner
            // caller drops it. Compare the flags explicitly.
            for (int i = 0; i < bindGenerators.size(); i++) {
                if (bindGenerators.get(i) instanceof Unnest
                        && userGenerators.get(i) instanceof Unnest) {
                    Unnest bindUnnest = (Unnest) bindGenerators.get(i);
                    Unnest userUnnest = (Unnest) userGenerators.get(i);
                    if (bindUnnest.isOuter() != userUnnest.isOuter()
                            || bindUnnest.needOrdinality() != userUnnest.needOrdinality()) {
                        return false;
                    }
                }
            }
            List<? extends Expression> bindOutput = bindGenerate.getGeneratorOutput();
            List<? extends Expression> userOutput = userGenerate.getGeneratorOutput();
            if (bindOutput.size() != userOutput.size()) {
                return false;
            }
            for (int i = 0; i < bindOutput.size(); i++) {
                if (!checkExpression(bindOutput.get(i), userOutput.get(i), placeholderValues)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares the hint lists of one query block: same size, same position, same hint
     * class and the same payload. The lists are compared ELEMENT-WISE (not as a set):
     * LEADING / ORDERED spell out a join order, so the order inside one block is part of
     * the hint. The payload comparison is structural rather than toString-based, so the
     * same hint written with its keys in a different order still matches:
     *
     * - SET_VAR: variable name -> value (an empty value is the bare
     *   SET_VAR(name) form). Variable names are matched case-insensitively (Doris
     *   variable names are).
     * - USE_MV: the referenced table groups and the on / off flag.
     * - LEADING: the leading parameter list (ORDER-SENSITIVE: it fixes the join order)
     *   and the distribution map.
     * - any other subclass: no payload known - the rendered form must still be equal so
     *   a future payload-carrying hint class is rejected instead of silently accepted.
     */
    private static boolean sameSelectHints(List<SelectHint> bindHints, List<SelectHint> userHints) {
        if (bindHints.size() != userHints.size()) {
            return false;
        }
        for (int i = 0; i < bindHints.size(); i++) {
            SelectHint bindHint = bindHints.get(i);
            SelectHint userHint = userHints.get(i);
            if (!bindHint.getClass().equals(userHint.getClass())
                    || !bindHint.getHintName().equalsIgnoreCase(userHint.getHintName())) {
                return false;
            }
            if (bindHint instanceof SelectHintSetVar) {
                Map<String, Optional<String>> bindParams =
                        ((SelectHintSetVar) bindHint).getParameters();
                Map<String, Optional<String>> userParams =
                        ((SelectHintSetVar) userHint).getParameters();
                if (bindParams.size() != userParams.size()) {
                    return false;
                }
                for (Map.Entry<String, Optional<String>> entry : bindParams.entrySet()) {
                    if (!Objects.equals(entry.getValue(),
                            getIgnoreCase(userParams, entry.getKey()))) {
                        return false;
                    }
                }
            } else if (bindHint instanceof SelectHintUseMv) {
                SelectHintUseMv bindMv = (SelectHintUseMv) bindHint;
                SelectHintUseMv userMv = (SelectHintUseMv) userHint;
                if (bindMv.isUseMv() != userMv.isUseMv()
                        || !bindMv.getTables().equals(userMv.getTables())) {
                    return false;
                }
            } else if (bindHint instanceof SelectHintLeading) {
                SelectHintLeading bindLeading = (SelectHintLeading) bindHint;
                SelectHintLeading userLeading = (SelectHintLeading) userHint;
                if (!bindLeading.getParameters().equals(userLeading.getParameters())
                        || !Objects.equals(bindLeading.getStrToHint(),
                                userLeading.getStrToHint())) {
                    return false;
                }
            } else if (!bindHint.toString().equals(userHint.toString())) {
                return false;
            }
        }
        return true;
    }

    /** Case-insensitive lookup in a hint parameter map (Doris variable names are). */
    private static Optional<String> getIgnoreCase(Map<String, Optional<String>> map, String key) {
        for (Map.Entry<String, Optional<String>> entry : map.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Structural expression comparison entry used by the AST matcher for the out-of-band
     * star payload items (EXCEPT / REPLACE): they are not reachable through children(),
     * so the matcher compares each item through here - including the explicit-alias name
     * and derived-alias parity checks of checkExpression.
     *
     * @param bindExpr          the bind-side payload item
     * @param userExpr          the user-side payload item
     * @param placeholderValues the placeholder value extraction result
     * @return whether the two items match
     */
    public static boolean checkPayloadExpression(Expression bindExpr, Expression userExpr,
            Map<Long, Expression> placeholderValues) {
        return checkExpression(bindExpr, userExpr, placeholderValues);
    }

    /** Single expression pair check (bind side parameterized, user side raw). */
    private static boolean checkExpression(Expression bindExpr, Expression userExpr,
            Map<Long, Expression> placeholderValues) {
        // Explicit output aliases (SELECT ... AS name) are part of the result contract:
        // the rewritten plan reuses the frozen text, so a baseline matched with a
        // different alias would report the CAPTURED column header. Derived names
        // (nameFromChild - the parser's fallback from the expression text, which may
        // carry the captured literal) stay out of the comparison. Both the analyzed
        // Alias and the parse-time UnboundAlias are covered: a raw parsed tree (the
        // bind side is re-parsed from the frozen text) only ever carries UnboundAlias,
        // whose name is only reachable through getAlias().
        String bindAliasName = explicitAliasName(bindExpr);
        String userAliasName = explicitAliasName(userExpr);
        if (bindAliasName != null && userAliasName != null
                && !bindAliasName.equals(userAliasName)) {
            return false;
        }
        // Alias KIND parity: an explicit alias is part of the result contract, a
        // nameFromChild alias is the parser's fallback from the expression text (and may
        // carry the captured literal). One side explicit + the other derived is not the
        // same contract - replay would expose the captured header. Now that the
        // transforms preserve nameFromChild, this only fires on genuinely different
        // spellings, but the parity requirement keeps the comparison symmetric.
        if (isDerivedAlias(bindExpr) != isDerivedAlias(userExpr)) {
            return false;
        }
        // A DERIVED label is deliberately NOT compared here: the frozen planSql pins the
        // CAPTURED label (the decompiler's sink emits it as an explicit alias), but the
        // caller running a value variant (SELECT k + 2 against a captured SELECT k + 1)
        // must keep the baseline - and must still see its OWN header. The replay hands
        // the caller's labels back instead (alignRootOutputLabels), so comparing the
        // derived text here would only reject valid matches. The derived alias may live
        // ANYWHERE (a nested subquery's projection carries one too, e.g. the scalar
        // subquery of TPCH q17), which is why the label contract is enforced on the
        // root's output items at replay instead of on every compared expression.
        return new SPMAstCheckVisitor().checkExpression(bindExpr, userExpr, placeholderValues);
    }

    /**
     * (replay half): the frozen planSql pins the CAPTURED output labels - a
     * derived label (the parser's nameFromChild: the expression text with the captured
     * literal, e.g. k + 1) is emitted by the decompiler's sink as an EXPLICIT
     * alias - while the matcher deliberately accepts a value variant such as
     * SELECT k + 2 for a captured SELECT k + 1. NereidsPlanner reports
     * the REPLAYED root's column names as the protocol header, so the replay must hand
     * the CALLER's own labels back instead of exposing the captured text.
     *
     * Renames the rewritten tree's caller-visible output items position by position. Both
     * trees are walked down their common wrapper chain first: a raw parse wraps every
     * query in an UnboundResultSink (which carries NO output items of its own and
     * rejects withOutputExprs) and a top-level LIMIT / TOP N sits above the projection as
     * well, so the list that reaches the caller lives on the outermost
     * LogicalProject, LogicalAggregate / LogicalWindow (which
     * produce the select list directly, without a separate projection above them) or on a
     * resolved sink. The shapes are already proven equal by the structural check, and an
     * arity mismatch (SELECT *, SELECT * EXCEPT(...) - the user's tree keeps the star as
     * ONE item) leaves the tree untouched, because those labels are real column names,
     * not captured expression text.
     *
     * A node that PRODUCES the caller's rows must stop the walk:
     * descending past an aggregate without a projection above it (or past a window) used
     * to land on an INNER derived-table project and rename ITS items position by
     * position. When the frozen plan had reordered that projection (the optimizer
     * projects the frozen (C_ACCTBAL, substring(...) AS cc) while the caller's
     * text spells (substr(...) AS cc, c_acctbal)), the positional rename swapped
     * the derived column NAMES and every outer reference bound to the swapped side - the
     * replay of TPCH q22 (GROUP BY cc, sum(c_acctbal) over such a derived plan) grouped by
     * c_acctbal and summed the country code instead. The rename must only ever
     * touch the node whose output items ARE the caller-visible list.
     *
     * @param rewritten the replayed tree (frozen text or parameterized fallback)
     * @param userPlan  the caller's own (namespace-qualified) tree
     * @return the rewritten tree exposing the caller's output labels
     */
    public static LogicalPlan alignRootOutputLabels(LogicalPlan rewritten, LogicalPlan userPlan) {
        // walk down while BOTH trees carry the same wrapper class, remembering the
        // rewritten ancestors, so the renamed node can replace its old self below them
        java.util.ArrayDeque<Plan> rewrittenAncestors = new java.util.ArrayDeque<>();
        Plan rewrittenNode = rewritten;
        Plan userNode = userPlan;
        while (true) {
            while (rewrittenNode.getClass() == userNode.getClass()
                    && !carriesOutputList(rewrittenNode)
                    && rewrittenNode.children().size() == 1
                    && userNode.children().size() == 1) {
                rewrittenAncestors.push(rewrittenNode);
                rewrittenNode = rewrittenNode.child(0);
                userNode = userNode.child(0);
            }
            // The caller's SET_VAR hint wrapper is CALLER-SIDE ONLY: the frozen replay
            // carries no hint (and the parameterized fallback strips it), and
            // carrySetVarHints re-attaches the settings AFTER this alignment. Walking past
            // it lets the alignment reach the caller's project instead of stopping at the
            // class mismatch and returning the captured header (a
            // SET_VAR(time_zone='+08:00') `v + 1` baseline matching a `v + 2` caller
            // reported the frozen `v + 1`).
            if (userNode instanceof LogicalSelectHint && userNode.children().size() == 1
                    && !(rewrittenNode instanceof LogicalSelectHint)) {
                userNode = userNode.child(0);
                continue;
            }
            break;
        }
        // A standalone one-row replay (CREATE SESSION BASELINE PLAN 'SELECT 1' matching
        // a later SELECT 2): neither tree carries an output-carrying unary node, so the
        // walk above cannot rename anything - the value is substituted, but the derived
        // header would keep the captured literal / placeholder text instead of the
        // caller's own label. Rebuild the relation's own item list with the caller's
        // labels; a shape that cannot be addressed skips the rewrite.
        if (userNode instanceof OneRowRelation || rewrittenNode instanceof OneRowRelation) {
            return alignOneRowOutput(rewritten, rewrittenNode, userNode, rewrittenAncestors);
        }
        // A baseline over a SET OPERATION. The caller's root IS the set node
        // (SELECT k + 2 FROM t UNION ALL ...), so its FIRST branch's labels are the
        // caller-visible header; the replayed tree may BE the same set (the fallback
        // replay) or wrap it in wrappers whose shape the frozen text pinned - a
        // projection carrying the CAPTURED labels, under a subquery alias, under
        // EXPLAIN... Descend the replayed tree through single-child wrappers to the node
        // the alignment can target:
        // - a set operation of the caller's class: align every BRANCH positionally;
        // - an output-carrying wrapper (the frozen text's label-pinning projection):
        //   align it against the caller's first branch - but only when a set operation
        //   of the caller's class really sits below it, so a wrapper over an unrelated
        //   subtree never has its list rewritten.
        // Anything else: the labels cannot be addressed -> skip the rewrite.
        if (userNode instanceof LogicalSetOperation) {
            java.util.ArrayDeque<Plan> extraAncestors = new java.util.ArrayDeque<>();
            Plan probe = rewrittenNode;
            while (!(probe instanceof LogicalSetOperation) && !carriesOutputList(probe)
                    && probe.children().size() == 1) {
                extraAncestors.push(probe);
                probe = probe.child(0);
            }
            if (probe instanceof LogicalSetOperation
                    && probe.getClass() == userNode.getClass()
                    && probe.children().size() == userNode.children().size()) {
                List<Plan> alignedChildren = new ArrayList<>();
                boolean changedChildren = false;
                for (int i = 0; i < probe.children().size(); i++) {
                    Plan child = probe.child(i);
                    Plan alignedChild = alignRootOutputLabels((LogicalPlan) child,
                            (LogicalPlan) userNode.child(i));
                    changedChildren |= alignedChild != child;
                    alignedChildren.add(alignedChild);
                }
                if (!changedChildren) {
                    return rewritten;
                }
                return rebuildThroughAncestors(probe.withChildren(alignedChildren),
                        extraAncestors, rewrittenAncestors);
            }
            if (!(probe instanceof LogicalSetOperation) && carriesOutputList(probe)
                    && setOperationOfClassBelow(probe, userNode.getClass())) {
                Plan firstBranch = userNode.child(0);
                if (firstBranch instanceof LogicalPlan && carriesOutputList(firstBranch)) {
                    LogicalPlan alignedWrapper = alignRootOutputLabels(
                            (LogicalPlan) probe, (LogicalPlan) firstBranch);
                    if (alignedWrapper == probe) {
                        return rewritten;
                    }
                    return rebuildThroughAncestors(alignedWrapper, extraAncestors,
                            rewrittenAncestors);
                }
                // A constant first branch (SELECT 1 AS c) has NO rebuildable output list -
                // no positional rename can be applied. Realigning is unnecessary when
                // every label already equals the wrapper's, and must not be faked
                // otherwise (the caller would keep the captured header).
                // The branch's top node may also be a non-output WRAPPER of the raw
                // parse - "SELECT 'store' channel, ... FROM (...) x WHERE ..." parses as
                // Filter(Project(...)), so the labels live one/several wrappers down
                // (tpcds q49's union branches carry a WHERE). Derive them from the
                // nearest output-carrying node below; the check stays conservative
                // because nothing is renamed on this path.
                List<String> callerLabels = firstBranch instanceof LogicalPlan
                        ? derivableBranchLabels(firstBranch) : null;
                // A left-deep union parse (A UNION ALL B UNION ALL C) nests the set
                // operations on the LEFT: the caller's "first branch" may itself be a set
                // operation (whose output list is not materialized), so the
                // caller-visible labels live on its leftmost LEAF branch.
                Plan callerBranch = firstBranch;
                while (callerLabels == null && callerBranch instanceof LogicalSetOperation) {
                    callerBranch = callerBranch.child(0);
                    callerLabels = callerBranch instanceof LogicalPlan
                            ? derivableBranchLabels(callerBranch) : null;
                }
                if (callerLabels == null && callerBranch != null) {
                    // The optimized caller's branch top may be a join carrying no
                    // rebuildable item list (the branch projection merged into its join);
                    // the caller-visible labels are then exactly the branch's own OUTPUT
                    // SLOT NAMES, taken positionally.
                    callerLabels = new ArrayList<>();
                    for (Slot slot : callerBranch.getOutput()) {
                        callerLabels.add(slot.getName());
                    }
                }
                if (callerLabels == null || callerLabels.isEmpty()
                        || !labelsEqual(callerLabels, outputItemsOf(probe))) {
                    throw new UnalignableOutputLabelsException(
                            "the caller's set-operation header labels cannot be determined"
                                    + " (caller branch=" + (callerBranch == null ? null
                                            : callerBranch.getClass().getSimpleName())
                                    + ", caller labels=" + callerLabels + ", replay items="
                                    + outputItemsOf(probe) + ")");
                }
                return rewritten;
            }
            throw new UnalignableOutputLabelsException(
                    "the replay's set-operation output cannot be aligned with the caller's"
                            + " labels");
        }
        if (rewrittenNode instanceof LogicalSetOperation) {
            throw new UnalignableOutputLabelsException(
                    "the replay's set-operation output cannot be aligned with the caller's"
                            + " labels");
        }
        // A grouped aggregate replay: the frozen decompilation wraps the caller's
        // aggregate in an OUTER SELECT c_N AS <captured header> to preserve the captured
        // result label, while the caller's root IS the aggregate. The projection's list
        // carries the caller-visible header and aligns POSITIONALLY with the aggregate's
        // output list, so the class mismatch alone must not skip the realignment - the
        // replay computes the caller's aggregate (sum(v + 2)) but used to report the
        // captured label (sum(v + 1)).
        boolean groupedAggregateReplay = rewrittenNode instanceof LogicalProject
                && userNode instanceof LogicalAggregate;
        if ((rewrittenNode.getClass() != userNode.getClass()
                || !carriesOutputList(rewrittenNode)) && !groupedAggregateReplay) {
            return rewritten;
        }
        List<NamedExpression> rewrittenItems = outputItemsOf(rewrittenNode);
        List<NamedExpression> userItems = outputItemsOf(userNode);
        // The caller's list may still hold a STAR while the rewritten (frozen) list is the
        // EXPANDED projection: the capture-time plan was analyzed, so its sink pinned one
        // item per derived column, while a raw parse keeps `*` as ONE item. Expand the
        // caller's stars through its OWN child relation first, so the labels of the
        // capture-time text can still be replaced (`SELECT *` over
        // `(SELECT k + 1 FROM t) s` kept the captured `k + 1` header on the replay of the
        // `k + 2` variant, although the original query exposes `k + 2`).
        List<String> userLabels = expandedOutputLabels(userItems, userNode);
        // A caller star whose expansion is only PARTLY derivable (a star
        // over a join whose leading side is a derived relation) carries the open-tail
        // marker; the derived prefix still realigns the frozen labels positionally, and
        // the positions behind the marker keep the frozen names (those are real column
        // names that need no realignment).
        if (userLabels == null) {
            // A star whose unknown-width tail is FOLLOWED by further caller items: the
            // caller's positions cannot be derived from its own list, but the frozen list
            // fixes the arity, so the trailing items are positioned from the END and the
            // star keeps its derivable prefix + the frozen names behind it (see
            // alignOpenStarWithTrailingItems). Returning the tree untouched exposed the
            // frozen sink's captured header for the trailing item (SELECT *, c + 2 FROM u
            // reported c + 1).
            return alignOpenStarWithTrailingItems(rewrittenNode, userNode, userItems,
                    rewrittenItems, rewritten, rewrittenAncestors);
        }
        boolean openTail = !userLabels.isEmpty()
                && userLabels.get(userLabels.size() - 1) == OPEN_TAIL_LABEL;
        int derivableCount = openTail ? userLabels.size() - 1 : userLabels.size();
        if (derivableCount > rewrittenItems.size()
                || (!openTail && derivableCount != rewrittenItems.size())) {
            return rewritten;
        }
        List<NamedExpression> aligned = new ArrayList<>(rewrittenItems.size());
        boolean changed = false;
        for (int i = 0; i < rewrittenItems.size(); i++) {
            NamedExpression rewrittenItem = rewrittenItems.get(i);
            NamedExpression userItem = userItems.size() == derivableCount && i < userItems.size()
                    ? userItems.get(i) : null;
            String userLabel = i < derivableCount ? userLabels.get(i) : null;
            String rewrittenLabel = outputLabelOf(rewrittenItem);
            if (userLabel == null || rewrittenLabel == null
                    || userLabel.equals(rewrittenLabel)) {
                // no label on one of the two sides (or both equal): the position carries no
                // captured header text to realign. A STAR projection is the important case
                // when its expansion is not derivable (a star over a join / base table,
                // `* EXCEPT` / `* REPLACE`): the frozen plan may project `*` while the
                // caller names a bare column, and its expansion to real columns happens at
                // binding - wrapping it in a renamed item builds an Alias over an unbound
                // star and the analyzer rejects the tree ("Invalid call to
                // ...getDataType() on unbound object").
                aligned.add(rewrittenItem);
                continue;
            }
            // the caller's own header text (explicit, derived or a bare column name)
            // replaces the label the frozen sink pinned; the SUBSTITUTED expression
            // itself stays untouched
            aligned.add(renameOutputItem(rewrittenItem, userLabel,
                    userItem != null && isDerivedAlias(userItem)));
            changed = true;
        }
        if (!changed) {
            return rewritten;
        }
        Plan rebuilt = rebuildWithOutputItems(rewrittenNode, aligned);
        // rebuild the skipped wrapper chain (innermost ancestor first)
        for (Plan ancestor : rewrittenAncestors) {
            rebuilt = ancestor.withChildren(java.util.List.of(rebuilt));
        }
        return (LogicalPlan) rebuilt;
    }

    /** Rebuilds one output-carrying node with a new caller-visible item list (see
     * carriesOutputList). */
    private static Plan rebuildWithOutputItems(Plan node, List<NamedExpression> items) {
        if (node instanceof LogicalProject) {
            return ((LogicalProject<?>) node).withProjects(items);
        }
        if (node instanceof LogicalAggregate) {
            // the aggregate IS the query block's output: an aggregate-rooted query
            // (SELECT cc, count(*) ... GROUP BY cc) has no projection above it, so the
            // aligned list must go through withAggOutput
            return ((LogicalAggregate<?>) node).withAggOutput(items);
        }
        if (node instanceof LogicalWindow) {
            return ((LogicalWindow<?>) node).withExpressionsAndChild(items, node.child(0));
        }
        return ((LogicalSink<?>) node).withOutputExprs(items);
    }

    /**
     * Whether this node directly carries the caller-visible output list: a projection
     * (always), an AGGREGATE or WINDOW (their output expressions ARE the caller-visible
     * select list - the walk must stop there instead of descending into an inner query
     * block, see alignRootOutputLabels), or a SINK that resolves its own output
     * items - the unbound result sink of a raw parse holds an EMPTY list and only wraps
     * its child.
     */
    private static boolean carriesOutputList(Plan node) {
        if (node instanceof LogicalProject
                || node instanceof LogicalAggregate
                || node instanceof LogicalWindow) {
            return true;
        }
        return node instanceof LogicalSink && !((LogicalSink<?>) node).getOutputExprs().isEmpty();
    }

    /**
     * Whether a set operation of the given class sits below node through a single-child
     * chain (a subquery alias, an unbound result sink, the SORT + AGGREGATE the frozen
     * text wraps a UNION DISTINCT in...). The search walks through ANY single-child node:
     * the caller's root is the set, so the frozen wrapper must eventually read from one,
     * and the alignment target stays the wrapper whose output list is the caller-visible
     * header (see alignRootOutputLabels).
     */
    private static boolean setOperationOfClassBelow(Plan node, Class<?> setClass) {
        if (node.children().size() != 1) {
            return false;
        }
        Plan probe = node.child(0);
        while (probe != null && !(probe instanceof LogicalSetOperation)
                && probe.children().size() == 1) {
            probe = probe.child(0);
        }
        return probe != null && probe.getClass() == setClass;
    }

    /**
     * Wraps the rebuilt node back under its recorded wrappers: the INNER deque holds the
     * wrappers between the rewritten root and the rebuilt node (innermost first, the
     * push order of the descent), the OUTER one the wrappers above the rewritten root
     * (see alignRootOutputLabels).
     */
    private static LogicalPlan rebuildThroughAncestors(Plan node,
            java.util.ArrayDeque<Plan> innerAncestors,
            java.util.ArrayDeque<Plan> outerAncestors) {
        Plan rebuilt = node;
        for (Plan ancestor : innerAncestors) {
            rebuilt = ancestor.withChildren(java.util.List.of(rebuilt));
        }
        for (Plan ancestor : outerAncestors) {
            rebuilt = ancestor.withChildren(java.util.List.of(rebuilt));
        }
        return (LogicalPlan) rebuilt;
    }

    /**
     * Aligns a standalone one-row replay (see alignRootOutputLabels): the labels live on
     * the OneRowRelation itself, so its item list is rebuilt with the caller's own
     * labels. A shape that cannot be addressed skips the rewrite - the caller must not
     * receive the captured header.
     */
    private static LogicalPlan alignOneRowOutput(LogicalPlan rewritten, Plan rewrittenNode,
            Plan userNode, java.util.ArrayDeque<Plan> rewrittenAncestors) {
        if (!(rewrittenNode instanceof OneRowRelation) || !(userNode instanceof OneRowRelation)) {
            throw new UnalignableOutputLabelsException(
                    "a one-row replay cannot be aligned with the caller's different shape");
        }
        List<NamedExpression> rewrittenItems = ((OneRowRelation) rewrittenNode).getProjects();
        List<NamedExpression> userItems = ((OneRowRelation) userNode).getProjects();
        if (rewrittenItems.size() != userItems.size()) {
            throw new UnalignableOutputLabelsException(
                    "the one-row replay's output arity cannot be aligned with the caller's");
        }
        List<NamedExpression> aligned = new ArrayList<>(rewrittenItems.size());
        boolean changed = false;
        for (int i = 0; i < rewrittenItems.size(); i++) {
            NamedExpression rewrittenItem = rewrittenItems.get(i);
            NamedExpression userItem = userItems.get(i);
            String userLabel = outputLabelOf(userItem);
            String rewrittenLabel = outputLabelOf(rewrittenItem);
            if (userLabel == null || rewrittenLabel == null || userLabel.equals(rewrittenLabel)) {
                aligned.add(rewrittenItem);
                continue;
            }
            aligned.add(renameOutputItem(rewrittenItem, userLabel, isDerivedAlias(userItem)));
            changed = true;
        }
        if (!changed) {
            return rewritten;
        }
        return rebuildThroughAncestors(
                withOneRowProjects((OneRowRelation) rewrittenNode, aligned),
                new java.util.ArrayDeque<>(), rewrittenAncestors);
    }

    /** Rebuilds one OneRowRelation with a new item list (see alignOneRowOutput). */
    private static Plan withOneRowProjects(OneRowRelation relation, List<NamedExpression> items) {
        if (relation instanceof UnboundOneRowRelation) {
            // the parsed relation has no with-projects rebuild (its withRelationId throws):
            // construct the copy directly
            return new UnboundOneRowRelation(
                    ((UnboundOneRowRelation) relation).getRelationId(), items);
        }
        if (relation instanceof LogicalOneRowRelation) {
            return ((LogicalOneRowRelation) relation).withProjects(items);
        }
        throw new UnalignableOutputLabelsException(
                "the one-row replay's relation shape cannot be rebuilt");
    }

    /**
     * The caller-visible labels of a set branch's top node, whether or not the node
     * carries a rebuildable output list (a constant branch SELECT 1 AS c has none - its
     * label lives on the one-row relation). null when the labels cannot be derived (see
     * alignRootOutputLabels).
     */
    private static List<String> branchLabels(Plan branch) {
        List<NamedExpression> items;
        if (carriesOutputList(branch)) {
            items = outputItemsOf(branch);
        } else if (branch instanceof OneRowRelation) {
            // both the bound and the parsed (unbound) constant relation expose their
            // select list here
            items = ((OneRowRelation) branch).getProjects();
        } else {
            return null;
        }
        List<String> labels = new ArrayList<>(items.size());
        for (NamedExpression item : items) {
            labels.add(outputLabelOf(item));
        }
        return labels;
    }

    /**
     * branchLabels through the branch's non-output wrappers: "SELECT 'store' channel,
     * ... FROM (...) x WHERE ..." parses as Filter(Project(...)), so the labels of a
     * union branch live below its WHERE (see alignRootOutputLabels' constant-branch
     * case, tpcds q49). Only the EQUALITY check uses this widening - nothing is
     * renamed through a wrapper that cannot rebuild its list.
     */
    private static List<String> derivableBranchLabels(Plan branch) {
        Plan node = branch;
        while (node != null && !carriesOutputList(node) && node.children().size() == 1) {
            node = node.child(0);
        }
        return node == null ? null : branchLabels(node);
    }

    /** Position-by-position label equality (null on either side = not equal). */
    private static boolean labelsEqual(List<String> callerLabels, List<NamedExpression> items) {
        if (callerLabels.size() != items.size()) {
            return false;
        }
        for (int i = 0; i < callerLabels.size(); i++) {
            String itemLabel = outputLabelOf(items.get(i));
            if (!labelMatches(callerLabels.get(i), itemLabel)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a caller-visible label names the same column as an item label. The
     * caller's project may hold an UNBOUND slot whose name still carries its qualifier
     * ("web.item") - the column the caller sees is the bare name, exactly what the
     * frozen sink pins ("item"), so a qualified name matches its own tail. Both sides
     * keep their exact-match path first: two DIFFERENT qualified names that merely share
     * a tail stay unequal.
     */
    private static boolean labelMatches(String callerLabel, String itemLabel) {
        if (callerLabel == null || itemLabel == null) {
            return false;
        }
        if (callerLabel.equals(itemLabel)) {
            return true;
        }
        return unqualifiedTail(callerLabel).equals(itemLabel)
                || unqualifiedTail(itemLabel).equals(callerLabel);
    }

    private static String unqualifiedTail(String label) {
        int dot = label.lastIndexOf('.');
        return dot >= 0 ? label.substring(dot + 1) : label;
    }

    /**
     * One label per caller-visible output COLUMN: a plain item contributes its own label,
     * a root STAR is expanded through the caller's own child relation (see
     * starExpansionForStar / starExpansion).
     *
     * @return the labels (null entries = the position carries no derivable label), the
     *         list ENDING with OPEN_TAIL_LABEL when the trailing positions'
     *         count / labels are not derivable, or null when a star's open tail cannot
     *         be placed (another item follows it - the caller then positions those items
     *         against the frozen list, see alignOpenStarWithTrailingItems)
     */
    private static List<String> expandedOutputLabels(List<NamedExpression> items, Plan node) {
        List<String> labels = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            NamedExpression item = items.get(i);
            if (item instanceof UnboundStar && node.children().size() == 1) {
                UnboundStar star = (UnboundStar) item;
                StarLabels expanded = starExpansionForStar(node.child(0), star);
                if (hasStarPayload(star)) {
                    // The EXCEPT payload REMOVES columns from the visible list: skipping
                    // the expansion left no caller label for the derived projection's
                    // remaining column, so the frozen sink kept reporting the captured
                    // `v + 1` while returning the substituted `v + 2` (see
                    // exceptAdjustedLabels). REPLACE keeps every label (it swaps VALUES).
                    if (expanded.openTail) {
                        // the underlying labels are not derivable (a base table): its real
                        // column names need no realignment and the payload cannot be
                        // projected onto them here
                        labels.add(outputLabelOf(item));
                        continue;
                    }
                    labels.addAll(exceptAdjustedLabels(star, expanded.labels));
                    continue;
                }
                if (expanded.openTail) {
                    if (i != items.size() - 1) {
                        // the open tail's length is unknown: the items after it cannot be
                        // positioned from the caller's own list
                        return null;
                    }
                    labels.addAll(expanded.labels);
                    labels.add(OPEN_TAIL_LABEL);
                    continue;
                }
                labels.addAll(expanded.labels);
                continue;
            }
            labels.add(outputLabelOf(item));
        }
        return labels;
    }

    /**
     * One payload star's caller-visible labels: the underlying relation's labels with
     * the EXCEPT columns removed and the REPLACE aliases applied. An EXCEPT name that
     * matches no derivable label declines: silently keeping the frozen list would
     * report the captured header of a column the caller removed.
     */
    private static List<String> exceptAdjustedLabels(UnboundStar star, List<String> labels) {
        return applyReplacedAliases(star, applyExceptedLabels(star, labels));
    }

    /** The EXCEPT removal half of one payload star's label derivation. */
    private static List<String> applyExceptedLabels(UnboundStar star, List<String> labels) {
        List<String> excepted = new ArrayList<>();
        for (NamedExpression slot : star.getExceptedSlots()) {
            String name = outputLabelOf(slot);
            if (name == null) {
                throw new UnalignableOutputLabelsException(
                        "the caller's * EXCEPT column name cannot be read; the headers"
                                + " cannot be positioned");
            }
            excepted.add(name);
        }
        if (excepted.isEmpty()) {
            return labels;
        }
        List<String> adjusted = new ArrayList<>(labels.size());
        int removed = 0;
        for (String label : labels) {
            boolean remove = false;
            for (String exceptedName : excepted) {
                if (label != null && (label.equals(exceptedName)
                        || label.equals(lastNamePart(exceptedName)))) {
                    remove = true;
                    break;
                }
            }
            if (remove) {
                removed++;
            } else {
                adjusted.add(label);
            }
        }
        if (removed != excepted.size()) {
            throw new UnalignableOutputLabelsException(
                    "the caller's * EXCEPT payload does not match the replay's derived"
                            + " output labels; the headers cannot be positioned");
        }
        return adjusted;
    }

    /**
     * The REPLACE half of one payload star's label derivation: each replacement
     * renames the output position it targets. The analyzer resolves the alias NAME
     * against the relation's scope with identifier-case rules and swaps the matching
     * column's item for the Alias, so the CALLER's own header for that position is the
     * alias name (SELECT * REPLACE (K + 1 AS k) FROM (SELECT id AS K FROM t) s reports
     * `k`). Applying only the EXCEPT payload left the underlying label `K` and
     * alignRootOutputLabels renamed the frozen `k` output to `K` even for an identical
     * replay. A replacement addressing no derived position (or more than one) DECLINES:
     * the analyzer rejects such a query, so guessing here would report headers the
     * caller's own analysis never produced.
     */
    private static List<String> applyReplacedAliases(UnboundStar star, List<String> labels) {
        if (star.getReplacedAlias().isEmpty()) {
            return labels;
        }
        List<String> adjusted = new ArrayList<>(labels);
        for (NamedExpression replaced : star.getReplacedAlias()) {
            String aliasName = outputLabelOf(replaced);
            if (aliasName == null) {
                throw new UnalignableOutputLabelsException(
                        "the caller's * REPLACE column name cannot be read; the headers"
                                + " cannot be positioned");
            }
            int matches = 0;
            for (int i = 0; i < adjusted.size(); i++) {
                String label = adjusted.get(i);
                if (label != null && (label.equalsIgnoreCase(aliasName)
                        || lastNamePart(label).equalsIgnoreCase(aliasName))) {
                    adjusted.set(i, aliasName);
                    matches++;
                }
            }
            if (matches != 1) {
                throw new UnalignableOutputLabelsException(
                        "the caller's * REPLACE column " + aliasName
                                + " does not address exactly one derived output label;"
                                + " the headers cannot be positioned");
            }
        }
        return adjusted;
    }

    /** One slot name's last component (an EXCEPT column may be written qualified). */
    private static String lastNamePart(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(dot + 1);
    }

    /**
     * Realigns the frozen output list when the caller's star has an unknown-width tail
     * FOLLOWED by further items (expandedOutputLabels returns null then): the caller's
     * own positions cannot be derived, but the FROZEN list fixes the arity, so the
     * trailing items are positioned from the END and the star contributes its derivable
     * PREFIX - the positions behind that prefix are real column names (a base table's,
     * or a join's underivable side) that keep their frozen labels. The previous outcome
     * returned the tree untouched, so a trailing item whose value was substituted kept
     * the captured header text (SELECT *, c + 2 FROM u reported c + 1). A shape whose
     * trailing items cannot be positioned declines the rewrite.
     */
    private static LogicalPlan alignOpenStarWithTrailingItems(Plan rewrittenNode,
            Plan userNode, List<NamedExpression> userItems,
            List<NamedExpression> rewrittenItems, Plan rewritten,
            java.util.ArrayDeque<Plan> rewrittenAncestors) {
        int starIndex = -1;
        List<String> prefix = null;
        if (userNode.children().size() == 1) {
            for (int i = 0; i < userItems.size(); i++) {
                NamedExpression item = userItems.get(i);
                if (!(item instanceof UnboundStar)
                        || hasStarPayload((UnboundStar) item)) {
                    continue;
                }
                StarLabels expanded = starExpansionForStar(userNode.child(0),
                        (UnboundStar) item);
                if (expanded.openTail && i != userItems.size() - 1) {
                    starIndex = i;
                    prefix = expanded.labels;
                    break;
                }
            }
        }
        if (starIndex < 0) {
            throw new UnalignableOutputLabelsException(
                    "the caller's star expansion is not derivable and its following"
                            + " labels cannot be positioned");
        }
        int trailingCount = userItems.size() - starIndex - 1;
        int frozenCount = rewrittenItems.size();
        int tailStart = frozenCount - trailingCount;
        if (tailStart < starIndex + prefix.size()) {
            // the frozen list is too short: the trailing items would overlap the star's
            // derivable prefix, an arity the frozen text cannot describe
            throw new UnalignableOutputLabelsException(
                    "the replay's output arity cannot be aligned with the caller's star"
                            + " expansion");
        }
        List<NamedExpression> aligned = new ArrayList<>(frozenCount);
        boolean changed = false;
        for (int i = 0; i < frozenCount; i++) {
            NamedExpression rewrittenItem = rewrittenItems.get(i);
            String userLabel = null;
            NamedExpression userItem = null;
            if (i < starIndex) {
                userLabel = outputLabelOf(userItems.get(i));
                userItem = userItems.get(i);
            } else if (i < starIndex + prefix.size()) {
                userLabel = prefix.get(i - starIndex);
            } else if (i >= tailStart) {
                int userIndex = starIndex + 1 + (i - tailStart);
                userLabel = outputLabelOf(userItems.get(userIndex));
                userItem = userItems.get(userIndex);
            }
            String rewrittenLabel = outputLabelOf(rewrittenItem);
            if (userLabel == null || rewrittenLabel == null
                    || userLabel.equals(rewrittenLabel)) {
                // the middle positions (the star's unknown-width tail) keep the frozen
                // names: those are real column names, not captured expression text
                aligned.add(rewrittenItem);
                continue;
            }
            aligned.add(renameOutputItem(rewrittenItem, userLabel,
                    userItem != null && isDerivedAlias(userItem)));
            changed = true;
        }
        if (!changed) {
            return (LogicalPlan) rewritten;
        }
        Plan rebuilt = rebuildWithOutputItems(rewrittenNode, aligned);
        for (Plan ancestor : rewrittenAncestors) {
            rebuilt = ancestor.withChildren(java.util.List.of(rebuilt));
        }
        return (LogicalPlan) rebuilt;
    }

    /** One `*`'s expansion: the derivable leading labels plus whether the TRAILING
     * positions' count / labels are unknown (see starExpansion). */
    private static final class StarLabels {
        final List<String> labels;
        final boolean openTail;

        StarLabels(List<String> labels, boolean openTail) {
            this.labels = labels;
            this.openTail = openTail;
        }
    }

    /** Whether a star carries an EXCEPT / REPLACE payload (then its expansion is not a
     * plain projection and must not be derived here). */
    private static boolean hasStarPayload(UnboundStar star) {
        return !star.getExceptedSlots().isEmpty() || !star.getReplacedAlias().isEmpty();
    }

    /**
     * One star's expansion, honouring its QUALIFIER: an UNQUALIFIED star selects every
     * relation's columns in output order (see starExpansion), while table.* selects ONLY
     * the columns of the relation its qualifier names - expanding the whole join for it
     * took the derived label of the WRONG side as the first selected output and renamed
     * e.g. u's first column to the caller's derived k + 2 (SELECT u.* FROM
     * (SELECT k + 1 FROM t) s CROSS JOIN u). A qualifier naming a relation with an
     * unknown-width expansion (a base table) keeps the frozen names - those ARE that
     * relation's real column names; a qualifier that cannot be resolved DECLINES the
     * replay instead of guessing.
     */
    private static StarLabels starExpansionForStar(Plan relation, UnboundStar star) {
        List<String> qualifier = star.getQualifier();
        if (qualifier == null || qualifier.isEmpty()) {
            return starExpansion(relation);
        }
        StarLabels qualified = qualifiedStarExpansion(relation, qualifier);
        if (qualified == null) {
            throw new UnalignableOutputLabelsException(
                    "the caller's qualified star " + star.toSql()
                            + " cannot be resolved against the replay's relations; the"
                            + " caller's headers cannot be positioned");
        }
        return qualified;
    }

    /**
     * The expansion of a QUALIFIED star: the relation its qualifier names, found by
     * walking the JOIN sides of the relation the star projects from. Null when no side is
     * addressed by the qualifier (the caller's own text may be invalid, or the relation
     * hides behind a shape this walk cannot address - the caller then declines).
     */
    private static StarLabels qualifiedStarExpansion(Plan relation, List<String> qualifier) {
        if (relation instanceof LogicalJoin) {
            for (Plan child : relation.children()) {
                StarLabels side = qualifiedStarExpansion(child, qualifier);
                if (side != null) {
                    return side;
                }
            }
            return null;
        }
        if (!relationAnswersQualifier(relation, qualifier)) {
            return null;
        }
        return starExpansion(relation);
    }

    /**
     * Whether one relation - or the wrapper chain above it - is addressed by a star's
     * qualifier: an alias (a subquery alias, or a FROM alias wrapping the relation) wins
     * over the table name, exactly as the analyzer resolves it, and a qualified table
     * reference matches by its last name part.
     */
    private static boolean relationAnswersQualifier(Plan relation, List<String> qualifier) {
        String wanted = qualifier.get(qualifier.size() - 1);
        Plan node = relation;
        while (node != null && node.children().size() == 1
                && !(node instanceof LogicalSubQueryAlias)
                && !(node instanceof UnboundRelation)
                && !carriesOutputList(node)) {
            node = node.child(0);
        }
        if (node instanceof LogicalSubQueryAlias) {
            return wanted.equals(((LogicalSubQueryAlias<?>) node).getAlias());
        }
        if (node instanceof UnboundRelation) {
            List<String> parts = ((UnboundRelation) node).getNameParts();
            return !parts.isEmpty() && wanted.equals(parts.get(parts.size() - 1));
        }
        return false;
    }

    /**
     * The labels a caller-side star expands to, derived from the CALLER's own tree: the
     * output items of the relation the star reads from (following the subquery-alias /
     * wrapper chain), or - for a JOIN relation - the derivation of its
     * sides IN OUTPUT ORDER. A DERIVED leading side contributes its labels; a base table
     * / underivable side expands to real column names that need no realignment, so it may
     * only be the TAIL: the expansion then ends with an open tail (its arity is unknown
     * at parse time) and the positions behind the derivable prefix keep their frozen
     * names. The previous implementation returned null for every join, and a
     * SELECT * over (SELECT k + 1 ...) s CROSS JOIN u then kept the
     * captured frozen sink label k + 1 for a k + 2 caller.
     *
     * @param relation the relation the star projects from
     * @return the derivable label prefix plus whether the tail is open
     */
    private static StarLabels starExpansion(Plan relation) {
        Plan node = relation;
        while (node != null && node.children().size() == 1 && !carriesOutputList(node)
                && (node instanceof LogicalSubQueryAlias
                        || node instanceof LogicalFilter
                        || (node instanceof LogicalSink
                                && ((LogicalSink<?>) node).getOutputExprs().isEmpty()))) {
            // LogicalFilter passes its child's output through unchanged: WITHOUT it an
            // outer WHERE over a derived projection fell to the empty open tail and the
            // frozen capture-time label survived the value substitution
            node = node.child(0);
        }
        if (node != null && carriesOutputList(node)) {
            List<String> labels = expandedOutputLabels(outputItemsOf(node), node);
            if (labels == null) {
                return new StarLabels(List.of(), true);
            }
            if (!labels.isEmpty() && labels.get(labels.size() - 1) == OPEN_TAIL_LABEL) {
                return new StarLabels(
                        new ArrayList<>(labels.subList(0, labels.size() - 1)), true);
            }
            return new StarLabels(labels, false);
        }
        if (node instanceof LogicalUsingJoin) {
            // USING merges every key into ONE leading output column (in USING order), then
            // the left side's OTHER columns, then the right side's: concatenating the two
            // sides kept the LEFT copy of the key at position 0 and shifted every derived
            // label behind it (see usingJoinStarExpansion)
            return usingJoinStarExpansion((LogicalUsingJoin<?, ?>) node);
        }
        if (node instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) node;
            List<Plan> children = node.children();
            if (join.getJoinType().isSemiOrAntiJoin() && children.size() == 2) {
                // A SEMI / ANTI join exposes ONLY its preserved side: LEFT SEMI / ANTI /
                // NULL-AWARE-LEFT-ANTI return the LEFT child's columns, RIGHT SEMI / ANTI
                // the RIGHT child's. Appending the DISCARDED side's labels first shifted
                // every position, and a matching caller had the surviving column renamed
                // with the other side's derived label (SELECT * FROM (SELECT id, v + 1
                // FROM t) s RIGHT SEMI JOIN u ON s.id = u.id renamed u.x to v + 2).
                return withMarkLabel(join, starExpansion(
                        join.getJoinType().isRightSemiOrAntiJoin()
                                ? children.get(1) : children.get(0)));
            }
            List<String> labels = new ArrayList<>();
            boolean openSeen = false;
            for (int i = 0; i < children.size(); i++) {
                StarLabels child = starExpansion(children.get(i));
                if (child.openTail) {
                    if (i == children.size() - 1) {
                        return new StarLabels(labels, true);
                    }
                    // an underivable side before further output: if a LATER side is
                    // derivable, its labels cannot be positioned behind the unknown width
                    // - exposing the frozen names for them reported the captured header
                    // (see UnalignableOutputLabelsException)
                    openSeen = true;
                    continue;
                }
                if (openSeen) {
                    throw new UnalignableOutputLabelsException(
                            "an unknown-width join input precedes derivable output labels;"
                                    + " the caller's headers cannot be positioned");
                }
                labels.addAll(child.labels);
            }
            return withMarkLabel(join, new StarLabels(labels, openSeen));
        }
        return new StarLabels(List.of(), true);
    }

    /**
     * Appends a MARK join's mark slot label AFTER the side / joined labels: a mark
     * join's ASTERISK output exposes the mark column there (see
     * LogicalJoin#computeAsteriskOutput), so omitting it left the caller's derived
     * list SHORTER than the frozen sink's - alignRootOutputLabels then gave up on a
     * literal-variant match and kept reporting the CAPTURED headers (the stale
     * `v + 1` header of the mark-join query). An open tail already covers the
     * trailing positions (the mark slot is the LAST one); a mark label that cannot
     * be derived DECLINES the replay instead of guessing.
     */
    private static StarLabels withMarkLabel(LogicalJoin<?, ?> join, StarLabels labels) {
        if (!join.isMarkJoin() || labels.openTail) {
            return labels;
        }
        String markLabel = outputLabelOf(join.getMarkJoinSlotReference().get());
        if (markLabel == null) {
            throw new UnalignableOutputLabelsException(
                    "the caller's mark-join slot label cannot be derived; the caller's"
                            + " headers cannot be positioned");
        }
        List<String> extended = new ArrayList<>(labels.labels);
        extended.add(markLabel);
        return new StarLabels(extended, false);
    }

    /**
     * The labels a USING join's `*` exposes: each key merges into ONE leading column (in
     * USING order), then the PRESERVED side's remaining columns. The merged column
     * carries the key's RESOLVED name - the analyzer renames it from the merge-source
     * side's own column (BindExpression#bindUsingJoin analyzes the key against the child
     * scopes and emits Alias(slot, slot.getName())) - so the derivation matches the key
     * against that side's labels with the ANALYZER's identifier-case rules instead of
     * comparing the raw spelling (USING(k) binds a column aliased K). A SEMI / ANTI
     * join exposes ONLY its preserved side (asterisk-binding reads
     * join.getAsteriskOutput(), which returns that child alone for those join types), so
     * the discarded side must not contribute labels; a side whose expansion is
     * underivable ends the derivation with an open tail (its columns are real names),
     * while a DERIVABLE side behind an open one cannot be positioned and declines.
     */
    private static StarLabels usingJoinStarExpansion(LogicalUsingJoin<?, ?> join) {
        List<Plan> children = join.children();
        boolean semiOrAnti = join.getJoinType().isSemiOrAntiJoin()
                && children.size() == 2;
        boolean rightPreserved = join.getJoinType().isRightJoin()
                || join.getJoinType().isRightSemiOrAntiJoin();
        // the sides contributing VISIBLE non-key columns, in output order
        List<Integer> visible = new ArrayList<>();
        if (semiOrAnti) {
            visible.add(rightPreserved ? 1 : 0);
        } else {
            visible.add(0);
            visible.add(1);
        }
        StarLabels[] sides = new StarLabels[children.size()];
        for (int index : visible) {
            sides[index] = starExpansion(children.get(index));
        }
        List<String> labels = new ArrayList<>();
        // the identifier-case comparison set: the analyzer's key consumption follows the
        // bound slots, where a derived name only differs from the raw key by case
        Set<String> consumedKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        int keySource = rightPreserved ? 1 : 0;
        for (Expression key : join.getUsingSlots()) {
            String raw = outputLabelOf(key);
            if (raw == null) {
                throw new UnalignableOutputLabelsException(
                        "the caller's USING key label cannot be derived; the headers"
                                + " cannot be positioned");
            }
            String name = resolvedUsingKeyLabel(raw, sides[keySource]);
            labels.add(name);
            consumedKeys.add(name);
        }
        boolean openSeen = false;
        for (int i = 0; i < visible.size(); i++) {
            StarLabels side = sides[visible.get(i)];
            if (side.openTail) {
                if (i == visible.size() - 1) {
                    return new StarLabels(labels, true);
                }
                openSeen = true;
                continue;
            }
            if (openSeen) {
                throw new UnalignableOutputLabelsException(
                        "an unknown-width USING input precedes derivable output labels;"
                                + " the caller's headers cannot be positioned");
            }
            for (String label : side.labels) {
                // the key columns are CONSUMED by the merge (one output column per key,
                // already contributed above): every side's copy must be skipped or every
                // later position shifts. The comparison follows the analyzer's
                // IDENTIFIER-case rules (USING(k) binds a column aliased K), and a
                // QUALIFIED label is matched by its last component like the scope does.
                if (label == null || !isConsumedUsingKey(consumedKeys, label)) {
                    labels.add(label);
                }
            }
        }
        return new StarLabels(labels, false);
    }

    /**
     * One USING key's caller-visible label: the merge-source side's OWN label for the
     * key (identifier-case match, qualified labels compared by their last component),
     * falling back to the raw spelling when that side's expansion is underivable.
     */
    private static String resolvedUsingKeyLabel(String rawKeyName, StarLabels sourceSide) {
        if (sourceSide != null && !sourceSide.openTail) {
            for (String label : sourceSide.labels) {
                if (label != null && (label.equalsIgnoreCase(rawKeyName)
                        || lastNamePart(label).equalsIgnoreCase(rawKeyName))) {
                    return label;
                }
            }
        }
        return rawKeyName;
    }

    /** Whether one side label IS one of the merged USING keys. */
    private static boolean isConsumedUsingKey(Set<String> consumedKeys, String label) {
        return consumedKeys.contains(label)
                || consumedKeys.contains(lastNamePart(label));
    }

    /**
     * Re-attaches the caller's SET_VAR hints to the replay. The hint is applied by
     * EliminateLogicalSelectHint while the statement is planned, but the SPM replay
     * replaces the whole tree BEFORE that pass and the frozen text carries no hint, so
     * the replay would otherwise run under the session's ORIGINAL settings - a
     * SET_VAR(time_zone='+08:00') caller evaluated from_unixtime(epoch_col) in its own
     * zone and returned different values. The hint sets a STATEMENT-scoped session
     * variable (SelectHintSetVar#setVarOnceInSql writes the SessionVariable and the
     * value is reverted at the end of the statement), so the BLOCK a hint sits in does
     * not scope its effect: collecting EVERY SET_VAR hint of the caller's tree and
     * placing them on the replay's top re-establishes exactly the settings the original
     * planning would have applied. (Placing them one by one on replayed sub-blocks was
     * tried first, but the replay's wrapper shape - result sink, EXPLAIN, project over
     * a subquery alias - is not stable enough to address them structurally, and a
     * misclassified hint silently disabled the whole rewrite.) The hint payload is part
     * of the match key (sameSelectHints), so the collected hints are the captured
     * query's own settings.
     *
     * @param replay the aligned replay tree
     * @param caller the caller's statement tree (the matched namespace copy)
     * @return the replay with the caller's SET_VAR hints re-attached (unchanged when
     *         there are none)
     */
    public static LogicalPlan carrySetVarHints(LogicalPlan replay, LogicalPlan caller) {
        List<SelectHint> setVars = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(caller, plan -> {
            if (!(plan instanceof LogicalSelectHint)) {
                return;
            }
            for (SelectHint hint : ((LogicalSelectHint<?>) plan).getHints()) {
                if (hint instanceof SelectHintSetVar) {
                    setVars.add(hint);
                }
            }
        });
        if (setVars.isEmpty()) {
            return replay;
        }
        return new LogicalSelectHint<>(com.google.common.collect.ImmutableList.copyOf(setVars),
                replay);
    }

    /**
     * Signals that the replay's caller-visible labels cannot be aligned with the caller's
     * own output labels (see alignRootOutputLabels): the caller must SKIP the rewrite
     * instead of exposing the CAPTURED headers (a SET root the frozen text wrapped in a
     * labeled projection, a derivable label behind an unknown-width join input).
     */
    public static class UnalignableOutputLabelsException extends RuntimeException {
        public UnalignableOutputLabelsException(String message) {
            super(message);
        }
    }

    /** The caller-visible output list of a node that carriesOutputList. */
    private static List<NamedExpression> outputItemsOf(Plan node) {
        if (node instanceof LogicalProject) {
            return ((LogicalProject<?>) node).getProjects();
        }
        if (node instanceof LogicalAggregate) {
            return ((LogicalAggregate<?>) node).getOutputExpressions();
        }
        if (node instanceof LogicalWindow) {
            return ((LogicalWindow<?>) node).getWindowExpressions();
        }
        return ((LogicalSink<?>) node).getOutputExprs();
    }

    /** One output item renamed to label, keeping the parse-time class. */
    private static NamedExpression renameOutputItem(NamedExpression item, String label,
            boolean nameFromChild) {
        if (item instanceof UnboundAlias) {
            return new UnboundAlias(((UnboundAlias) item).child(), label, nameFromChild);
        }
        if (item instanceof Alias) {
            return new Alias(((Alias) item).child(), label, nameFromChild);
        }
        // a BARE item (a plain column of the frozen text): wrap it in an alias carrying
        // the caller's label, mirroring the parse-time class of an unbound slot
        if (item instanceof UnboundSlot) {
            return new UnboundAlias(item, label, nameFromChild);
        }
        return new Alias(item, label, nameFromChild);
    }

    /**
     * The label one top-level output item exposes: its explicit alias when it has one, a
     * derived (nameFromChild) alias text, otherwise the BARE column name of a slot.
     *
     * The bare-column case is a real contract gap: a manual plan may render a bind
     * column under another label (CREATE BASELINE PLAN 'SELECT k FROM t' WITH
     * 'SELECT k AS other FROM t'), so the frozen sink exposes other while the
     * caller's item stays a bare UnboundSlot. Returning null here kept the
     * captured alias as the caller's JDBC column label instead of restoring k.
     */
    private static String outputLabelOf(Expression item) {
        String explicit = explicitAliasName(item);
        if (explicit != null) {
            return explicit;
        }
        String derived = derivedAliasName(item);
        if (derived != null) {
            return derived;
        }
        // A STAR is a Slot subclass but its "name" is not a caller label: it expands to
        // the real column names downstream, so the alignment must leave the tree alone
        // (the same reason the arity check exists).
        if (item instanceof UnboundStar) {
            return null;
        }
        if (item instanceof UnboundSlot) {
            return ((UnboundSlot) item).getName();
        }
        if (item instanceof Slot) {
            return ((Slot) item).getName();
        }
        return null;
    }

    /** The derived (nameFromChild) alias text of one expression, or null. */
    private static String derivedAliasName(Expression expr) {
        if (expr instanceof Alias && ((Alias) expr).isNameFromChild()) {
            return ((Alias) expr).getName();
        }
        if (expr instanceof UnboundAlias && ((UnboundAlias) expr).isNameFromChild()) {
            return ((UnboundAlias) expr).getAlias().orElse(null);
        }
        return null;
    }

    /** Whether the expression is a nameFromChild (derived) alias. */
    private static boolean isDerivedAlias(Expression expr) {
        if (expr instanceof Alias) {
            return ((Alias) expr).isNameFromChild();
        }
        if (expr instanceof UnboundAlias) {
            return ((UnboundAlias) expr).isNameFromChild();
        }
        return false;
    }

    /**
     * The user-written name of an explicit output alias (Alias or parse-time
     * UnboundAlias), or null when the expression is not one. A nameFromChild
     * fallback is the expression text rather than an identifier and does not qualify.
     */
    private static String explicitAliasName(Expression expr) {
        if (expr instanceof Alias) {
            Alias alias = (Alias) expr;
            return alias.isNameFromChild() ? null : alias.getName();
        }
        if (expr instanceof UnboundAlias) {
            UnboundAlias alias = (UnboundAlias) expr;
            return !alias.isNameFromChild() && alias.getAlias().isPresent()
                    ? alias.getAlias().get() : null;
        }
        return null;
    }

    /**
     * Compares every non-parameterizable scan identity field of a base-table relation:
     * partition selection, tablet selection, hints, index, sample, snapshot and scan
     * parameters. TableSnapshot / TableScanParams have no value-based
     * equals, so their stable textual form is compared as well.
     */
    private static boolean sameScanIdentity(UnboundRelation bind, UnboundRelation user) {
        // formal and TEMPORARY partitions are two namespaces that may carry the SAME
        // partition names (PARTITION(p1) vs TEMPORARY PARTITION(p1)); after a
        // formal/temp lifecycle transition the name list alone would match a baseline
        // frozen against the other namespace and replay the now-wrong data (failing
        // with fallback disabled). The namespaces must be compared in both directions.
        return bind.isTempPart() == user.isTempPart()
                && sameSelectionIgnoreOrder(bind.getPartNames(), user.getPartNames())
                && sameSelectionIgnoreOrder(bind.getTabletIds(), user.getTabletIds())
                && Objects.equals(bind.getHints(), user.getHints())
                && Objects.equals(bind.getIndexName(), user.getIndexName())
                && sameOptionalValue(bind.getTableSample(), user.getTableSample())
                && sameOptionalValue(bind.getTableSnapshot(), user.getTableSnapshot())
                && sameScanParams(bind.getScanParams(), user.getScanParams());
    }

    /**
     * The stable textual identity of every CONCRETE (non-parameterizable) scan selector
     * of one base-table relation - partition / tablet selection, hints, index, sample,
     * snapshot and scan parameters (each with the SAME semantics
     * sameScanIdentity compares: selections are multisets, sample / snapshot
     * compare by value or text, scan parameters by type + payloads). The audit dedup
     * uses this description to keep two same-digest statements apart when their concrete
     * selectors differ: the digest renders PARTITION(p1) and PARTITION(p2) both as
     * PARTITION(?).
     */
    public static String describeScanSelector(UnboundRelation relation) {
        List<String> partitions = new ArrayList<>();
        if (relation.getPartNames() != null) {
            partitions.addAll(relation.getPartNames());
        }
        Collections.sort(partitions);
        List<String> tablets = new ArrayList<>();
        if (relation.getTabletIds() != null) {
            for (Long tablet : relation.getTabletIds()) {
                tablets.add(String.valueOf(tablet));
            }
        }
        Collections.sort(tablets);
        TableScanParams scanParams = relation.getScanParams();
        String scanParamsText = scanParams == null ? ""
                : String.valueOf(scanParams.getParamType()) + '|'
                        + String.valueOf(scanParams.getMapParams()) + '|'
                        + String.valueOf(scanParams.getListParams());
        return (relation.isTempPart() ? "temp" : "formal")
                + '|' + partitions
                + '|' + tablets
                + '|' + String.valueOf(relation.getHints())
                + '|' + String.valueOf(relation.getIndexName())
                + '|' + describeTableSample(relation.getTableSample().orElse(null))
                + '|' + relation.getTableSnapshot().map(Object::toString).orElse("")
                + '|' + scanParamsText;
    }

    /**
     * Selector rendering of a TABLESAMPLE clause from its FIELDS. The default
     * Object#toString must never be used here: TableSample overrides
     * equals / hashCode by value but not toString, so its default
     * text is the IDENTITY hash - unstable across parses and process runs. It made one
     * query's audit fingerprint differ between two parses of the same statement (the
     * dedup identity then no longer collapsed them) and made a bind / plan pair carrying
     * the SAME sample render as two different selectors (a legitimate baseline rejected
     * by the mismatch guard).
     *
     * @param sample the sample clause, may be null
     * @return the stable field rendering, empty when there is no sample
     */
    @VisibleForTesting
    static String describeTableSample(TableSample sample) {
        if (sample == null) {
            return "";
        }
        return (sample.isPercent ? "percent:" : "rows:") + sample.sampleValue
                + ",seek:" + sample.seek;
    }

    /** The scan-selector descriptions of every base-table relation in the plan. */
    public static String scanSelectorFingerprint(Plan plan) {
        StringBuilder sb = new StringBuilder();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof UnboundRelation) {
                sb.append(describeScanSelector((UnboundRelation) node)).append('\u0002');
            }
        });
        return sb.toString();
    }

    /** The ids of every SPM placeholder reachable from the plan (subquery plans included). */
    public static Set<Long> collectPlaceholderIds(Plan plan) {
        Set<Long> ids = new HashSet<>();
        collectPlaceholderIds(plan, ids);
        return ids;
    }

    /** Recurses one plan collecting placeholder ids from its nodes' expressions. */
    private static void collectPlaceholderIds(Plan plan, Set<Long> ids) {
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expr : node.getExpressions()) {
                collectPlaceholderIds(expr, ids);
            }
            for (Expression expr : outOfBandExpressions(node)) {
                collectPlaceholderIds(expr, ids);
            }
        });
    }

    /** Recurses one expression tree collecting placeholder ids. */
    private static void collectPlaceholderIds(Expression expr, Set<Long> ids) {
        if (expr instanceof SpmConstVar) {
            ids.add(((SpmConstVar) expr).getId());
        } else if (expr instanceof SpmConstList) {
            ids.add(((SpmConstList) expr).getId());
        }
        if (expr instanceof SubqueryExpr) {
            collectPlaceholderIds(((SubqueryExpr) expr).getQueryPlan(), ids);
        }
        for (Expression child : expr.children()) {
            collectPlaceholderIds(child, ids);
        }
    }

    /**
     * Partition and tablet selections are multisets: FROM t PARTITION(p1, p2) / TABLET(1,
     * 2) read exactly the same data as the opposite order, while the decompiler emits the
     * selection in id order regardless of how the user ordered it. A size + HashSet
     * comparison was too weak: it equated (p1, p1, p2) with (p1, p2, p2) - the default
     * OLAP_SCAN_PARTITION_PRUNE rewrite deduplicates those ids, but
     * disable_nereids_rules=OLAP_SCAN_PARTITION_PRUNE is a supported setting that SPM
     * deliberately preserves, and on that path the duplicate entries reach the scan
     * ranges and the frozen SQL keeps the captured multiplicity - so a matched count
     * could double a DIFFERENT partition than the user asked for. Frequency maps compare
     * the real multiplicities.
     */
    private static boolean sameSelectionIgnoreOrder(List<?> bind, List<?> user) {
        if (bind == null || user == null) {
            return bind == user;
        }
        if (bind.equals(user)) {
            return true;
        }
        if (bind.size() != user.size()) {
            return false;
        }
        Map<Object, Integer> frequencies = new HashMap<>();
        for (Object entry : bind) {
            frequencies.merge(entry, 1, Integer::sum);
        }
        for (Object entry : user) {
            Integer remaining = frequencies.get(entry);
            if (remaining == null || remaining == 0) {
                return false;
            }
            frequencies.put(entry, remaining - 1);
        }
        return true;
    }

    /** For tests: the partition / tablet multiset comparison of the L3 scan identity. */
    @VisibleForTesting
    public static boolean sameSelectionIgnoreOrderForTest(List<?> bind, List<?> user) {
        return sameSelectionIgnoreOrder(bind, user);
    }

    /** For tests: the L3 scan-identity comparison (incl. the temp-partition namespace). */
    @VisibleForTesting
    public static boolean sameScanIdentityForTest(UnboundRelation bind, UnboundRelation user) {
        return sameScanIdentity(bind, user);
    }

    /**
     * Rejects a manual plan whose SCAN SELECTORS diverge from the bind text's for any
     * table both statements read.
     *
     * The bind text is the MATCHING KEY: a caller matching it carries the BIND's
     * selection (sameScanIdentity), while the frozen plan scans the PLAN's. The pair
     * CREATE ... 'SELECT k FROM t' WITH 'SELECT k FROM t PARTITION(p1)' is
     * initially equivalent (only p1 exists), but after ADD PARTITION p2 the
     * unpinned caller still matches and the frozen plan silently reads only p1 - every
     * row in p2 disappears from the result. The mirror case silently WIDENS the result,
     * and the same holds for a plan-only TABLET() / TABLESAMPLE() / FOR TIMESTAMP AS OF
     * / index selection. The schema fingerprint cannot catch any of this (it hashes the
     * table identity and base columns, not the selectors), so such a pair is rejected
     * here - write both statements with the same selection.
     *
     * @param bindPlan the parsed (unbound) bind tree
     * @param planPlan the parsed (unbound) plan tree (may be the same object)
     * @param bindSql  the bind text (for the error message)
     */
    public static void rejectScanSelectorMismatch(Plan bindPlan, Plan planPlan, String bindSql) {
        if (bindPlan == planPlan) {
            return; // one parse (bindSql == planSql): symmetric by construction
        }
        Map<String, List<ScanSelectorOccurrence>> bindSelectors = scanSelectorsByTable(bindPlan);
        Map<String, List<ScanSelectorOccurrence>> planSelectors = scanSelectorsByTable(planPlan);
        for (Map.Entry<String, List<ScanSelectorOccurrence>> entry : bindSelectors.entrySet()) {
            if (!planSelectors.containsKey(entry.getKey())) {
                // The plan does not read this table at all (it is a manual plan over its
                // OWN table set - the fingerprint covers the plan-side tables): there is
                // no selection to compare. Only a table BOTH statements read can carry a
                // divergent selection.
                continue;
            }
            List<ScanSelectorOccurrence> fromBind = entry.getValue();
            List<ScanSelectorOccurrence> fromPlan = planSelectors.get(entry.getKey());
            if (scanSelectorsAligned(fromBind, fromPlan)) {
                continue;
            }
            List<String> bindSelectorList = selectorOnly(fromBind);
            List<String> planSelectorList = selectorOnly(fromPlan);
            if (sameSelectionIgnoreOrder(bindSelectorList, planSelectorList)) {
                // The SAME selector multiset sits on DIFFERENT occurrences. Comparing only
                // the per-table multiset accepted a reversed self join - bind
                // `t PARTITION(p1) a CROSS JOIN t PARTITION(p2) b` vs a manual plan writing
                // `t PARTITION(p1) b CROSS JOIN t PARTITION(p2) a` produced the identical
                // list `t -> [p1, p2]`, so CREATE accepted them, and with k=1 in p1 and
                // k=11 in p2 the bind returned (1,11) while the frozen plan returned
                // (11,1). Each occurrence keeps its own pin, so the
                // pin/occurrence mapping is only verifiable when the alias-attached lists
                // line up: a self-join occurrence carries its own PARTITION / TABLET /
                // TABLESAMPLE / index selection, and after a partition change the replay
                // silently returns the other pairing. Reject it: write the selections under
                // the SAME aliases in the same occurrence order in both statements.
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "SPM cannot align the plan SQL with the bind SQL: the scan selectors"
                                + " of table '" + entry.getKey() + "' are the same set but pin"
                                + " DIFFERENT occurrences (bind side " + fromBind + ", plan side "
                                + fromPlan + ", in statement order). A self-join occurrence"
                                + " keeps its own PARTITION / TABLET / TABLESAMPLE / index"
                                + " selection, so the pin/occurrence mapping is ambiguous - after"
                                + " a partition change the replay silently returns the other"
                                + " pairing. Write the selections under the SAME aliases in the"
                                + " same occurrence order in both statements: " + bindSql);
            }
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: their scan"
                            + " selectors of table '" + entry.getKey() + "' differ (bind side "
                            + fromBind + ", plan side " + fromPlan + "). The bind text is the"
                            + " matching key, so every caller matching it carries the BIND's"
                            + " selection while the frozen plan would read the plan's own one"
                            + " - after a partition change rows silently appear or disappear."
                            + " Write both statements with the same PARTITION / TABLET /"
                            + " TABLESAMPLE / FOR TIMESTAMP / index selection: " + bindSql);
        }
    }

    /**
     * Whether two occurrences of one table carry the SAME scan selector, attached to the
     * SAME occurrence. A table read ONCE per statement is compared by its selector alone
     * (the alias cannot disambiguate anything, and a manual plan may name its relation
     * differently). With SEVERAL occurrences of the same table the comparison is by the
     * pair (alias, selector) in statement order: the reversed self join of
     * produced identical per-table selector lists while pinning them to swapped aliases,
     * and every replay then returned the other pairing.
     *
     * @param bind the bind side's occurrences, in statement order
     * @param plan the plan side's occurrences, in statement order
     * @return whether the two statements' scan selectors agree
     */
    private static boolean scanSelectorsAligned(List<ScanSelectorOccurrence> bind,
            List<ScanSelectorOccurrence> plan) {
        if (bind.size() != plan.size()) {
            return false;
        }
        if (bind.size() <= 1) {
            // single occurrence (or none): selector only - aliases are interchangeable here
            return bind.isEmpty() || bind.get(0).selector.equals(plan.get(0).selector);
        }
        for (int i = 0; i < bind.size(); i++) {
            if (!bind.get(i).alias.equals(plan.get(i).alias)
                    || !bind.get(i).selector.equals(plan.get(i).selector)) {
                return false;
            }
        }
        return true;
    }

    /** The selector-only view of an occurrence list (used for the error-message choice). */
    private static List<String> selectorOnly(List<ScanSelectorOccurrence> occurrences) {
        List<String> selectors = new ArrayList<>();
        for (ScanSelectorOccurrence occurrence : occurrences) {
            selectors.add(occurrence.selector);
        }
        return selectors;
    }

    /**
     * Rejects a manual plan whose LOGICAL query diverges from the bind text.
     * The bind text is the MATCHING KEY: any caller matching it gets the
     * plan side replayed with the caller's values, so a plan that drops the caller's row
     * filter, reads another table, changes the output columns / arity or re-orders the
     * caller's uncapped result silently returns rows the caller never asked for - and
     * neither the schema fingerprint nor the scan-selector guard can see it.
     *
     * The four contracts checked here:
     *    sources: every table the plan reads is read by the bind text - bind
     *       SELECT k FROM t with plan SELECT k FROM u makes a caller's
     *       t-query return u's rows (t={1}, u={9} returns 9);
     *    output: the two output lists have the same ARITY and, per position,
     *       the same underlying expression (only the LABEL may differ - a manual plan may
     *       render a bind column under another name and the replay restores the caller's
     *       label). Bind k vs plan v otherwise exposes v's value under
     *       the caller's name (k=1, v=9 returns 9), a wider plan changes the result
     *       arity;
     *    filters: every row filter conjunct of the bind text (WHERE / HAVING)
     *       exists in the plan text - a dropped filter makes a caller restricted to
     *       k = 2 receive every row (e.g. {1,2});
     *    ordering: both trees expose the same TOP-LEVEL ORDER BY contract
     *       (keys, direction, null placement, or none) - an uncapped caller keeps its
     *       ordering contract, and a plan that flips the direction returns (2,1) for the
     *       caller that asked for (1,2).
     * After those set-wise contracts the two trees are compared NODE BY NODE and input by
     * input (see rejectRelationalTopologyDivergence): a predicate that MOVED to another
     * input, an operator that was added / dropped (DISTINCT, a window, a join type) and a
     * plan that shares only part of the bind's relations all pass the set-wise checks.
     * The checks compare the two PARSED trees, so an equivalent plan written in another
     * shape (requalified columns, reordered predicates) is rejected: write both
     * statements with the same tables, filters and ordering.
     *
     * @param bindPlan the parsed (unbound) bind tree
     * @param planPlan the parsed (unbound) plan tree (may be the same object)
     * @param bindSql  the bind text (for the error message)
     */
    public static void rejectManualPlanDivergence(Plan bindPlan, Plan planPlan, String bindSql) {
        if (bindPlan == planPlan) {
            return; // one parse (bindSql == planSql): symmetric by construction
        }
        Set<String> bindTables = scanSelectorsByTable(bindPlan).keySet();
        Set<String> planTables = scanSelectorsByTable(planPlan).keySet();
        if (!bindTables.containsAll(planTables)) {
            java.util.Set<String> extra = new java.util.LinkedHashSet<>(planTables);
            extra.removeAll(bindTables);
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: the plan reads table(s)"
                            + " " + extra + " the bind text never reads. A caller matching the"
                            + " bind text (over " + bindTables + ") would be silently"
                            + " answered with those tables' rows. Write the plan over the"
                            + " SAME tables as the bind text: " + bindSql);
        }
        if (!planTables.containsAll(bindTables)) {
            java.util.Set<String> missing = new java.util.LinkedHashSet<>(bindTables);
            missing.removeAll(planTables);
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: the plan does not read"
                            + " table(s) " + missing + " the bind text reads. A caller"
                            + " matching the bind text (over " + bindTables + ") would be"
                            + " answered from the plan's tables alone. Write the plan over the"
                            + " SAME tables as the bind text: " + bindSql);
        }
        List<NamedExpression> bindItems = topLevelOutputItems(bindPlan);
        List<NamedExpression> planItems = topLevelOutputItems(planPlan);
        if (bindItems == null || planItems == null) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: the two statements'"
                            + " output columns cannot be compared (an unsupported shape such"
                            + " as a set operation); write the plan as the same query shape:"
                            + " " + bindSql);
        }
        if (bindItems.size() != planItems.size()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: the two statements"
                            + " expose " + bindItems.size() + " vs " + planItems.size()
                            + " output columns, so a matching caller would receive a"
                            + " different result arity. Write the plan with the SAME output"
                            + " columns: " + bindSql);
        }
        for (int i = 0; i < bindItems.size(); i++) {
            String bindExpr = outputExpressionText(bindItems.get(i));
            String planExpr = outputExpressionText(planItems.get(i));
            if (!bindExpr.equals(planExpr)) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "SPM cannot align the plan SQL with the bind SQL: output column "
                                + (i + 1) + " reads '" + bindExpr + "' in the bind text but '"
                                + planExpr + "' in the plan text, so a matching caller would"
                                + " receive another column's value under its own name (only"
                                + " the column LABEL may differ). Write the plan with the"
                                + " SAME output expressions: " + bindSql);
            }
        }
        Set<String> bindFilters = filterConjunctTexts(bindPlan);
        Set<String> planFilters = filterConjunctTexts(planPlan);
        if (!planFilters.containsAll(bindFilters)) {
            java.util.Set<String> missing = new java.util.LinkedHashSet<>(bindFilters);
            missing.removeAll(planFilters);
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: the plan drops row"
                            + " filter(s) " + missing + " of the bind text, so a matching"
                            + " caller would receive rows it filtered out. Write the plan with"
                            + " the SAME filters: " + bindSql);
        }
        List<String> bindOrder = rootOrderContract(bindPlan);
        List<String> planOrder = rootOrderContract(planPlan);
        if (!bindOrder.equals(planOrder)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: their top-level"
                            + " ORDER BY contracts differ (bind " + bindOrder + ", plan "
                            + planOrder + "), so a matching caller would receive its rows in"
                            + " another order. Write the plan with the SAME ordering: "
                            + bindSql);
        }
        // The set-wise checks above cannot see an operator that CHANGED the rows, a
        // predicate that MOVED to another input, or an operator that was dropped: the
        // topology proof compares the two trees node by node and input by input
        // (see rejectRelationalTopologyDivergence).
        rejectRelationalTopologyDivergence(bindPlan, planPlan, bindSql);
    }

    /**
     * Proves that the manual plan's RELATIONAL TOPOLOGY equals the bind text's, operator
     * by operator and input by input: the same relation occurrences, the same join types
     * with the same join predicates, the same row-forming operators (aggregation / grouping
     * keys, set-operation kind, window functions, projections, sorts / row caps) and every
     * row filter attached to the SAME relational input.
     *
     * The plain CREATE BASELINE PLAN 'bind' WITH 'plan' pair stores the PLAN-side tree, and
     * neither replay path restores the caller's shape: a caller matching the bind text is
     * answered with the plan's rows. Every divergence this comparison rejects is therefore
     * a silent result change:
     *   relation occurrences / join type / ON condition: a bind
     *       SELECT t.k FROM t LEFT JOIN u ON t.k = u.k bound to an INNER JOIN plan returns
     *       no row for an unmatched t row although the caller returns it; a plan that OMITS
     *       u entirely (accepted by the previous one-way table check) answers a u-reading
     *       caller from t alone;
     *   row-forming operators: SELECT DISTINCT k FROM t binds to a
     *       manual SELECT k FROM t - both expose k, so the output check passed, while
     *       t={1,1} yields one caller row and two replay rows (the DISTINCT flag sits on
     *       the PROJECT node). The same gap admitted changed GROUP BY keys, UNION ALL
     *       against UNION, and a changed window function beneath an outer projection;
     *   filter placement: an EQUAL-TEXT containment check accepted SELECT k FROM t against
     *       a plan SELECT k FROM t WHERE k IS NOT NULL (it silently drops the caller's NULL
     *       rows), and even equal predicate text is not enough - moving u.v IS NOT NULL from
     *       after a LEFT JOIN into the join's right input turns an unmatched left row into a
     *       filtered one.
     * An equivalent plan written in ANOTHER shape is rejected: the comparison must prove
     * equivalence, not guess it - write both statements with the same relations (same names
     * and aliases), joins, operators and filter placement.
     *
     * @param bindPlan the parsed (unbound) bind tree
     * @param planPlan the parsed (unbound) plan tree (may be the same object)
     * @param bindSql  the bind text (for the error message)
     */
    public static void rejectRelationalTopologyDivergence(Plan bindPlan, Plan planPlan,
            String bindSql) {
        if (bindPlan == planPlan) {
            return; // one parse (bindSql == planSql): symmetric by construction
        }
        compareRelationalNodes(bindPlan, planPlan, "the root operator", bindSql,
                new IdentityHashMap<>(), true);
    }

    /**
     * One lockstep step of rejectRelationalTopologyDivergence: the two nodes must be the
     * SAME operator with the same payload, the same number of inputs, and recursively the
     * same subtree under every input. Expression subqueries (IN / EXISTS / scalar
     * subqueries in predicates and projections) and AUXILIARY plans (the WITH definitions
     * of a LogicalCTE) are compared as plans as well, so a divergence inside a subquery's
     * own query block or a CTE body cannot hide behind an opaque reference.
     *
     * @param visitedPairs the node pairs already proven equivalent (identity), so a SHARED
     *                     or recursive CTE reference cannot recurse forever
     * @param labelRelaxed whether this position is the ALIGNABLE root output list (see
     *                     alignRootOutputLabels: only that list may render a bare column
     *                     under another label; every nested list keeps its name-to-
     *                     expression mapping because parents resolve through it)
     */
    private static void compareRelationalNodes(Plan bind, Plan plan, String position,
            String bindSql, Map<Plan, Set<Plan>> visitedPairs, boolean labelRelaxed) {
        // SELECT hints carry no relational semantics (they decide planning knobs and the
        // CREATE-time parse mode, not which rows come out), and the bind text may carry a
        // SET_VAR hint its plan text does not: compare below them.
        bind = unwrapSelectHints(bind);
        plan = unwrapSelectHints(plan);
        if (bind == null || plan == null) {
            if (bind == plan) {
                return;
            }
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: " + position
                            + " exists only in the " + (bind == null ? "plan" : "bind")
                            + " text, so the plan's computation cannot be proven equivalent."
                            + " Write both statements with the same relational structure: "
                            + bindSql);
        }
        if (!visitedPairs.computeIfAbsent(bind, k -> Collections.newSetFromMap(
                new IdentityHashMap<>())).add(plan)) {
            return; // this node pair was already proven equivalent (shared / recursive CTE)
        }
        if (bind.getClass() != plan.getClass()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: " + position
                            + " is " + describeRelationalNode(bind) + " in the bind text but "
                            + describeRelationalNode(plan) + " in the plan text, so the plan's"
                            + " computation cannot be proven equivalent. Write both statements"
                            + " with the same operators: " + bindSql);
        }
        compareRelationalPayload(bind, plan, position, bindSql, labelRelaxed);
        List<Plan> bindChildren = bind.children();
        List<Plan> planChildren = plan.children();
        if (bindChildren.size() != planChildren.size()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: " + position
                            + " has " + bindChildren.size() + " input(s) in the bind text but "
                            + planChildren.size() + " in the plan text, so the plan's computation"
                            + " cannot be proven equivalent. Write both statements with the same"
                            + " relational structure: " + bindSql);
        }
        for (int i = 0; i < bindChildren.size(); i++) {
            // the alignable root list may pass through single-input wrappers (a LIMIT /
            // sort / subquery alias above the output-carrying node); once the carrying
            // node is reached, everything below it resolves through its names
            compareRelationalNodes(bindChildren.get(i), planChildren.get(i),
                    position + "'s input " + (i + 1), bindSql, visitedPairs,
                    labelRelaxed && bindChildren.size() == 1 && !carriesOutputList(bind));
        }
        List<? extends Plan> bindAuxiliary = bind.extraPlans();
        List<? extends Plan> planAuxiliary = plan.extraPlans();
        if (bindAuxiliary.size() != planAuxiliary.size()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: " + position
                            + " carries " + bindAuxiliary.size() + " auxiliary plan(s) (e.g."
                            + " WITH definitions) in the bind text but " + planAuxiliary.size()
                            + " in the plan text, so the plan's computation cannot be proven"
                            + " equivalent. Write both statements with the same CTEs: "
                            + bindSql);
        }
        for (int i = 0; i < bindAuxiliary.size(); i++) {
            compareRelationalNodes(bindAuxiliary.get(i), planAuxiliary.get(i),
                    position + "'s auxiliary plan " + (i + 1), bindSql, visitedPairs, false);
        }
        List<Plan> bindSubqueries = expressionSubqueryPlans(bind);
        List<Plan> planSubqueries = expressionSubqueryPlans(plan);
        if (bindSubqueries.size() != planSubqueries.size()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL: " + position
                            + " carries " + bindSubqueries.size() + " expression subquery(ies)"
                            + " in the bind text but " + planSubqueries.size() + " in the plan"
                            + " text, so the plan's computation cannot be proven equivalent."
                            + " Write both statements with the same subqueries: " + bindSql);
        }
        for (int i = 0; i < bindSubqueries.size(); i++) {
            compareRelationalNodes(bindSubqueries.get(i), planSubqueries.get(i),
                    position + "'s subquery " + (i + 1), bindSql, visitedPairs, false);
        }
    }

    /** The payload comparison of one operator pair (same class, see
     * compareRelationalNodes); labelRelaxed = the alignable root output list. */
    private static void compareRelationalPayload(Plan bind, Plan plan, String position,
            String bindSql, boolean labelRelaxed) {
        if (bind instanceof LogicalUsingJoin) {
            LogicalUsingJoin<?, ?> bindJoin = (LogicalUsingJoin<?, ?>) bind;
            LogicalUsingJoin<?, ?> planJoin = (LogicalUsingJoin<?, ?>) plan;
            if (bindJoin.getJoinType() != planJoin.getJoinType()) {
                throw topologyDivergence(position, "the join type differs (bind "
                        + bindJoin.getJoinType() + ", plan " + planJoin.getJoinType() + ")",
                        bindSql);
            }
            // the USING key list is POSITIONAL: the binder emits the merged key
            // columns in USING order (the caller's SELECT * leads with the first
            // USING column), so USING(a,b) is not interchangeable with USING(b,a)
            assertOrderedTexts(position, "the USING columns",
                    expressionTexts(bindJoin.getUsingSlots()),
                    expressionTexts(planJoin.getUsingSlots()), bindSql);
            // an ASOF join stores its MATCH_CONDITION beside the USING slots: at an EQUAL
            // timestamp l.d >= r.d picks the right row l.d > r.d skips - the replay
            // would choose another row while every other check passes
            Optional<Expression> bindMatch = bindJoin.getMatchCondition();
            Optional<Expression> planMatch = planJoin.getMatchCondition();
            // the boundary-preserving comparison: `a.d` (ONE quoted component) and the
            // qualified reference a.d render the same toSql while picking DIFFERENT right
            // rows, so the plain text equality admitted a manual plan with another
            // MATCH_CONDITION (see boundaryPreservingDeepText)
            if (bindMatch.isPresent() != planMatch.isPresent()
                    || (bindMatch.isPresent() && !boundaryPreservingDeepText(bindMatch.get())
                            .equals(boundaryPreservingDeepText(planMatch.get())))) {
                throw topologyDivergence(position, "the match condition differs (bind "
                        + (bindMatch.isPresent() ? bindMatch.get().toSql() : "none")
                        + ", plan " + (planMatch.isPresent() ? planMatch.get().toSql() : "none")
                        + "); an ASOF join picks its right row with it", bindSql);
            }
            return;
        }
        if (bind instanceof LogicalJoin) {
            LogicalJoin<?, ?> bindJoin = (LogicalJoin<?, ?>) bind;
            LogicalJoin<?, ?> planJoin = (LogicalJoin<?, ?>) plan;
            if (bindJoin.getJoinType() != planJoin.getJoinType()) {
                throw topologyDivergence(position, "the join type differs (bind "
                        + bindJoin.getJoinType() + ", plan " + planJoin.getJoinType()
                        + "); an unmatched row of one side survives an OUTER join but is"
                        + " dropped by an INNER one", bindSql);
            }
            // a MARK join keeps an unmatched left row with a mark slot while the plain
            // semi / anti form removes (or keeps) it: the same JoinType and ON predicate
            // do NOT make them interchangeable
            if (bindJoin.isMarkJoin() != planJoin.isMarkJoin()) {
                throw topologyDivergence(position, "the MARK-join form differs (bind mark="
                        + bindJoin.isMarkJoin() + ", plan mark=" + planJoin.isMarkJoin()
                        + "); the mark form preserves an unmatched left row with a mark"
                        + " slot the plain form drops", bindSql);
            }
            if (bindJoin.isMarkJoin()) {
                assertOrderedTexts(position, "the mark slot",
                        markSlotTexts(bindJoin), markSlotTexts(planJoin), bindSql);
            }
            assertEquivalentTexts(position, "the join's ON condition",
                    joinConditionTexts(bindJoin), joinConditionTexts(planJoin), bindSql);
            if (bindJoin.isMarkJoin()) {
                // the MARK_CONDITION is NOT one of the ON predicates: the bind join
                // evaluates it into a three-valued mark (a right row failing it stays
                // visible with mark FALSE/NULL), while the same predicate in ON
                // FILTERS that right row out. Compare the two predicate roles
                // separately so a manual plan cannot move one into the other.
                assertOrderedTexts(position, "the MARK_CONDITION",
                        expressionTexts(bindJoin.getMarkJoinConjuncts()),
                        expressionTexts(planJoin.getMarkJoinConjuncts()), bindSql);
            }
            return;
        }
        if (bind instanceof LogicalAggregate) {
            LogicalAggregate<?> bindAggregate = (LogicalAggregate<?>) bind;
            LogicalAggregate<?> planAggregate = (LogicalAggregate<?>) plan;
            assertEquivalentTexts(position, "the GROUP BY keys",
                    expressionTexts(bindAggregate.getGroupByExpressions()),
                    expressionTexts(planAggregate.getGroupByExpressions()), bindSql);
            // POSITION and NAME are semantic here: MIN(k) x vs MAX(k) y is another value
            // for the caller's x, and only an ALIGNABLE root list may rename
            assertOrderedTexts(position, "the aggregated output expressions",
                    namedTexts(outputItemsOf(bindAggregate), labelRelaxed),
                    namedTexts(outputItemsOf(planAggregate), labelRelaxed), bindSql);
            return;
        }
        if (bind instanceof LogicalRepeat) {
            LogicalRepeat<?> bindRepeat = (LogicalRepeat<?>) bind;
            LogicalRepeat<?> planRepeat = (LogicalRepeat<?>) plan;
            assertEquivalentTexts(position, "the GROUPING SETS",
                    groupingSetTexts(bindRepeat.getGroupingSets()),
                    groupingSetTexts(planRepeat.getGroupingSets()), bindSql);
            // the repeat's OUTPUT mapping (one column per grouping set position) must
            // match as well: it decides which column a parent reads
            assertOrderedTexts(position, "the grouping-set output expressions",
                    namedTexts(bindRepeat.getOutputExpressions(), labelRelaxed),
                    namedTexts(planRepeat.getOutputExpressions(), labelRelaxed), bindSql);
            return;
        }
        if (bind instanceof LogicalWindow) {
            LogicalWindow<?> bindWindow = (LogicalWindow<?>) bind;
            LogicalWindow<?> planWindow = (LogicalWindow<?>) plan;
            assertOrderedTexts(position, "the window expressions",
                    namedTexts(bindWindow.getWindowExpressions(), labelRelaxed),
                    namedTexts(planWindow.getWindowExpressions(), labelRelaxed), bindSql);
            return;
        }
        if (bind instanceof LogicalSetOperation) {
            // the class equality of compareRelationalNodes already pins UNION vs
            // INTERSECT / EXCEPT; DISTINCT vs ALL is the qualifier
            if (!((LogicalSetOperation) bind).getQualifier()
                    .equals(((LogicalSetOperation) plan).getQualifier())) {
                throw topologyDivergence(position, "the set-operation kind differs (bind "
                        + ((LogicalSetOperation) bind).getQualifier() + ", plan "
                        + ((LogicalSetOperation) plan).getQualifier() + "); a DISTINCT union"
                        + " collapses duplicate rows an ALL one keeps", bindSql);
            }
            return;
        }
        if (bind instanceof LogicalTopN) {
            LogicalTopN<?> bindTopN = (LogicalTopN<?>) bind;
            LogicalTopN<?> planTopN = (LogicalTopN<?>) plan;
            assertOrderedTexts(position, "the order keys",
                    sortContractOf(bindTopN.getOrderKeys()),
                    sortContractOf(planTopN.getOrderKeys()), bindSql);
            if (bindTopN.getLimit() != planTopN.getLimit()
                    || bindTopN.getOffset() != planTopN.getOffset()) {
                throw topologyDivergence(position, "the row cap differs (bind LIMIT "
                        + bindTopN.getLimit() + " OFFSET " + bindTopN.getOffset()
                        + ", plan LIMIT " + planTopN.getLimit() + " OFFSET "
                        + planTopN.getOffset() + ")", bindSql);
            }
            return;
        }
        if (bind instanceof LogicalSort) {
            assertOrderedTexts(position, "the order keys",
                    sortContractOf(((LogicalSort<?>) bind).getOrderKeys()),
                    sortContractOf(((LogicalSort<?>) plan).getOrderKeys()), bindSql);
            return;
        }
        if (bind instanceof LogicalLimit) {
            LogicalLimit<?> bindLimit = (LogicalLimit<?>) bind;
            LogicalLimit<?> planLimit = (LogicalLimit<?>) plan;
            if (bindLimit.getLimit() != planLimit.getLimit()
                    || bindLimit.getOffset() != planLimit.getOffset()) {
                throw topologyDivergence(position, "the row cap differs (bind LIMIT "
                        + bindLimit.getLimit() + " OFFSET " + bindLimit.getOffset()
                        + ", plan LIMIT " + planLimit.getLimit() + " OFFSET "
                        + planLimit.getOffset() + ")", bindSql);
            }
            return;
        }
        if (bind instanceof org.apache.doris.nereids.trees.plans.algebra.Filter) {
            // the SAME filter (type and conjuncts) attached to the SAME input: the
            // one-way text containment cannot see an added plan-side predicate
            // (SELECT k FROM t against ... WHERE k IS NOT NULL) nor a conjunct that
            // MOVED between two inputs (u.v IS NOT NULL after a LEFT JOIN vs inside its
            // right input)
            org.apache.doris.nereids.trees.plans.algebra.Filter bindFilter =
                    (org.apache.doris.nereids.trees.plans.algebra.Filter) bind;
            org.apache.doris.nereids.trees.plans.algebra.Filter planFilter =
                    (org.apache.doris.nereids.trees.plans.algebra.Filter) plan;
            assertEquivalentTexts(position, "the row-filter conjuncts",
                    flattenedConjunctTexts(bindFilter.getConjuncts()),
                    flattenedConjunctTexts(planFilter.getConjuncts()), bindSql);
            return;
        }
        if (bind instanceof LogicalProject) {
            LogicalProject<?> bindProject = (LogicalProject<?>) bind;
            LogicalProject<?> planProject = (LogicalProject<?>) plan;
            // SELECT DISTINCT parses onto the PROJECT's flag (not a separate operator), so
            // the two projections below expose the same k while t={1,1} yields one caller
            // row and two replay rows
            if (bindProject.isDistinct() != planProject.isDistinct()) {
                throw topologyDivergence(position, "the DISTINCT flag differs (bind "
                        + bindProject.isDistinct() + ", plan " + planProject.isDistinct()
                        + "); DISTINCT collapses duplicate rows a plain projection keeps",
                        bindSql);
            }
            // k AS x, v AS y vs k AS y, v AS x: the ordered EXPRESSIONS alone are equal
            // (k, v) while the names a parent resolves through are swapped - keep the
            // (name, expression) mapping everywhere except the alignable root list
            assertOrderedTexts(position, "the projected expressions",
                    namedTexts(outputItemsOf(bind), labelRelaxed),
                    namedTexts(outputItemsOf(plan), labelRelaxed), bindSql);
            return;
        }
        if (bind instanceof UnboundTVFRelation) {
            // a table function carries its PROPERTIES (numbers("number"="10")): the class
            // equality alone accepts another property map, and a matching 10-row caller
            // would replay the plan's 20-row generation
            UnboundTVFRelation bindTvf = (UnboundTVFRelation) bind;
            UnboundTVFRelation planTvf = (UnboundTVFRelation) plan;
            String bindFunction = tvfText(bindTvf);
            String planFunction = tvfText(planTvf);
            if (!bindFunction.equals(planFunction)) {
                throw topologyDivergence(position, "the table function differs (bind "
                        + bindFunction + ", plan " + planFunction + "); the plan would"
                        + " generate other rows", bindSql);
            }
            return;
        }
        if (bind instanceof UnboundRelation) {
            UnboundRelation bindRelation = (UnboundRelation) bind;
            UnboundRelation planRelation = (UnboundRelation) plan;
            if (!bindRelation.getNameParts().equals(planRelation.getNameParts())) {
                throw topologyDivergence(position, "the scanned table differs (bind "
                        + bindRelation.getNameParts() + ", plan "
                        + planRelation.getNameParts() + "); the plan would read another"
                        + " table's rows", bindSql);
            }
            return;
        }
        if (bind instanceof UnboundInlineTable) {
            // (VALUES ...) cells are NOT parameterized (the transformer leaves them
            // concrete), so a plan carrying other values replays its own: the row
            // structure (arity and order) and every cell must match exactly.
            assertOrderedTexts(position, "the VALUES rows",
                    inlineTableRowTexts((UnboundInlineTable) bind),
                    inlineTableRowTexts((UnboundInlineTable) plan), bindSql);
            return;
        }
        if (bind instanceof UnboundOneRowRelation) {
            // a one-row relation is a leaf with payload: a manual plan computing
            // another expression (pi() against e()) differs without any literal the
            // placeholder guard could compare, and the frozen replay would compute
            // the PLAN's expression
            assertOrderedTexts(position, "the one-row projections",
                    namedTexts(((UnboundOneRowRelation) bind).getProjects(), false),
                    namedTexts(((UnboundOneRowRelation) plan).getProjects(), false),
                    bindSql);
            return;
        }
        if (bind instanceof LogicalSubQueryAlias) {
            LogicalSubQueryAlias<?> bindAlias = (LogicalSubQueryAlias<?>) bind;
            LogicalSubQueryAlias<?> planAlias = (LogicalSubQueryAlias<?>) plan;
            if (!Objects.equals(bindAlias.getAlias(), planAlias.getAlias())) {
                throw topologyDivergence(position, "the alias differs (bind '"
                        + bindAlias.getAlias() + "', plan '" + planAlias.getAlias()
                        + "'); the caller's columns and predicates resolve through it",
                        bindSql);
            }
            if (!Objects.equals(bindAlias.getColumnAliases().orElse(null),
                    planAlias.getColumnAliases().orElse(null))) {
                throw topologyDivergence(position, "the column aliases differ (bind "
                        + bindAlias.getColumnAliases().orElse(null) + ", plan "
                        + planAlias.getColumnAliases().orElse(null) + ")", bindSql);
            }
            if (bindAlias.isRecursiveCte() != planAlias.isRecursiveCte()) {
                throw topologyDivergence(position, "the recursive-CTE flag differs",
                        bindSql);
            }
            return;
        }
        if (bind instanceof LogicalGenerate) {
            LogicalGenerate<?> bindGenerate = (LogicalGenerate<?>) bind;
            LogicalGenerate<?> planGenerate = (LogicalGenerate<?>) plan;
            assertOrderedTexts(position, "the generator functions",
                    expressionTexts(bindGenerate.getGenerators()),
                    expressionTexts(planGenerate.getGenerators()), bindSql);
            assertUnnestModes(position, bindGenerate.getGenerators(),
                    planGenerate.getGenerators(), bindSql);
            assertEquivalentTexts(position, "the post-generator filter",
                    flattenedConjunctTexts(bindGenerate.getConjuncts()),
                    flattenedConjunctTexts(planGenerate.getConjuncts()), bindSql);
            // LATERAL VIEW ... AS x vs AS y: the generated columns are new NAMES the
            // caller resolves through (star alignment cannot derive them), so both the
            // arity and the names must match. A return-many-column generator's output
            // slot carries an INTERNAL statement-scoped name while the AS names sit in
            // expandColumnAlias; a single-name generator's slot IS the alias.
            List<Slot> bindGenerated = bindGenerate.getGeneratorOutput();
            List<Slot> planGenerated = planGenerate.getGeneratorOutput();
            if (bindGenerated.size() != planGenerated.size()) {
                throw topologyDivergence(position, "the generator output arity differs (bind "
                        + bindGenerated.size() + " column(s), plan " + planGenerated.size()
                        + ")", bindSql);
            }
            assertOrderedTexts(position, "the generated column aliases",
                    expandColumnAliasTexts(bindGenerate.getExpandColumnAlias()),
                    expandColumnAliasTexts(planGenerate.getExpandColumnAlias()), bindSql);
            boolean aliasesCarryNames = bindGenerate.getExpandColumnAlias().stream()
                    .anyMatch(aliases -> !aliases.isEmpty())
                    || planGenerate.getExpandColumnAlias().stream()
                            .anyMatch(aliases -> !aliases.isEmpty());
            if (!aliasesCarryNames) {
                assertOrderedTexts(position, "the generated column names",
                        slotNames(bindGenerated), slotNames(planGenerated), bindSql);
            }
            return;
        }
        if (bind instanceof LogicalCTE) {
            if (((LogicalCTE<?>) bind).isRecursive()
                    != ((LogicalCTE<?>) plan).isRecursive()) {
                throw topologyDivergence(position, "the recursive-CTE flag differs",
                        bindSql);
            }
            return;
        }
        // Every remaining operator pair is pinned by its class and the lockstep input /
        // auxiliary / subquery recursion; parse-local payload (CTE ids, relation ids) is
        // deliberately not comparable across two parses.
    }

    /** The tree below any SELECT-hint wrappers (see compareRelationalNodes). */
    private static Plan unwrapSelectHints(Plan node) {
        Plan current = node;
        while (current instanceof LogicalSelectHint && current.children().size() == 1) {
            current = current.child(0);
        }
        return current;
    }

    /** The human-readable operator name of a node (topology error messages). */
    private static String describeRelationalNode(Plan node) {
        if (node instanceof LogicalJoin) {
            return "a join (" + ((LogicalJoin<?, ?>) node).getJoinType() + ")";
        }
        if (node instanceof LogicalUsingJoin) {
            return "a USING join (" + ((LogicalUsingJoin<?, ?>) node).getJoinType() + ")";
        }
        if (node instanceof LogicalAggregate) {
            return "an aggregation / DISTINCT";
        }
        if (node instanceof LogicalRepeat) {
            return "a GROUPING SETS operator";
        }
        if (node instanceof LogicalWindow) {
            return "a window operator";
        }
        if (node instanceof LogicalSetOperation) {
            return "a set operation (" + node.getClass().getSimpleName() + ")";
        }
        if (node instanceof LogicalTopN) {
            return "a TOP-N (ORDER BY with a row cap)";
        }
        if (node instanceof LogicalSort) {
            return "a sort";
        }
        if (node instanceof LogicalLimit) {
            return "a LIMIT (row cap)";
        }
        if (node instanceof org.apache.doris.nereids.trees.plans.algebra.Filter) {
            return "a row filter (" + node.getClass().getSimpleName() + ")";
        }
        if (node instanceof LogicalProject) {
            return "a projection";
        }
        if (node instanceof UnboundRelation) {
            return "a scan of " + ((UnboundRelation) node).getNameParts();
        }
        if (node instanceof LogicalSubQueryAlias) {
            return "a subquery alias ('" + ((LogicalSubQueryAlias<?>) node).getAlias() + "')";
        }
        if (node instanceof LogicalGenerate) {
            return "a generator (LATERAL VIEW / EXPLODE)";
        }
        return "a " + node.getClass().getSimpleName() + " operator";
    }

    /** The topology-divergence exception of one position (see
     * rejectRelationalTopologyDivergence). */
    private static org.apache.doris.nereids.exceptions.AnalysisException topologyDivergence(
            String position, String detail, String bindSql) {
        return new org.apache.doris.nereids.exceptions.AnalysisException(
                "SPM cannot align the plan SQL with the bind SQL: " + position
                        + " " + detail + ". A caller matching the bind text is answered with"
                        + " the frozen plan's rows, so the two statements must describe the"
                        + " SAME computation - write both with the same relations (same names"
                        + " and aliases), joins, operators and filter placement: " + bindSql);
    }

    /** Ordered-list equality of two expression-text lists (same entries, same order). */
    private static void assertOrderedTexts(String position, String what, List<String> bind,
            List<String> plan, String bindSql) {
        if (bind.equals(plan)) {
            return;
        }
        throw topologyDivergence(position, what + " differ (bind " + bind + ", plan " + plan
                + ")", bindSql);
    }

    /** Multiset equality of two expression-text lists (same entries, any order). */
    private static void assertEquivalentTexts(String position, String what, List<String> bind,
            List<String> plan, String bindSql) {
        if (sameSelectionIgnoreOrder(bind, plan)) {
            return;
        }
        throw topologyDivergence(position, what + " differ (bind " + bind + ", plan " + plan
                + ")", bindSql);
    }

    /** The boundary-preserving toSql texts of one expression list (see
     * boundaryPreservingDeepText): slots render with their component encoding, so a
     * dotted column never compares like a qualified reference in any of the lockstep
     * checks (join conditions, GROUP BY keys, USING slots, generators...). */
    private static List<String> expressionTexts(
            Collection<? extends Expression> expressions) {
        List<String> texts = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            texts.add(boundaryPreservingDeepText(expression));
        }
        return texts;
    }

    /** The label-ignoring texts of one output-item list (see outputExpressionText). */
    private static List<String> labelStrippedTexts(List<NamedExpression> items) {
        List<String> texts = new ArrayList<>(items.size());
        for (NamedExpression item : items) {
            texts.add(outputExpressionText(item));
        }
        return texts;
    }

    /** One output list's "nameexpression" pairs; only with labelRelaxed the
     * label part is dropped (see outputExpressionText). */
    private static List<String> namedTexts(List<NamedExpression> items, boolean labelRelaxed) {
        if (labelRelaxed) {
            return labelStrippedTexts(items);
        }
        List<String> texts = new ArrayList<>(items.size());
        for (NamedExpression item : items) {
            String label = outputLabelOf(item);
            texts.add((label == null ? "" : label) + '\u0001' + outputExpressionText(item));
        }
        return texts;
    }

    /** The mark-slot names of a MARK join, in order (see compareRelationalPayload).
     * The unbound tree has no output list yet, so the name comes from the parse-time
     * mark reference itself. */
    private static List<String> markSlotTexts(LogicalJoin<?, ?> join) {
        List<String> texts = new ArrayList<>();
        join.getMarkJoinSlotReference().ifPresent(reference -> texts.add(reference.toSql()));
        return texts;
    }

    /** One TVF relation's function name plus its (sorted) property map as one text. */
    private static String tvfText(UnboundTVFRelation relation) {
        return relation.getFunctionName() + '|'
                + new java.util.TreeMap<>(relation.getProperties().getMap());
    }

    /** The names of one slot list, in order. */
    private static List<String> slotNames(List<? extends Slot> slots) {
        List<String> names = new ArrayList<>(slots.size());
        for (Slot slot : slots) {
            names.add(slot.getName());
        }
        return names;
    }

    /** One text per generator output's AS-name list, in order (see the GENERATE branch). */
    private static List<String> expandColumnAliasTexts(List<List<String>> aliases) {
        List<String> texts = new ArrayList<>(aliases.size());
        for (List<String> alias : aliases) {
            texts.add(String.join(",", alias));
        }
        return texts;
    }

    /** The conjunct texts of one filter's predicate set, AND nodes flattened into leaves
     * (see filterConjunctTexts). */
    private static List<String> flattenedConjunctTexts(Collection<Expression> predicates) {
        List<String> texts = new ArrayList<>();
        for (Expression predicate : predicates) {
            collectConjunctTextsInto(predicate, texts);
        }
        return texts;
    }

    /** One predicate's leaf texts into a list: an AND is split into its children. */
    private static void collectConjunctTextsInto(Expression predicate, List<String> texts) {
        if (predicate instanceof And) {
            for (Expression child : ((And) predicate).children()) {
                collectConjunctTextsInto(child, texts);
            }
            return;
        }
        texts.add(boundaryPreservingDeepText(predicate));
    }

    /** The ORDINARY (ON) predicate texts of one join: the hash / other conjuncts and
     * an explicit ON clause. The MARK conjuncts are compared separately, by ROLE:
     * moving one into ON turns a three-valued mark into a row filter. */
    private static List<String> joinConditionTexts(LogicalJoin<?, ?> join) {
        List<String> texts = expressionTexts(join.getHashJoinConjuncts());
        texts.addAll(expressionTexts(join.getOtherJoinConjuncts()));
        Optional<Expression> onClause = join.getOnClauseCondition();
        if (onClause.isPresent()) {
            texts.add(boundaryPreservingDeepText(onClause.get()));
        }
        return texts;
    }

    /** The one-row texts of a VALUES relation: row arity and cell order preserved. */
    private static List<String> inlineTableRowTexts(UnboundInlineTable table) {
        List<String> rows = new ArrayList<>();
        for (List<NamedExpression> row : table.getConstantExprsList()) {
            List<String> cells = new ArrayList<>(row.size());
            for (NamedExpression cell : row) {
                cells.add(outputExpressionText(cell));
            }
            rows.add(cells.toString());
        }
        return rows;
    }

    /**
     * The CONCRETE payloads of the relation forms whose contents the audit digest
     * masks while SPM compares them STRUCTURALLY: the rows of an inline VALUES table
     * (row boundaries and per-cell order are part of the identity - (1,2),(3,4) and
     * (1),(2),(3),(4) render to the same digest) and a table-valued function's
     * property map. Used by the capture dedup to refine two same-digest rows into
     * separate candidates (see AuditLogScanner#dedupIdentity).
     *
     * @param plan the parsed statement
     * @return the fingerprint ("" when the tree carries no such relation; also on an
     *         unexpected walk failure - a missing fingerprint only loses refinement)
     */
    public static String concreteRelationPayloadFingerprint(LogicalPlan plan) {
        final StringBuilder fingerprint = new StringBuilder();
        try {
            SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
                if (node instanceof UnboundInlineTable) {
                    fingerprint.append("values:")
                            .append(inlineTableRowTexts((UnboundInlineTable) node))
                            .append('\u0001');
                } else if (node instanceof UnboundTVFRelation) {
                    UnboundTVFRelation tvf = (UnboundTVFRelation) node;
                    fingerprint.append("tvf:").append(tvf.getFunctionName())
                            .append('(')
                            .append(tvf.getProperties() == null ? "" : tvf.getProperties())
                            .append(')').append('\u0001');
                }
            });
        } catch (RuntimeException e) {
            return "";
        }
        return fingerprint.toString();
    }

    /** The UNNEST mode flags of the parallel generator lists (see the GENERATE
     * branch): a LEFT JOIN UNNEST keeps an unmatched row with NULL while the inner
     * form drops it, and both share the class and the rendered text. */
    private static void assertUnnestModes(String position, List<Function> bind,
            List<Function> plan, String bindSql) {
        for (int i = 0; i < bind.size(); i++) {
            if (bind.get(i) instanceof Unnest && plan.get(i) instanceof Unnest) {
                Unnest bindUnnest = (Unnest) bind.get(i);
                Unnest planUnnest = (Unnest) plan.get(i);
                if (bindUnnest.isOuter() != planUnnest.isOuter()
                        || bindUnnest.needOrdinality() != planUnnest.needOrdinality()) {
                    throw topologyDivergence(position, "the UNNEST mode differs (bind"
                            + " outer=" + bindUnnest.isOuter() + ", ordinality="
                            + bindUnnest.needOrdinality() + "; plan outer="
                            + planUnnest.isOuter() + ", ordinality="
                            + planUnnest.needOrdinality() + "); an OUTER unnest keeps"
                            + " an unmatched row with NULL where the inner form drops"
                            + " it", bindSql);
                }
            }
        }
    }

    /** The GROUPING SETS rendering of one repeat operator. */
    private static List<String> groupingSetTexts(List<List<Expression>> groupingSets) {
        List<String> texts = new ArrayList<>(groupingSets.size());
        for (List<Expression> groupingSet : groupingSets) {
            texts.add(expressionTexts(groupingSet).toString());
        }
        return texts;
    }

    /** The direction / null-placement / expression contract of one order-key list. */
    private static List<String> sortContractOf(List<OrderKey> keys) {
        List<String> contract = new ArrayList<>(keys.size());
        for (OrderKey key : keys) {
            contract.add((key.isAsc() ? "ASC" : "DESC") + "/"
                    + (key.isNullFirst() ? "NULLS_FIRST" : "NULLS_LAST") + ":"
                    + boundaryPreservingDeepText(key.getExpr()));
        }
        return contract;
    }

    /** The subquery plans one node's expressions carry, in a stable order. */
    private static List<Plan> expressionSubqueryPlans(Plan node) {
        List<Plan> subqueries = new ArrayList<>();
        for (Expression expression : node.getExpressions()) {
            collectExpressionSubqueryPlans(expression, subqueries);
        }
        for (Expression expression : outOfBandExpressions(node)) {
            collectExpressionSubqueryPlans(expression, subqueries);
        }
        return subqueries;
    }

    /** One expression's SubqueryExpr query plans, depth-first. */
    private static void collectExpressionSubqueryPlans(Expression expression,
            List<Plan> subqueries) {
        if (expression instanceof SubqueryExpr) {
            subqueries.add(((SubqueryExpr) expression).getQueryPlan());
        }
        for (Expression child : expression.children()) {
            collectExpressionSubqueryPlans(child, subqueries);
        }
    }

    /**
     * The caller-visible output list of one query tree: descend through single-child
     * wrappers (sink / sort / limit / subquery alias) to the node that carries the list
     * (see carriesOutputList); null when the tree exposes no comparable list
     * (e.g. a set operation root).
     */
    private static List<NamedExpression> topLevelOutputItems(Plan root) {
        Plan node = root;
        while (node != null && !carriesOutputList(node) && node.children().size() == 1) {
            node = node.child(0);
        }
        return node != null && carriesOutputList(node) ? outputItemsOf(node) : null;
    }

    /**
     * One output item's canonical text IGNORING its label (see
     * rejectManualPlanDivergence).
     */
    private static String outputExpressionText(NamedExpression item) {
        Expression expression = item;
        if (item instanceof Alias) {
            expression = ((Alias) item).child();
        } else if (item instanceof UnboundAlias) {
            expression = ((UnboundAlias) item).child();
        }
        return boundaryPreservingText(expression);
    }

    /**
     * One expression's text for the create-time comparison: the boundary-preserving DEEP
     * encoding, not just the bare-slot special case. A COMPUTED output expression nests
     * its slots (`a.b` + 1 in the SELECT list, an arithmetic ON fragment), and the plain
     * toSql fallback rendered a column literally named `a.b` (ONE quoted component) and
     * the qualified reference a.b (two components) identically - the output / projection
     * checks accepted the pair and the replay then read the WRONG column for callers
     * selecting the dotted name.
     */
    private static String boundaryPreservingText(Expression expression) {
        return boundaryPreservingDeepText(expression);
    }

    /**
     * One parsed predicate's / join condition's comparison text: the rendered SQL PLUS
     * the component encoding of every slot inside it. UnboundSlot#toSql joins the name
     * components with '.', so a column literally named "a.b" (ONE component) and the
     * qualified reference a.b (two components) render identically while filtering
     * DIFFERENT columns - a manual plan reading alias a's column b passed the WHERE /
     * JOIN ON containment and lockstep checks as if it carried the bind's filter on the
     * dotted column. The appended encodings restore the component boundaries that the
     * rendered text loses; every other part of the expression (operators, function
     * names, literal VALUES) stays in the rendered text.
     */
    private static String boundaryPreservingDeepText(Expression expression) {
        StringBuilder text = new StringBuilder(expression.toSql());
        List<String> slots = new ArrayList<>();
        collectSlotEncodings(expression, slots);
        text.append('\u0002');
        for (String slot : slots) {
            text.append(slot).append('\u0003');
        }
        return text.toString();
    }

    /** The component encodings of every slot inside one expression, depth-first (see
     * boundaryPreservingDeepText). */
    private static void collectSlotEncodings(Expression expression, List<String> slots) {
        if (expression instanceof UnboundSlot) {
            slots.add(slotComponentEncoding((UnboundSlot) expression));
        }
        if (expression instanceof UnboundStar) {
            // The REPLACE payload and the EXCEPT list live OUTSIDE children() (see
            // SPMAstCheckVisitor), so the child descent below never records their slots:
            // the bare excepted slot renders through UnboundSlot#toSql, where
            // `a.b` (ONE dotted component) and the qualified reference a.b (two
            // components) coincide - a manual plan could exclude / replace the OTHER
            // column while the output texts matched and hand its column to callers.
            UnboundStar star = (UnboundStar) expression;
            for (Expression excepted : star.getExceptedSlots()) {
                collectSlotEncodings(excepted, slots);
            }
            for (Expression replaced : star.getReplacedAlias()) {
                collectSlotEncodings(replaced, slots);
            }
        }
        for (Expression child : expression.children()) {
            collectSlotEncodings(child, slots);
        }
    }

    /** One slot's name components as an unambiguous (length-prefixed) text: ["a.b"]
     * and ["a", "b"] must never encode alike. */
    private static String slotComponentEncoding(UnboundSlot slot) {
        StringBuilder encoded = new StringBuilder();
        for (String part : slot.getNameParts()) {
            encoded.append(part.length()).append(':').append(part);
        }
        return encoded.toString();
    }

    /**
     * Every row-filter conjunct text of one tree (each LogicalFilter or
     * LogicalHaving anywhere in the statement, subqueries included). A
     * conjunct that IS an AND is FLATTENED into its leaves: the unbound parse of
     * WHERE k1 = 1 AND k2 IS NOT NULL keeps ONE And node in the conjunct list
     * (rendered as the whole AND[(k1 = 1),(not k2 IS NULL)] compound), so a
     * containment test against the bind's single (k1 = 1) conjunct would
     * reject a plan that genuinely carries it.
     */
    private static Set<String> filterConjunctTexts(Plan plan) {
        Set<String> conjuncts = new java.util.LinkedHashSet<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            Collection<Expression> predicates;
            if (node instanceof LogicalFilter) {
                predicates = ((LogicalFilter<?>) node).getConjuncts();
            } else if (node instanceof LogicalHaving) {
                predicates = ((LogicalHaving<?>) node).getConjuncts();
            } else {
                return;
            }
            for (Expression predicate : predicates) {
                collectConjunctTexts(predicate, conjuncts);
            }
        });
        return conjuncts;
    }

    /** One predicate's leaf texts: an AND is split into its children (see
     * filterConjunctTexts). */
    private static void collectConjunctTexts(Expression predicate, Set<String> conjuncts) {
        if (predicate instanceof And) {
            for (Expression child : ((And) predicate).children()) {
                collectConjunctTexts(child, conjuncts);
            }
            return;
        }
        conjuncts.add(boundaryPreservingDeepText(predicate));
    }

    /**
     * The caller-visible top-level ORDER BY contract of one tree: one entry per order
     * key (direction/nulls:expression-text), empty when the tree exposes no
     * top-level sort. The descent mirrors topLevelLimitOf: it follows the
     * wrapper chain and STOPS at a projection, so a sort below a projection is not the
     * caller-visible contract.
     */
    private static List<String> rootOrderContract(Plan plan) {
        Plan node = plan;
        while (node != null && !(node instanceof LogicalSort) && !(node instanceof LogicalTopN)
                && !(node instanceof LogicalProject) && node.children().size() == 1) {
            node = node.child(0);
        }
        List<OrderKey> keys = null;
        if (node instanceof LogicalSort) {
            keys = ((LogicalSort<?>) node).getOrderKeys();
        } else if (node instanceof LogicalTopN) {
            keys = ((LogicalTopN<?>) node).getOrderKeys();
        }
        if (keys == null) {
            return List.of();
        }
        return sortContractOf(keys);
    }

    /**
     * Whether the REPLAYED tree still exposes the caller's own top-level ORDER BY
     * contract. rowLimitsWithin / rowLimitsSurviveReplay
     * only compare ROW CAPS: a caller with an UNCAPPED sort - ORDER BY k ASC and
     * no LIMIT - matched a bind that also had none, so the limit contract passed without
     * ever comparing the sort and a manual / frozen plan ordering by DESC returned
     * (2,1) for the caller that asked for (1,2).
     *
     * A caller WITHOUT a top-level sort passes: an unordered result has no visible
     * ordering contract to preserve (a TopN the replay carries for a caller LIMIT is
     * governed by the row-cap checks, not by this one).
     *
     * The caller's shape must be a PREFIX of the replay's: the frozen (decompiled)
     * optimal plan appends deterministic TIE-BREAKER keys after the caller's own - the
     * tpch q18 frozen TopN sorts by the caller's o_totalprice DESC,
     * o_orderdate ASC PLUS c_name / c_custkey / o_orderkey - and those extra
     * keys only refine ties the caller's ORDER BY left unspecified, while a caller key
     * DROPPED from or FLIPPED in the replay breaks the visible ordering.
     *
     * @param replayed the replayed tree (frozen text or parameterized fallback)
     * @param userPlan the caller's own tree
     * @return whether the caller's visible ordering survives the replay
     */
    public static boolean orderContractPreserved(Plan replayed, Plan userPlan) {
        List<String> caller = rootSortShape(userPlan);
        if (caller.isEmpty()) {
            return true;
        }
        List<String> replay = rootSortShape(replayed);
        return replay.size() >= caller.size() && replay.subList(0, caller.size()).equals(caller);
    }

    /**
     * The ORDER BY contract entries WITHOUT the order-key expression: the replay
     * comparison must not reject a legitimate rewrite because the order key is spelled
     * differently across the user text and the regenerated frozen text (ORDER BY
     * 1 / ORDER BY v in the caller vs ORDER BY c_1 in the frozen
     * projection - lesson: order-key expressions are not comparable
     * across the two trees; the L3 match plus the CREATE-time full contract pin the
     * expressions themselves).
     */
    private static List<String> rootSortShape(Plan plan) {
        List<String> entries = new ArrayList<>();
        for (String entry : rootOrderContract(plan)) {
            int colon = entry.indexOf(':');
            entries.add(colon < 0 ? entry : entry.substring(0, colon));
        }
        return entries;
    }

    /** For tests: the top-level ORDER BY contract entries of one tree. */
    @VisibleForTesting
    public static List<String> rootOrderContractForTest(Plan plan) {
        return rootOrderContract(plan);
    }

    /**
     * One base-table occurrence's scan selector together with the relation's alias.
     */
    private static final class ScanSelectorOccurrence {
        final String alias;
        final String selector;

        ScanSelectorOccurrence(String alias, String selector) {
            this.alias = alias == null ? "" : alias;
            this.selector = selector;
        }

        @Override
        public String toString() {
            return alias.isEmpty() ? selector : alias + ':' + selector;
        }
    }

    /**
     * The scan-selector description of every base-table relation together with the ALIAS
     * of the occurrence it belongs to, grouped by the relation's LAST name part (the table
     * name) and kept in STATEMENT (walk) ORDER: the bind and plan texts may qualify their
     * tables differently, while a self join contributes one entry per occurrence - the
     * caller compares the lists of one table per occurrence, so a pin stays attached to
     * the occurrence it belongs to (see scanSelectorsAligned).
     */
    private static Map<String, List<ScanSelectorOccurrence>> scanSelectorsByTable(Plan plan) {
        Map<String, List<ScanSelectorOccurrence>> byTable = new HashMap<>();
        collectScanSelectors(plan, "", byTable,
                Collections.newSetFromMap(new IdentityHashMap<>()));
        return byTable;
    }

    /**
     * The walk behind scanSelectorsByTable: mirrors walkPlans (children,
     * extraPlans and expression subqueries, each node ONCE) while tracking the alias of
     * the closest enclosing LogicalSubQueryAlias - a plain FROM t a
     * parses as an alias node wrapping the unbound relation.
     */
    private static void collectScanSelectors(Plan node, String alias,
            Map<String, List<ScanSelectorOccurrence>> byTable, Set<Plan> visited) {
        if (node == null || !visited.add(node)) {
            return;
        }
        String scopeAlias = alias;
        if (node instanceof LogicalSubQueryAlias) {
            String own = ((LogicalSubQueryAlias<?>) node).getAlias();
            if (own != null && !own.isEmpty()) {
                scopeAlias = own;
            }
        }
        if (node instanceof UnboundRelation) {
            UnboundRelation relation = (UnboundRelation) node;
            List<String> nameParts = relation.getNameParts();
            String table = nameParts == null || nameParts.isEmpty()
                    ? "" : nameParts.get(nameParts.size() - 1);
            byTable.computeIfAbsent(table, k -> new ArrayList<>())
                    .add(new ScanSelectorOccurrence(scopeAlias, describeScanSelector(relation)));
        }
        for (Plan child : node.children()) {
            collectScanSelectors(child, scopeAlias, byTable, visited);
        }
        for (Plan extra : node.extraPlans()) {
            collectScanSelectors(extra, scopeAlias, byTable, visited);
        }
        for (Expression expression : node.getExpressions()) {
            collectScanSelectorsFromExpression(expression, scopeAlias, byTable, visited);
        }
    }

    /** Recurses one expression tree looking for subquery plans (mirrors walkPlans). */
    private static void collectScanSelectorsFromExpression(Expression expression, String alias,
            Map<String, List<ScanSelectorOccurrence>> byTable, Set<Plan> visited) {
        if (expression instanceof SubqueryExpr) {
            collectScanSelectors(((SubqueryExpr) expression).getQueryPlan(), alias, byTable,
                    visited);
        }
        if (expression instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                collectScanSelectorsFromExpression(replaced, alias, byTable, visited);
            }
        }
        for (Expression child : expression.children()) {
            collectScanSelectorsFromExpression(child, alias, byTable, visited);
        }
    }

    // ==================== view guard ====================

    /**
     * Whether a plan references a VIEW anywhere in the statement.
     *
     * SPM freezes / replays plans over the BASE tables a view expands to (InlineLogicalView
     * replaces the LogicalView wrapper during analysis), and the replay is planned BEFORE
     * the normal authorization pass: authorizing the expanded plan checks the base tables
     * instead of the view, so a view-only user is denied on the base tables while a
     * base-table user passes the same view query without ever being checked against the
     * view. A plan that references a view must therefore never be replayed, and a frozen
     * plan must never be produced from it: the parameterized bind / plan trees keep the
     * view reference text, so the rewrite replays them through normal analysis and
     * authorization. Unresolvable relations are reported as not-a-view here; the normal
     * analysis pass surfaces the resolution error.
     *
     * The WHOLE statement is inspected, not just children(): a view behind a CTE body or
     * an IN / EXISTS / scalar subquery would otherwise be missed, because those plans are
     * held by getAliasQueries() / extraPlans() / SubqueryExpr.queryPlan instead of
     * children() (see walkPlans).
     */
    public static boolean referencesView(ConnectContext ctx, Plan plan) {
        if (plan == null || ctx == null || ctx.getStatementContext() == null) {
            return false;
        }
        final boolean[] viewReferenced = {false};
        walkPlansScoped(plan, Collections.emptySet(), (Plan node, Set<String> visibleCtes) -> {
            if (viewReferenced[0]) {
                return;
            }
            if (node instanceof LogicalView) {
                viewReferenced[0] = true;
            } else if (node instanceof UnboundRelation) {
                UnboundRelation relation = (UnboundRelation) node;
                // A single-part name that the WITH clause visible AT THIS POINT binds is a
                // CTE reference, not a catalog relation: resolving it against the catalog
                // ("WITH c AS (SELECT k FROM t) SELECT k FROM c" while a catalog view c
                // exists) reported a view and made every matching query exit at
                // viewReferenced - the baseline never applied.
                if (!isCteReference(relation, visibleCtes) && isViewRelation(ctx, relation)) {
                    viewReferenced[0] = true;
                }
            }
        });
        return viewReferenced[0];
    }

    /**
     * Visits every plan node reachable from a statement: the regular children, the plans
     * a node holds OUTSIDE children() - CTE bodies (LogicalCTE.getAliasQueries() through
     * extraPlans()), IN / EXISTS / scalar subquery plans (SubqueryExpr.queryPlan, surfaced
     * by LogicalFilter.extraPlans() and by the node's own expressions) - and the plans
     * reachable through expression coercions. An inspection that walks only children()
     * would not see a view (referencesView) or a SET_VAR hint
     * (SPMOptimizer#checkProtectedSetVarHints) that lives in a CTE body / subquery.
     *
     * @param root    the plan to start from (may be null)
     * @param visitor called once per reachable node; may throw
     * @param <E>     the visitor's exception type, propagated to the caller
     */
    public static <E extends Exception> void walkPlans(Plan root, PlanWalker<E> visitor) throws E {
        walkPlans(root, visitor, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    /**
     * walkPlans with the identity set of already-visited nodes: a node reachable
     * through TWO paths (a LogicalFilter exposes its predicate's subquery plans both
     * through extraPlans() and through the predicate expression itself) is visited ONCE.
     * Without the set the nested-subquery depth n was walked 2^n times (a 20-level
     * k = (SELECT ... WHERE k = (SELECT ...)) exceeds a million visits, and the
     * bind-side fingerprint resolved every repeated table again after the match
     * deadline). Identity - not equals() - is the right key: the walkers only INSPECT the
     * tree, and a shared plan object always appears under the same CTE scope.
     */
    private static <E extends Exception> void walkPlans(Plan root, PlanWalker<E> visitor,
            Set<Plan> visited) throws E {
        if (root == null || !visited.add(root)) {
            return;
        }
        visitor.visit(root);
        for (Plan child : root.children()) {
            walkPlans(child, visitor, visited);
        }
        for (Plan extra : root.extraPlans()) {
            walkPlans(extra, visitor, visited);
        }
        for (Expression expression : root.getExpressions()) {
            walkSubqueryPlans(expression, visitor, visited);
        }
    }

    /** Recurses one expression tree looking for subquery plans (coercions included). */
    private static <E extends Exception> void walkSubqueryPlans(Expression expression,
            PlanWalker<E> visitor, Set<Plan> visited) throws E {
        if (expression instanceof SubqueryExpr) {
            walkPlans(((SubqueryExpr) expression).getQueryPlan(), visitor, visited);
        }
        if (expression instanceof UnboundStar) {
            // SELECT * REPLACE((SELECT ... FROM v) AS k): the replacement payloads live in
            // getReplacedAlias(), OUTSIDE children(), so a subquery plan behind one stayed
            // invisible to every walkPlans-based inspection (the view guard froze the
            // expanded base-table plan, replay then checked base-table privileges instead
            // of the original view).
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                walkSubqueryPlans(replaced, visitor, visited);
            }
        }
        for (Expression child : expression.children()) {
            walkSubqueryPlans(child, visitor, visited);
        }
    }

    /** Plan visitor for walkPlans; the exception type is chosen by the caller. */
    public interface PlanWalker<E extends Exception> {
        void visit(Plan plan) throws E;
    }

    /** Plan visitor for walkPlansScoped: receives the CTE aliases visible at the node. */
    private interface ScopedPlanWalker<E extends Exception> {
        void visit(Plan plan, Set<String> visibleCtes) throws E;
    }

    /**
     * Like walkPlans, but tracks the CTE aliases visible at every visited node
     * (mirroring AnalyzeCTE's scoping rules): a CTE's main query sees every alias of that
     * WITH, alias body i only the aliases defined before it (plus itself under a real
     * recursive CTE), nested WITH nodes extend the enclosing scope and expression
     * subqueries inherit the scope of the point they appear in. Callers use the scope to
     * tell a WITH reference apart from a same-named CATALOG relation (the view guard and
     * the bind-side table fingerprint).
     *
     * @param root        the plan to start from (may be null)
     * @param visibleCtes the CTE aliases visible at root (normalized)
     * @param visitor     called once per reachable node; may throw
     * @param <E>         the visitor's exception type, propagated to the caller
     */
    private static <E extends Exception> void walkPlansScoped(Plan root, Set<String> visibleCtes,
            ScopedPlanWalker<E> visitor) throws E {
        walkPlansScoped(root, visibleCtes, visitor,
                Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    /**
     * walkPlansScoped with the identity set of already-visited nodes (see
     * walkPlans(Plan, PlanWalker, Set)): the same subquery plan is reachable
     * through a filter's extraPlans() AND through its predicate expression, so without
     * the set a nested-subquery chain was walked exponentially often.
     */
    private static <E extends Exception> void walkPlansScoped(Plan root, Set<String> visibleCtes,
            ScopedPlanWalker<E> visitor, Set<Plan> visited) throws E {
        if (root == null || !visited.add(root)) {
            return;
        }
        visitor.visit(root, visibleCtes);
        if (root instanceof LogicalCTE) {
            LogicalCTE<? extends Plan> cte = (LogicalCTE<? extends Plan>) root;
            Set<String> extended = new LinkedHashSet<>(visibleCtes);
            for (LogicalSubQueryAlias<Plan> alias : cte.getAliasQueries()) {
                extended.add(normalizeCteName(alias.getAlias()));
            }
            Set<String> mainScope = Collections.unmodifiableSet(extended);
            walkPlansScoped(cte.child(0), mainScope, visitor, visited);
            List<LogicalSubQueryAlias<Plan>> aliases = cte.getAliasQueries();
            for (int i = 0; i < aliases.size(); i++) {
                Set<String> bodyScope = new LinkedHashSet<>(visibleCtes);
                for (int j = 0; j < i; j++) {
                    bodyScope.add(normalizeCteName(aliases.get(j).getAlias()));
                }
                // a self reference binds to the WITH clause only in a real recursive CTE;
                // under a plain WITH it is an ordinary base-table reference
                if (cte.isRecursive() && aliases.get(i).isRecursiveCte()) {
                    bodyScope.add(normalizeCteName(aliases.get(i).getAlias()));
                }
                walkPlansScoped(aliases.get(i), Collections.unmodifiableSet(bodyScope), visitor,
                        visited);
            }
            return;
        }
        for (Plan child : root.children()) {
            walkPlansScoped(child, visibleCtes, visitor, visited);
        }
        for (Plan extra : root.extraPlans()) {
            walkPlansScoped(extra, visibleCtes, visitor, visited);
        }
        for (Expression expression : root.getExpressions()) {
            walkSubqueryPlansScoped(expression, visibleCtes, visitor, visited);
        }
    }

    /** Scoped counterpart of walkSubqueryPlans (see walkPlansScoped). */
    private static <E extends Exception> void walkSubqueryPlansScoped(Expression expression,
            Set<String> visibleCtes, ScopedPlanWalker<E> visitor, Set<Plan> visited) throws E {
        if (expression instanceof SubqueryExpr) {
            walkPlansScoped(((SubqueryExpr) expression).getQueryPlan(), visibleCtes, visitor,
                    visited);
        }
        if (expression instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                walkSubqueryPlansScoped(replaced, visibleCtes, visitor, visited);
            }
        }
        for (Expression child : expression.children()) {
            walkSubqueryPlansScoped(child, visibleCtes, visitor, visited);
        }
    }

    /** Whether a one-part relation name is bound by a CTE alias visible at this point. */
    private static boolean isCteReference(UnboundRelation relation, Set<String> visibleCtes) {
        if (visibleCtes.isEmpty()) {
            return false;
        }
        List<String> parts = relation.getNameParts();
        return parts != null && parts.size() == 1
                && visibleCtes.contains(normalizeCteName(parts.get(0)));
    }

    /**
     * Whether the statement writes to a file destination (SELECT ... INTO OUTFILE). The
     * destination (path / format / properties) and the sink's metadata live OUTSIDE the
     * expression list, so the generic SPM comparison would accept a user export targeting
     * a different destination - and the rewritten tree keeps the CAPTURED sink fields
     * (LogicalSink.withChildren preserves them), so the replay would write to the
     * baseline's destination. SPM rejects such statements at freeze and never rewrites
     * them.
     */
    public static boolean containsFileSink(Plan plan) {
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof LogicalFileSink) {
                found[0] = true;
            }
        });
        return found[0];
    }

    private static boolean isViewRelation(ConnectContext ctx, UnboundRelation relation) {
        try {
            // Resolve WITHOUT the planner's per-statement resolved-table cache: this guard
            // runs BEFORE collectAndLockTable, and caching the TableIf here would hand the
            // later bind / lock pass a detached PRE-LOCK object - a concurrent DROP /
            // CREATE t committing in between would be invisible (CollectRelation and
            // BindExpression reuse the cached instance, and the replay's post-plan
            // fingerprint compares that same old object with the stored identity).
            TableIf table = ctx.getStatementContext().resolveTableWithoutCache(
                    RelationUtil.getQualifierName(ctx, relation.getNameParts()),
                    Optional.of(relation));
            return table instanceof View;
        } catch (RuntimeException e) {
            // unresolvable / plugin table without metadata here: not treated as a view,
            // the normal analysis pass reports the error
            return false;
        }
    }

    // ==================== LIMIT / OFFSET canonical digest ====================

    /**
     * Canonicalizes an SPM digest for the Level 1/2 matching key: the TOP-LEVEL LIMIT /
     * OFFSET values are adopted from the user query at rewrite time
     * (SPMPlanTreeSupport#mergeLimits) and must therefore not be part of the key - but
     * LogicalLimit.toDigest() renders " OFFSET ?" only for a NON-ZERO offset, so a
     * baseline captured as "LIMIT 10" and a matching "LIMIT 20 OFFSET 5" query produced
     * different digests, the candidate list came back empty and the OFFSET query could
     * never reach the structural match that adopts its values. Dropping every
     * " OFFSET ?" suffix makes the key offset-independent exactly like it already is
     * limit-independent.
     *
     * A LIMIT / OFFSET inside a subquery plan is NOT reachable by the positional merge:
     * those values stay exact-compared at Level 3 (checkPlan insideSubquery), so removing
     * them from the digest can only skip/accept a candidate, never replay a wrong slice.
     *
     * @param digest the raw toSpmDigest() text
     * @return the offset-independent matching key
     */
    public static String canonicalSpmDigest(String digest) {
        return digest == null ? null : digest.replace(" OFFSET ?", "");
    }

    // ==================== schema identity (frozen-baseline binding) ====================

    /**
     * Fingerprint of the base tables referenced by a (still unbound) statement: one
     * deterministic entry per resolvable catalog relation,
     * tableName|tableId|schemaHash, sorted and joined with ';'. CREATE persists
     * it with the baseline and the rewrite validates it BEFORE replaying a frozen plan:
     * the bind key is built from the unbound query, so SELECT * FROM t WHERE k=1
     * keeps the same digest and Level-3 tree after ALTER TABLE t ADD COLUMN extra
     * (or after a DROP + CREATE), while the frozen result sink still emits the
     * creator-time output columns - the matched replay would silently return the old
     * column set instead of the current star expansion. The table id also catches
     * drop / recreate (a new id), and the schema hash catches column add / drop / type
     * changes in place.
     *
     * Resolution mirrors isViewRelation: unresolvable relations are skipped
     * (the normal analysis pass reports the error). A statement without a resolvable
     * relation (SELECT 1, TVF-only, ...) yields an EMPTY fingerprint, which disables
     * the check - there is no schema identity to bind to.
     *
     * @param ctx  the session context (null yields an empty fingerprint)
     * @param plan the unbound plan (null yields an empty fingerprint)
     * @return the fingerprint (possibly empty, never null)
     */
    public static String schemaFingerprint(ConnectContext ctx, Plan plan) {
        if (plan == null || ctx == null || ctx.getStatementContext() == null) {
            return "";
        }
        TreeSet<String> entries = new TreeSet<>();
        collectBindSideFingerprintEntries(ctx, plan, Map.of(), entries);
        return joinFingerprintEntries(entries);
    }

    /**
     * CREATE-time fingerprint: the bind-side entries UNION the tables of the OPTIMIZED
     * physical plan.
     *
     * Two defects are fixed by taking the plan side from the optimized plan's OWN
     * relations:
     *
     * - the fingerprint must describe the SAME metadata snapshot that produced the
     *   frozen output slots: SPMOptimizer.optimize returns AFTER planWithLock released
     *   the table locks, so re-resolving the tables here could hash a NEWER schema (an
     *   ALTER TABLE t ADD COLUMN x committing in between) than the frozen planSql was
     *   built against - the replay guard would then accept the stale baseline and
     *   silently omit x. The optimized plan's catalog relations hold the under-lock
     *   TableIf objects, so hashing THEM pins the frozen slots and the fingerprint to
     *   one snapshot;
     * - tables used only by the stored PLAN text (bind over t, plan over u) were
     *   missing entirely: after u is dropped, a matching t query still passed the
     *   guard, rewrote to frozen SQL over the missing u and failed with
     *   enable_spm_fallback=false although the original query was valid. The physical
     *   walk adds every plan-side table, so a dropped u now skips the stale baseline.
     *
     * @param ctx           the creating session
     * @param bindPlan      the parsed (unbound) bind tree
     * @param optimizedPlan the SPM-optimized physical plan the frozen SQL came from
     * @param storedPlanSql the planSql text that gets STORED (the decompiled frozen text
     *                      or the user fallback) - its function names are fingerprinted
     * @return the fingerprint (possibly empty, never null)
     */
    public static String schemaFingerprintForCreate(ConnectContext ctx, Plan bindPlan,
            Plan optimizedPlan, String storedPlanSql) {
        if (bindPlan == null || ctx == null || ctx.getStatementContext() == null) {
            return "";
        }
        TreeSet<String> entries = new TreeSet<>();
        collectBindSideFingerprintEntries(ctx, bindPlan,
                lockedTablesByQualifiedName(optimizedPlan), entries);
        collectPhysicalTableEntries(optimizedPlan, entries);
        // Functions used ONLY by the stored plan (bind "SELECT k FROM t", plan
        // "SELECT f(k) AS k FROM t") must be pinned too: the bind side never mentions
        // f, so redefining it (x+1 -> x+2) left this fingerprint - and the bind SQL -
        // unchanged, and the replay guard accepted a frozen plan inlining the OLD body.
        // The entries come from the STORED TEXT (not from the optimized plan's bound
        // functions): the frozen text is the artifact every replay re-plans, and a
        // re-plan may legally choose an equivalent builtin with another NAME
        // (years_add vs date_add), which made a plan-side walk permanently mismatch.
        collectPlanTextFunctionEntries(ctx, storedPlanSql, entries);
        return joinFingerprintEntries(entries);
    }

    /**
     * REPLAY-time revalidation of a frozen baseline (see
     * org.apache.doris.nereids.spm.SPMPlanner#verifyReplayMetadata): the stored
     * fingerprint recomputed from the metadata the REPLAYED plan was actually planned
     * with - the planned physical plan's catalog relations - plus the baseline's bind
     * tree resolved in this context. The pre-match guard runs BEFORE the query planner
     * takes its metadata locks, so an ALTER TABLE committing in between could otherwise
     * drift between validation and planning.
     *
     * @param ctx         the replaying session
     * @param bindPlan    the baseline's parameterized bind tree (may be null)
     * @param plannedPlan the replayed plan after planning (may be null)
     * @param storedPlanSql the planSql text the replay was built from (same value the
     *                    CREATE persisted, so the function entries match)
     * @return the fingerprint (possibly empty, never null)
     */
    public static String schemaFingerprintForReplay(ConnectContext ctx, Plan bindPlan,
            Plan plannedPlan, String storedPlanSql) {
        if (ctx == null) {
            return "";
        }
        TreeSet<String> entries = new TreeSet<>();
        if (bindPlan != null && ctx.getStatementContext() != null) {
            collectBindSideFingerprintEntries(ctx, bindPlan,
                    lockedTablesByQualifiedName(plannedPlan), entries);
        }
        collectPhysicalTableEntries(plannedPlan, entries);
        collectPlanTextFunctionEntries(ctx, storedPlanSql, entries);
        return joinFingerprintEntries(entries);
    }

    /** Bind-side table + function entries (see the callers' contracts). */
    private static void collectBindSideFingerprintEntries(ConnectContext ctx, Plan plan,
            Map<String, TableIf> lockedTables, TreeSet<String> entries) {
        walkPlansScoped(plan, Collections.emptySet(), (Plan node, Set<String> visibleCtes) -> {
            if (node instanceof UnboundRelation) {
                UnboundRelation relation = (UnboundRelation) node;
                if (isCteReference(relation, visibleCtes)) {
                    // A reference to a WITH alias is not a catalog relation: resolving "c"
                    // would pin a same-named catalog VIEW / table the query never reads (a
                    // phantom entry that also fails the pre-match containment check).
                    return;
                }
                try {
                    List<String> qualifier =
                            RelationUtil.getQualifierName(ctx, relation.getNameParts());
                    // Inside the create / replay window the FROZEN output slots were produced
                    // by a plan that already held the table locks: re-resolving the table
                    // here could hash a NEWER schema (an ALTER TABLE ADD COLUMN x committing
                    // after planWithLock released them) than the frozen slots were built
                    // from - same-text queries would pass the guard and silently omit x.
                    // When the plan carries the relation, its TableIf IS the under-lock
                    // snapshot and is hashed instead.
                    TableIf locked = lookupLockedTable(lockedTables, qualifier,
                            relation.getNameParts());
                    // Resolve WITHOUT the planner's resolved-table cache: this fingerprint
                    // runs BEFORE collectAndLockTable, and caching the TableIf here would
                    // hand the later bind / lock pass a detached PRE-LOCK object - a
                    // concurrent DROP / CREATE t would be invisible (CollectRelation reuses
                    // the cached instance, so the lock and the post-plan fingerprint both
                    // operate on the old table). The replay path revalidates against the
                    // relations the plan actually locked (verifyReplayMetadata).
                    TableIf table = locked != null ? locked
                            : ctx.getStatementContext().resolveTableWithoutCache(
                                    qualifier, Optional.of(relation));
                    entries.add(describeTableForFingerprint(table));
                } catch (RuntimeException e) {
                    // unresolvable: not part of the fingerprint, the analysis pass reports
                }
            }
        });
        // Non-table dependencies (validated before every replay exactly like the table
        // schema): analysis INLINES an alias-UDF body, so f(x): x+1 -> x+2 leaves both
        // the bind SQL and the table fingerprint unchanged while the frozen SQL keeps
        // projecting the old body. Every referenced function contributes an entry, so a
        // changed definition fails the same check as a changed table schema. key(...)
        // is a volatile secret (folded at optimize time): marker only, such baselines
        // are rejected at CREATE (rejectVolatileFunctionDependencies).
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expr : node.getExpressions()) {
                collectFunctionDependencies(ctx, expr, entries);
            }
            // the ASOF MATCH_CONDITION lives outside getExpressions(): an alias UDF used
            // only as the ASOF boundary must be fingerprinted like any other call
            for (Expression expr : outOfBandExpressions(node)) {
                collectFunctionDependencies(ctx, expr, entries);
            }
        });
    }

    /**
     * The expressions of one plan node that are NOT reachable through
     * Plan#getExpressions(): today the ASOF USING join's MATCH_CONDITION
     * (LogicalUsingJoin#getMatchCondition()). Missing it made the function
     * dependency walks blind to an alias UDF used ONLY as the ASOF boundary: analysis
     * inlines its body into the frozen SQL, so redefining the UDF changes which right row
     * a direct ASOF query picks while the stored fingerprint still matches and replays
     * the OLD boundary.
     */
    private static List<Expression> outOfBandExpressions(Plan node) {
        if (node instanceof LogicalUsingJoin) {
            Optional<Expression> matchCondition =
                    ((LogicalUsingJoin<?, ?>) node).getMatchCondition();
            if (matchCondition.isPresent()) {
                return List.of(matchCondition.get());
            }
        }
        return List.of();
    }

    /**
     * Tables of one planned tree keyed for the bind-side lookup: the bare table name and,
     * when the catalog object is available, the db-qualified name. These are the TableIf
     * instances the planner held its metadata locks on.
     */
    private static Map<String, TableIf> lockedTablesByQualifiedName(Plan plan) {
        if (plan == null) {
            return Map.of();
        }
        Map<String, TableIf> tables = new HashMap<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof org.apache.doris.nereids.trees.plans.physical
                    .PhysicalCatalogRelation) {
                TableIf table = ((org.apache.doris.nereids.trees.plans.physical
                        .PhysicalCatalogRelation) node).getTable();
                tables.put(table.getName().toLowerCase(Locale.ROOT), table);
                if (table.getDatabase() != null && table.getDatabase().getFullName() != null) {
                    tables.put((table.getDatabase().getFullName() + "." + table.getName())
                            .toLowerCase(Locale.ROOT), table);
                }
            }
        });
        return tables;
    }

    /**
     * Resolves one bind relation against the planned tree's tables (see the caller). The
     * exact qualifier wins; a qualifier whose suffix is a planned db-qualified name wins
     * next; the bare table name is used only as the last resort and only when it is
     * unambiguous.
     */
    private static TableIf lookupLockedTable(Map<String, TableIf> lockedTables,
            List<String> qualifier, List<String> nameParts) {
        if (lockedTables.isEmpty()) {
            return null;
        }
        boolean explicitNamespace = qualifier != null && !qualifier.isEmpty();
        if (explicitNamespace) {
            String lower = String.join(".", qualifier).toLowerCase(Locale.ROOT);
            TableIf exact = lockedTables.get(lower);
            if (exact != null) {
                return exact;
            }
            int dot = lower.indexOf('.');
            if (dot >= 0 && dot + 1 < lower.length()) {
                TableIf dbQualified = lockedTables.get(lower.substring(dot + 1));
                if (dbQualified != null) {
                    // The suffix drops the CATALOG component: cat2.db.t must never be
                    // fingerprinted as the bind table of cat1.db.t just because the
                    // plan-side map carries only the db-qualified key (a table of another
                    // catalog sharing database + table name). The stored entry would pin
                    // cat2's table id, the CORRECT cat1 fingerprint failed the pre-match
                    // containment check and the baseline could never match again. This
                    // branch runs BEFORE the bare-name guard below, so the guard's
                    // database check never saw it - verify the database AND the catalog
                    // here (an unverifiable name is accepted: only a PROVABLE mismatch
                    // rejects the pinned table, and a rejection falls through to the
                    // cache-less resolution of the caller, which hashes the real table).
                    if (databaseMatchesQualifier(dbQualified, qualifier)
                            && catalogMatchesQualifier(dbQualified, qualifier)) {
                        return dbQualified;
                    }
                }
            }
        }
        if (nameParts == null || nameParts.isEmpty()) {
            return null;
        }
        String bare = nameParts.get(nameParts.size() - 1).toLowerCase(Locale.ROOT);
        TableIf byBare = lockedTables.get(bare);
        if (byBare == null) {
            return null;
        }
        // unambiguous only: another db-qualified key pointing at a DIFFERENT table means
        // the bare name cannot identify the relation safely
        for (Map.Entry<String, TableIf> entry : lockedTables.entrySet()) {
            if (entry.getKey().endsWith("." + bare) && entry.getValue() != byBare) {
                return null;
            }
        }
        if (explicitNamespace && (!databaseMatchesQualifier(byBare, qualifier)
                || !catalogMatchesQualifier(byBare, qualifier))) {
            // The bind relation names a DIFFERENT explicit database (or CATALOG) than the
            // candidate's own: SELECT k FROM db1.t must never be fingerprinted as the
            // planned db2.t just because both tables are named t - the stored fingerprint
            // then omitted db1.t and the next query's CORRECT bind fingerprint failed the
            // pre-match containment check (the new baseline never applied). The catalog
            // half matters even in ONE catalog name space: cat1.db.t and cat2.db.t resolve
            // to different tables, and the bare name cannot tell them apart.
            return null;
        }
        return byBare;
    }

    /**
     * Whether a candidate table's OWN database matches the EXPLICIT qualifier of a bind
     * relation. An unknown database (partially mocked / db-less table) is accepted: only
     * a PROVABLE mismatch rejects the pinned table.
     */
    private static boolean databaseMatchesQualifier(TableIf table, List<String> qualifier) {
        if (table.getDatabase() == null || table.getDatabase().getFullName() == null) {
            return true;
        }
        String dbFullName = table.getDatabase().getFullName().toLowerCase(Locale.ROOT);
        // the qualifier is RelationUtil's [catalog, db, table] triple: the DATABASE is the
        // second-to-last component (using the last one compared the db name against the
        // TABLE name, so the bare-name fallback was rejected for every resolvable table)
        String qualifierDb = qualifier.get(qualifier.size() >= 2
                ? qualifier.size() - 2 : qualifier.size() - 1).toLowerCase(Locale.ROOT);
        return dbFullName.equals(qualifierDb) || dbFullName.endsWith("." + qualifierDb);
    }

    /**
     * Whether a candidate table's catalog matches the EXPLICIT catalog of a bind
     * relation. Mirrors databaseMatchesQualifier: an unknown catalog (partially
     * mocked table) or a qualifier without a catalog component is accepted, only a
     * PROVABLE mismatch rejects the pinned table - two catalogs may hold a database and
     * table of the same names.
     */
    private static boolean catalogMatchesQualifier(TableIf table, List<String> qualifier) {
        if (table.getDatabase() == null || table.getDatabase().getCatalog() == null
                || table.getDatabase().getCatalog().getName() == null
                || qualifier.size() < 3) {
            return true;
        }
        String catalogName = table.getDatabase().getCatalog().getName().toLowerCase(Locale.ROOT);
        String qualifierCatalog = qualifier.get(qualifier.size() - 3).toLowerCase(Locale.ROOT);
        return catalogName.equals(qualifierCatalog);
    }

    /** Tables of a planned physical tree, taken from each relation's OWN TableIf. */
    private static void collectPhysicalTableEntries(Plan plan, TreeSet<String> entries) {
        if (plan == null) {
            return;
        }
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof org.apache.doris.nereids.trees.plans.physical
                    .PhysicalCatalogRelation) {
                entries.add(describeTableForFingerprint(
                        ((org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation)
                                node).getTable()));
            }
        });
    }

    /**
     * Function entries of ONE stored plan TEXT - the exact names every replay of that
     * text will resolve again, so both fingerprint computations (CREATE / replay) walk
     * the SAME string and stay symmetric. The parse runs pinned to the DEFAULT mode
     * (which is also the mode a frozen text is re-parsed with, see
     * BaselinePlan#getPlanSqlMode()), so a session that carries another mode
     * cannot produce a different name set.
     */
    private static void collectPlanTextFunctionEntries(ConnectContext ctx, String planSql,
            TreeSet<String> entries) {
        if (planSql == null || planSql.isEmpty()) {
            return;
        }
        LogicalPlan plan;
        try {
            Plan parsed = org.apache.doris.qe.SqlModeHelper.withSqlMode(
                    org.apache.doris.qe.SqlModeHelper.MODE_DEFAULT,
                    () -> new org.apache.doris.nereids.parser.NereidsParser()
                            .parseSingle(planSql));
            if (!(parsed instanceof LogicalPlan)) {
                return;
            }
            plan = (LogicalPlan) parsed;
        } catch (RuntimeException e) {
            // a text this SIDE cannot parse contributes no function names; the replay
            // itself will fail loudly on it
            return;
        }
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expr : node.getExpressions()) {
                collectFunctionDependencies(ctx, expr, entries);
            }
            // the parsed ASOF MATCH_CONDITION is out of band here as well
            for (Expression expr : outOfBandExpressions(node)) {
                collectFunctionDependencies(ctx, expr, entries);
            }
        });
    }

    /**
     * Whether every entry of the CURRENT bind-side fingerprint is still present in the
     * STORED one. The stored fingerprint is the bind side UNION the plan side of the
     * frozen plan (schemaFingerprintForCreate), and the pre-match guard runs
     * BEFORE the query is planned - the plan side cannot be recomputed there (it is
     * revalidated after planning, see
     * org.apache.doris.nereids.spm.SPMPlanner#verifyReplayMetadata). Membership
     * is still fail-closed for the bind side: a DROP + CREATE (new table id) or an ALTER
     * (new schema hash) replaces an entry instead of extending the fingerprint, so the
     * new entry is no longer contained.
     *
     * A STORED entry may lack the nullability section (a row persisted before that
     * section existed, legacyEntryOf): such an entry accepts the CURRENT one
     * with any nullability, because the pre-upgrade fingerprint simply did not record
     * it. A stored entry that HAS the section is compared verbatim - this is exactly how
     * "the declared nullability changed after the plan was frozen" fails closed.
     *
     * @param stored  the fingerprint persisted with the baseline (may be null/empty)
     * @param current the bind-side fingerprint of the current query (may be null/empty)
     * @return whether the current bind side is contained (an empty current side always is)
     */
    public static boolean schemaFingerprintBindSideContained(String stored, String current) {
        if (current == null || current.isEmpty()) {
            return true;
        }
        if (stored == null || stored.isEmpty()) {
            return false;
        }
        java.util.Set<String> storedEntries =
                new TreeSet<>(Arrays.asList(stored.split(";")));
        for (String entry : current.split(";")) {
            if (entry.isEmpty()) {
                continue;
            }
            if (!storedEntries.contains(entry)
                    && !storedEntries.contains(legacyEntryOf(entry))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a freshly computed fingerprint still describes the STORED one: either the
     * two are equal, or the stored one simply predates the nullability section (see
     * legacyEntryOf). Used by the post-planning replay validation, which
     * compares the FULL (bind + plan side) fingerprints as two unordered sets.
     *
     * @param stored  the fingerprint persisted with the baseline
     * @param current the fingerprint recomputed from the replayed plan
     * @return whether the stored fingerprint still describes the current metadata
     */
    public static boolean schemaFingerprintEquivalent(String stored, String current) {
        if (stored == null || current == null) {
            return stored == null && current == null;
        }
        java.util.Set<String> storedEntries =
                new TreeSet<>(Arrays.asList(stored.split(";")));
        java.util.Set<String> currentEntries =
                new TreeSet<>(Arrays.asList(current.split(";")));
        if (storedEntries.equals(currentEntries)) {
            return true;
        }
        java.util.Set<String> legacyCurrent = new TreeSet<>();
        for (String entry : currentEntries) {
            legacyCurrent.add(legacyEntryOf(entry));
        }
        return storedEntries.equals(legacyCurrent);
    }

    /**
     * The PRE-nullability format of one fingerprint entry: the trailing
     * nullability section removed. A fingerprint persisted before that
     * section existed carries such entries; a fingerprint written since never does.
     */
    private static String legacyEntryOf(String entry) {
        int index = entry.lastIndexOf('|' + NULLABILITY_SECTION);
        return index < 0 ? entry : entry.substring(0, index);
    }

    /** Join of fingerprint entries (';'-separated, order-independent). */
    private static String joinFingerprintEntries(TreeSet<String> entries) {
        if (entries.isEmpty()) {
            return "";
        }
        StringBuilder fingerprint = new StringBuilder();
        for (String entry : entries) {
            if (fingerprint.length() > 0) {
                fingerprint.append(';');
            }
            fingerprint.append(entry);
        }
        return fingerprint.toString();
    }

    /** One fingerprint entry of one resolved table: name + id + base-schema hash. */
    private static String describeTableForFingerprint(TableIf table) {
        StringBuilder schema = new StringBuilder();
        StringBuilder nullability = new StringBuilder();
        for (Column column : table.getBaseSchema()) {
            // NULLABILITY is part of the replay contract: SPM leaves ELIMINATE_NOT_NULL
            // enabled, so "v IS NOT NULL" freezes away when v is declared NOT NULL - and
            // ALTER TABLE t MODIFY COLUMN v INT NULL changes no name / type hashed here.
            // Without the flag the frozen (now filterless) plan kept matching after the
            // column became nullable, replaying rows the original query filtered out.
            //
            // The flag travels as a SEPARATE, FIXED-SIZE entry section: folding it into
            // the schema hash would rewrite the hash of EVERY table that has a NOT NULL
            // column (invalidating every baseline persisted before the section existed),
            // while appending one digit per column grew the entry without bound - a
            // VALID query joining ten 500-column tables would exceed the fingerprint
            // column's VARCHAR(4096) before the table names and could not be persisted at
            // all. The section is a digest of the flag list, so it still separates
            // nullability changes exactly (a section-less pre-upgrade entry is accepted
            // by the comparisons below, two section-carrying entries must agree).
            schema.append(column.getName()).append(':')
                    .append(column.getType().toString())
                    .append(',');
            nullability.append(column.isAllowNull() ? '1' : '0');
        }
        return table.getName() + "|" + table.getId() + "|" + SPMUtils.hashOf(schema.toString())
                + "|" + NULLABILITY_SECTION + SPMUtils.hashOf(nullability.toString());
    }

    /** Records one function call's dependency entry (see the schemaFingerprint caller). */
    private static void collectFunctionDependencies(ConnectContext ctx, Expression expr,
            TreeSet<String> entries) {
        if (expr instanceof org.apache.doris.nereids.analyzer.UnboundFunction) {
            entries.add(describeFunctionDependency(ctx,
                    (org.apache.doris.nereids.analyzer.UnboundFunction) expr));
        }
        if (expr instanceof UnboundStar) {
            // UnboundStar has NO expression children: the payload of
            // SELECT * REPLACE(f(k) AS k) lives in getReplacedAlias(), OUTSIDE children(),
            // so the walk missed every function referenced there and a redefinition of f
            // left the stored fingerprint unchanged while the frozen plan kept the old body.
            for (NamedExpression replaced : ((UnboundStar) expr).getReplacedAlias()) {
                collectFunctionDependencies(ctx, replaced, entries);
            }
        }
        for (Expression child : expr.children()) {
            collectFunctionDependencies(ctx, child, entries);
        }
    }

    /**
     * One dependency entry of one referenced function, a function of EVERYTHING that can
     * change how the call RESOLVES:
     *
     * - the WRITTEN database qualifier: a qualified call resolves only through that
     *   database's UDF scope, while an unqualified one searches the CURRENT database
     *   then the global scope. The old lookup always searched ctx.getDatabase() and
     *   IGNORED function.getDbName(), so other_db.f(varchar_col) hashed (or missed)
     *   the wrong scope and changing the real overload left the entry unchanged;
     * - every same-name UDF overload (alias body + saved definition-time variables +
     *   argument types), NOT just the first one: the plans checked here are UNBOUND, so
     *   the exact overload the analyzer will pick cannot be reproduced; hashing the
     *   whole set fails closed when ANY overload changes;
     * - the effective builtin/UDF choice: prefer_udf_over_builtin decides between a
     *   colliding alias UDF and a builtin of the same name (and a qualified call never
     *   falls back to a builtin), so flipping the flag after freezing a colliding
     *   abs(INT) invalidates the baseline instead of replaying the other implementation.
     *
     * key(...) folds a named secret into the plan and can be recreated without touching
     * any table: a volatile marker is recorded and CREATE refuses such baselines.
     */
    private static String describeFunctionDependency(ConnectContext ctx,
            org.apache.doris.nereids.analyzer.UnboundFunction function) {
        return describeFunctionResolution(ctx, function.getDbName(), function.getName());
    }

    /** One dependency entry shared by the bind-side (unbound) and plan-text walkers. */
    private static String describeFunctionResolution(ConnectContext ctx, String writtenDbRaw,
            String nameRaw) {
        // normalized: the parser preserves the written case, SUM and sum are the same
        // function and must produce the same entry
        String name = nameRaw == null ? "" : nameRaw.toLowerCase(java.util.Locale.ROOT);
        if ("key".equals(name)) {
            return "fn:key|volatile";
        }
        String writtenDb = writtenDbRaw == null || writtenDbRaw.isEmpty() ? null : writtenDbRaw;
        boolean qualified = writtenDb != null;
        try {
            org.apache.doris.catalog.FunctionRegistry registry =
                    org.apache.doris.catalog.Env.getCurrentEnv().getFunctionRegistry();
            // NAME-level UDF lookup: the plans checked here are UNBOUND, so the
            // argument-matching overload of findFunctionBuilder cannot resolve them.
            // findUdfBuilder lowercases the name and scans [db, global] with db = the
            // WRITTEN qualifier when present, otherwise the current database.
            java.util.List<org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder>
                    udfCandidates = registry.findUdfBuilder(
                            qualified ? writtenDb : (ctx == null ? null : ctx.getDatabase()), name);
            boolean builtinExists = registry.getName2BuiltinBuilders()
                    .get(name) != null
                    || registry.isBuiltinAggStateCombinator(name);
            boolean preferUdf = org.apache.doris.qe.ConnectContext.get() != null
                    && org.apache.doris.qe.ConnectContext.get().getSessionVariable()
                            .preferUdfOverBuiltin;
            java.util.TreeSet<String> overloads = new java.util.TreeSet<>();
            for (org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder candidate
                    : udfCandidates) {
                overloads.add(describeUdfCandidate(candidate));
            }
            // the effective resolution MIRRORS FunctionRegistry#findFunctionBuilder's
            // scope / preference order (arity and argument-type filtering cannot be
            // reproduced on an unbound call, so any overload change invalidates through
            // the hash instead)
            String kind;
            if (!qualified && registry.isBuiltinAggStateCombinator(name)) {
                kind = "builtin";
            } else if (qualified) {
                kind = overloads.isEmpty() ? "none" : "udf";
            } else if (preferUdf) {
                kind = !overloads.isEmpty() ? "udf" : (builtinExists ? "builtin" : "none");
            } else {
                kind = builtinExists ? "builtin" : (overloads.isEmpty() ? "none" : "udf");
            }
            return "fn:" + (qualified ? writtenDb + "." : "") + name
                    + "|" + kind
                    + "|u=" + (overloads.isEmpty() ? "-"
                            : SPMUtils.hashOf(String.join(",", overloads)));
        } catch (RuntimeException e) {
            // name-level lookup hiccup (null database / privilege probe): a stable
            // identity, so both sides degrade identically instead of mismatching
            return "fn?:" + name;
        }
    }

    /** Stable identity of one same-name UDF candidate (body for alias UDFs). */
    private static String describeUdfCandidate(
            org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder builder) {
        String signature = "";
        if (builder instanceof org.apache.doris.nereids.trees.expressions.functions.udf
                .UdfBuilder) {
            org.apache.doris.nereids.trees.expressions.functions.udf.UdfBuilder udfBuilder =
                    (org.apache.doris.nereids.trees.expressions.functions.udf.UdfBuilder) builder;
            signature = udfBuilder.getArgTypes() + "|" + udfBuilder.hasVarArguments();
        }
        if (builder instanceof org.apache.doris.nereids.trees.expressions.functions.udf
                .AliasUdfBuilder) {
            org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf udf =
                    ((org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder)
                            builder).getAliasUdf();
            return "alias:" + (udf == null ? "null"
                    : udf.getUnboundFunction().toSql() + "|" + udf.getSessionVariables())
                    + "|" + signature;
        }
        return "other:" + builder.functionClass().getName() + "|" + signature;
    }

    /**
     * Rejects creating a baseline over a plan referencing a VOLATILE non-table
     * dependency: key(...) folds the named encryption key into a concrete value during
     * optimization, so a recreated key would leave the frozen SQL with the old secret
     * while the same bind SQL still matches. The folded value cannot be re-validated
     * before replay, so such baselines are refused at CREATE (alias UDF definitions are
     * instead TRACKED by the schema fingerprint: a definition change fails the replay
     * check).
     *
     * @param bindPlan the unbound bind plan
     */
    public static void rejectVolatileFunctionDependencies(LogicalPlan bindPlan) {
        rejectVolatileFunctionDependencies(bindPlan, null);
    }

    /**
     * Rejects creating a baseline whose BIND tree or PLAN tree references the VOLATILE
     * key(...) dependency. Both inputs are checked before optimizing / storing: the plan
     * text may be the only text carrying the secret, and KEY db.key parses
     * straight to a bound EncryptKeyRef - the name-only UnboundFunction check
     * missed it entirely, so the optimizer folded the key into the frozen plan and replay
     * served the creator's secret.
     *
     * @param bindPlan the parsed bind tree (may be null)
     * @param planPlan the parsed plan tree (may be null)
     */
    public static void rejectVolatileFunctionDependencies(LogicalPlan bindPlan,
            LogicalPlan planPlan) {
        rejectVolatileTree(bindPlan);
        if (planPlan != null && planPlan != bindPlan) {
            rejectVolatileTree(planPlan);
        }
    }

    private static void rejectVolatileTree(LogicalPlan plan) {
        if (plan == null) {
            return;
        }
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expr : node.getExpressions()) {
                if (referencesKeyFunction(expr)) {
                    found[0] = true;
                }
            }
            // the ASOF MATCH_CONDITION lives outside getExpressions(): a key(...) used
            // only as the ASOF boundary must be refused just like any other
            for (Expression expr : outOfBandExpressions(node)) {
                if (referencesKeyFunction(expr)) {
                    found[0] = true;
                }
            }
            // A LATERAL VIEW / UNNEST generator argument lives outside getExpressions()
            // too: one whose value FOLDS to a constant under session state (e.g.
            // explode(array(from_unixtime(0)))) is evaluated in the CREATOR's session
            // time zone and stored as that literal in the frozen plan text, so a replay
            // under another session's zone would return the creator's value although the
            // caller's own query evaluates the function in its own zone. Matching keeps
            // generator arguments concrete, so the value can only be frozen - such a
            // baseline is refused instead (see rejectFoldedZoneSensitiveGenerators).
            if (node instanceof LogicalGenerate) {
                rejectFoldedZoneSensitiveGenerators((LogicalGenerate<? extends Plan>) node);
            }
        });
        if (found[0]) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM does not support baselines using the key() function: the folded key"
                            + " value cannot be re-validated before replay");
        }
    }

    /**
     * Refuses a baseline whose GENERATOR argument would freeze a session-time-zone
     * dependent value: a constant-argument from_unixtime / unix_timestamp / now-family
     * call inside a LATERAL VIEW / UNNEST argument is FOLDED by the optimizer in the
     * CREATOR's session zone and rendered as that literal into the frozen plan text (the
     * generator arguments stay concrete for matching, see visitLogicalGenerate), so a
     * caller in another zone would replay the creator's value. The user must rewrite the
     * argument to depend on a column (evaluated per row in the CALLER's zone) or drop the
     * baseline.
     */
    private static void rejectFoldedZoneSensitiveGenerators(LogicalGenerate<? extends Plan> generate) {
        for (Expression generator : generate.getGenerators()) {
            if (containsFoldedZoneSensitiveCall(generator)) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "SPM does not support a baseline whose generator argument folds a"
                                + " session-time-zone dependent function ("
                                + generator.toSql() + "): the value is evaluated in the"
                                + " CREATOR's session zone and frozen into the plan text, so a"
                                + " replay in another zone would return the creator's value."
                                + " Make the argument depend on a column, or drop the"
                                + " baseline");
            }
        }
    }

    /** Whether one expression tree contains a constant-argument zone-sensitive call. */
    private static boolean containsFoldedZoneSensitiveCall(Expression expression) {
        if (expression
                instanceof org.apache.doris.nereids.trees.expressions.functions.Function
                && isZoneSensitiveFunctionName(
                        ((org.apache.doris.nereids.trees.expressions.functions.Function) expression)
                                .getName())
                && isLiteralOnly(expression)) {
            return true;
        }
        for (Expression child : expression.children()) {
            if (containsFoldedZoneSensitiveCall(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a frozen planSql would persist a value the CREATOR's session evaluated while
     * the caller's own query evaluates it in its own session. The two OUT-OF-BAND payloads
     * the placeholder machinery never parameterizes are checked:
     *
     * - an inline VALUES cell (UnboundInlineTable has no child plans, so the whole-tree
     *   transformer never sees its cells; the cells are additionally compared CONCRETELY,
     *   see the VALUES alignment guard): SELECT * FROM (VALUES (from_unixtime(0))) t
     * - a * REPLACE payload (stored outside the star's expression children and compared
     *   concretely, see SPMAstCheckVisitor): SELECT * REPLACE(from_unixtime(0) AS a) FROM t
     *
     * In both positions the creator-context optimization FOLDS a constant-argument
     * zone-sensitive call into the CREATOR's zone literal (the placeholder that blocks the
     * fold elsewhere is never created there), and the decompiled text would serve that
     * value to every later caller although the caller's own query evaluates the function in
     * its zone. The CREATE declines the freeze for such a statement instead: the raw
     * planSql is stored and the rewrite replays the parameterized tree, which re-evaluates
     * the expression under the CALLER's session. A call spelled over a COLUMN stays
     * freezable - it is evaluated per row, in the caller's own session.
     *
     * @param plan the parsed (unbound) tree
     * @return true when the freeze must be declined for such a payload
     */
    public static boolean containsFoldedSessionSensitivePayload(LogicalPlan plan) {
        if (plan == null) {
            return false;
        }
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (found[0]) {
                return;
            }
            if (node instanceof InlineTable) {
                for (List<NamedExpression> row : ((InlineTable) node).getConstantExprsList()) {
                    for (NamedExpression cell : row) {
                        if (containsFoldedZoneSensitiveCall(cell)) {
                            found[0] = true;
                            return;
                        }
                    }
                }
            }
            for (Expression expr : node.getExpressions()) {
                if (containsFoldedZoneSensitiveStarPayload(expr)) {
                    found[0] = true;
                    return;
                }
            }
        });
        return found[0];
    }

    /** Whether an expression tree carries a star whose REPLACE payload folds a
     * constant-argument zone-sensitive call (see containsFoldedSessionSensitivePayload). */
    private static boolean containsFoldedZoneSensitiveStarPayload(Expression expression) {
        if (expression instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expression).getReplacedAlias()) {
                if (containsFoldedZoneSensitiveCall(replaced)) {
                    return true;
                }
            }
        }
        for (Expression child : expression.children()) {
            if (containsFoldedZoneSensitiveStarPayload(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The functions whose value depends on the SESSION time zone (and whose constant
     * arguments therefore must not be frozen): from_unixtime / unix_timestamp interpret
     * their input in the session zone, and the now-family reads the session's current
     * local time.
     */
    private static boolean isZoneSensitiveFunctionName(String name) {
        return "from_unixtime".equalsIgnoreCase(name)
                || "unix_timestamp".equalsIgnoreCase(name)
                || "now".equalsIgnoreCase(name)
                || "curdate".equalsIgnoreCase(name)
                || "curtime".equalsIgnoreCase(name)
                || "current_timestamp".equalsIgnoreCase(name)
                || "current_date".equalsIgnoreCase(name)
                || "current_time".equalsIgnoreCase(name)
                || "localtime".equalsIgnoreCase(name)
                || "localtimestamp".equalsIgnoreCase(name)
                || "sysdate".equalsIgnoreCase(name);
    }

    /**
     * Whether an expression consists of literals only (foldable to a constant): a call
     * over a COLUMN is evaluated at execution time - in the CALLER's session zone - and is
     * therefore safe to freeze.
     */
    private static boolean isLiteralOnly(Expression expression) {
        if (expression instanceof org.apache.doris.nereids.trees.expressions.literal.Literal) {
            return true;
        }
        if (expression.children().isEmpty()) {
            return false;
        }
        for (Expression child : expression.children()) {
            if (!isLiteralOnly(child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean referencesKeyFunction(Expression expr) {
        // KEY db.key parses to EncryptKeyRef directly (no UnboundFunction on the path),
        // so recognizing the resolved shape is what makes `SELECT KEY k.x ...` fail fast
        // instead of storing a plan with the folded secret.
        if (expr instanceof EncryptKeyRef) {
            return true;
        }
        if (expr instanceof org.apache.doris.nereids.analyzer.UnboundFunction
                && "key".equalsIgnoreCase(
                        ((org.apache.doris.nereids.analyzer.UnboundFunction) expr).getName())) {
            return true;
        }
        if (expr instanceof UnboundStar) {
            for (NamedExpression replaced : ((UnboundStar) expr).getReplacedAlias()) {
                if (referencesKeyFunction(replaced)) {
                    return true;
                }
            }
        }
        for (Expression child : expr.children()) {
            if (referencesKeyFunction(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Optional value equality with a textual fallback for value types without equals.
     *
     * TableSample is compared by its VALUE equality only: it overrides
     * equals / hashCode by value but not toString, so its default
     * text is the identity hash. The textual fallback was therefore wrong in BOTH
     * directions for it - two identical clauses parsed separately (bind vs plan, two
     * audit parses of one statement) compared unequal, and two DIFFERENT samples compared
     * equal whenever the two objects' identity hashes collided.
     */
    private static <T> boolean sameOptionalValue(Optional<T> bind, Optional<T> user) {
        if (!bind.isPresent() || !user.isPresent()) {
            return bind.isPresent() == user.isPresent();
        }
        T bindValue = bind.get();
        T userValue = user.get();
        if (bindValue instanceof TableSample && userValue instanceof TableSample) {
            return bindValue.equals(userValue);
        }
        return Objects.equals(bindValue, userValue)
                || Objects.equals(bindValue.toString(), userValue.toString());
    }

    private static boolean sameScanParams(TableScanParams bind, TableScanParams user) {
        if (bind == null || user == null) {
            return bind == user;
        }
        // TableScanParams overrides neither equals nor toString and every parse creates a
        // fresh instance, so identity and default equality / string comparison are all
        // false even for identical @branch / @tag / @options / @incr syntax - a baseline
        // using scan parameters could never hit. Compare the type and the payloads.
        return Objects.equals(bind.getParamType(), user.getParamType())
                && Objects.equals(bind.getMapParams(), user.getMapParams())
                && Objects.equals(bind.getListParams(), user.getListParams());
    }

    /**
     * Removes the CHECK ROW POLICY / DATA MASK markers from a plan tree. SPM optimizes the
     * frozen plan in the CREATOR's context, so leaving the markers in place makes the
     * rewrite resolve the CREATOR's row filter / data mask into the frozen SQL - another
     * user who merely matches the same bind query would then replay the creator's policy
     * as an ordinary predicate / projection (rows disappear or values stay masked).
     *
     * The STORED parameterized trees keep their markers: an in-memory fallback replay
     * still evaluates the EXECUTING user's policy. A frozen text is re-parsed at replay,
     * which re-creates the markers for that user as well - so every replay keeps the
     * normal policy checks of whoever runs it, while the frozen plan itself carries none
     * of the creator's.
     *
     * @param plan the tree to strip (may be null)
     * @return the same structure without LogicalCheckPolicy nodes
     * @throws IllegalStateException when a marker survives the strip (a carrier the
     *                               passes do not know); the CREATE must then fail rather
     *                               than freeze the creator's policy
     */
    public static LogicalPlan stripCheckPolicy(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }
        // Two passes, because the markers appear in two places:
        //
        // 1) a plan an EXPRESSION owns (IN / EXISTS / scalar subquery plans - including the
        //    ones in a * REPLACE payload - and the ASOF MATCH_CONDITION): the marker of
        //    the subquery's relation ("WHERE k IN (SELECT k FROM u)") is not reachable
        //    through children(), so the standard expression transform walks the tree and
        //    rebuilds each owning node around the stripped subquery plan. Without this the
        //    nested analyzer expanded the CREATOR's policy on u into an ordinary filter
        //    INSIDE the frozen SQL: a different user matching the same bind query then
        //    replayed the creator's row filter / data mask, although that user has no
        //    policy (or a different one) - and the policy applies per relation, so a
        //    subquery relation leaked exactly like a top-level one.
        //
        // 2) the markers wrapped around the tree's OWN relations (children / CTE bodies).
        Plan expressionsStripped = transform(plan, STRIP_POLICY_IN_EXPRESSION);
        Plan stripped = stripCheckPolicyNodes(expressionsStripped == null
                ? plan : expressionsStripped);
        LogicalPlan strippedPlan = stripped instanceof LogicalPlan
                ? (LogicalPlan) stripped : plan;
        if (containsCheckPolicyMarker(strippedPlan)) {
            // The two passes cover every carrier a PARSED tree can put a marker in
            // (relations inside children / CTE bodies and inside expression-owned
            // subquery plans). A survivor means a future shape is outside them, and
            // freezing it would persist the CREATOR's row filter / data mask into the
            // frozen SQL for every matching user: refuse the CREATE instead.
            throw new IllegalStateException("SPM cannot strip every CHECK ROW POLICY /"
                    + " DATA MASK marker from the plan SQL (a relation is still wrapped in"
                    + " one); refusing to freeze a plan that may carry the creator's policy");
        }
        return strippedPlan;
    }

    /** Whether any relation of the tree is still wrapped in a policy marker. */
    private static boolean containsCheckPolicyMarker(Plan plan) {
        boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof LogicalCheckPolicy) {
                found[0] = true;
            }
        });
        return found[0];
    }

    /** Recursive worker of STRIP_POLICY_IN_EXPRESSION. */
    private static Expression stripPolicyOfExpression(Expression expr) {
        if (expr instanceof SubqueryExpr) {
            LogicalPlan subPlan = ((SubqueryExpr) expr).getQueryPlan();
            LogicalPlan stripped = stripCheckPolicy(subPlan);
            return stripped != null && stripped != subPlan
                    ? ((SubqueryExpr) expr).withSubquery(stripped) : expr;
        }
        if (expr instanceof UnboundStar) {
            // SELECT * REPLACE((SELECT max(v) FROM u) AS k): the replacement payloads own
            // their subquery plans OUTSIDE children(), so the generic recursion below never
            // reaches them.
            UnboundStar star = (UnboundStar) expr;
            List<NamedExpression> replaced = star.getReplacedAlias();
            if (replaced.isEmpty()) {
                return expr;
            }
            boolean changed = false;
            List<NamedExpression> newReplaced = new ArrayList<>(replaced.size());
            for (NamedExpression replacement : replaced) {
                Expression newReplacement = stripPolicyOfExpression(replacement);
                newReplaced.add(newReplacement instanceof NamedExpression
                        ? (NamedExpression) newReplacement : replacement);
                changed |= newReplacement != replacement;
            }
            return changed ? new UnboundStar(star.getQualifier(), star.getExceptedSlots(),
                    newReplaced, star.getIndexInSqlString()) : expr;
        }
        if (expr.children().isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(expr.children().size());
        for (Expression child : expr.children()) {
            Expression newChild = stripPolicyOfExpression(child);
            newChildren.add(newChild);
            changed |= newChild != child;
        }
        return changed ? expr.withChildren(newChildren) : expr;
    }

    /** Recursive worker of stripCheckPolicy. */
    private static Plan stripCheckPolicyNodes(Plan plan) {
        if (plan instanceof LogicalCTE) {
            // CTE bodies live OUTSIDE children()
            LogicalCTE<?> cte = (LogicalCTE<?>) plan;
            List<LogicalSubQueryAlias<Plan>> newAliasQueries =
                    new ArrayList<>(cte.getAliasQueries().size());
            boolean changed = false;
            for (LogicalSubQueryAlias<Plan> alias : cte.getAliasQueries()) {
                Plan stripped = stripCheckPolicyNodes(alias);
                newAliasQueries.add((LogicalSubQueryAlias<Plan>) stripped);
                if (stripped != alias) {
                    changed = true;
                }
            }
            Plan newChild = cte.child(0) == null ? null : stripCheckPolicyNodes(cte.child(0));
            if (newChild != cte.child(0)) {
                changed = true;
            }
            if (!changed) {
                return cte;
            }
            try {
                return new LogicalCTE<Plan>(cte.isRecursive(), newAliasQueries, newChild);
            } catch (RuntimeException e) {
                return cte;
            }
        }
        List<Plan> children = plan.children();
        boolean changed = false;
        List<Plan> newChildren = new ArrayList<>(children.size());
        for (Plan child : children) {
            Plan stripped = stripCheckPolicyNodes(child);
            newChildren.add(stripped);
            if (stripped != child) {
                changed = true;
            }
        }
        Plan current = plan;
        if (changed) {
            try {
                current = plan.withChildren(newChildren);
            } catch (RuntimeException e) {
                current = plan;
            }
        }
        return current instanceof LogicalCheckPolicy ? current.child(0) : current;
    }

    // ==================== non-expression literal merge (LIMIT / OFFSET) ====================

    /**
     * Merges the user query's non-expression literals (LogicalLimit limit/offset) into
     * the rewritten plan tree, position aligned. Such literals are plain long fields on
     * the plan node (not Expressions), so they are not parameterized like scalar
     * literals; a user query whose LIMIT differs from the bind SQL must still be
     * rewritten with the USER limit/offset, otherwise the rewritten plan would run with
     * the captured limit and return a truncated result.
     *
     * @param plan     the rewritten plan tree (placeholders already substituted)
     * @param userPlan the user's original plan tree (source of the limit / offset)
     * @return the plan tree with the user's limit / offset merged in
     */
    public static LogicalPlan mergeLimits(LogicalPlan plan, LogicalPlan userPlan) {
        if (plan == null || userPlan == null) {
            return plan;
        }
        Plan merged = mergeLimitNode(plan, userPlan);
        return merged instanceof LogicalPlan ? (LogicalPlan) merged : plan;
    }

    /** Recursively merges the user's limit / offset into the plan, node by node. */
    private static Plan mergeLimitNode(Plan plan, Plan user) {
        if (plan.getClass() != user.getClass()) {
            // structural guard: never reshape a mismatching tree
            return plan;
        }
        if (plan instanceof LogicalCTE && user instanceof LogicalCTE) {
            LogicalCTE<?> cte = (LogicalCTE<?>) plan;
            LogicalCTE<?> userCte = (LogicalCTE<?>) user;
            // Separate bind / plan texts: the SPM optimizer preserves the CTE definitions
            // of both in the frozen SQL, and a matching query may declare FEWER (or more)
            // CTEs. The positional recursion must never run off the shorter list - an
            // IndexOutOfBounds here aborted the whole rewrite, and the caller silently
            // ignored the accepted baseline on EVERY match. Unmatched subtrees stay
            // unchanged (there is no corresponding position to merge a limit from).
            if (cte.getAliasQueries().size() != userCte.getAliasQueries().size()
                    || cte.children().size() != userCte.children().size()
                    || (cte.child(0) == null) != (userCte.child(0) == null)) {
                return plan;
            }
            Plan newChild = cte.child(0) == null ? null
                    : mergeLimitNode(cte.child(0), userCte.child(0));
            boolean childChanged = newChild != cte.child(0);
            List<LogicalSubQueryAlias<Plan>> newAliasQueries =
                    new ArrayList<>(cte.getAliasQueries().size());
            boolean aliasChanged = false;
            List<LogicalSubQueryAlias<Plan>> userAliasQueries = userCte.getAliasQueries();
            for (int i = 0; i < cte.getAliasQueries().size(); i++) {
                LogicalSubQueryAlias<Plan> alias = cte.getAliasQueries().get(i);
                LogicalSubQueryAlias<Plan> userAlias = userAliasQueries.get(i);
                Plan newAlias = mergeLimitNode(alias, userAlias);
                newAliasQueries.add((LogicalSubQueryAlias<Plan>) newAlias);
                if (newAlias != alias) {
                    aliasChanged = true;
                }
            }
            if (!childChanged && !aliasChanged) {
                return cte;
            }
            try {
                return new LogicalCTE<Plan>(cte.isRecursive(), newAliasQueries, newChild);
            } catch (RuntimeException e) {
                return cte;
            }
        }

        // merge the children first
        List<Plan> children = plan.children();
        List<Plan> userChildren = user.children();
        if (children.size() != userChildren.size()) {
            // same class with a different arity (defensive): never index past the end
            return plan;
        }
        boolean changed = false;
        List<Plan> newChildren = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            Plan newChild = mergeLimitNode(children.get(i), userChildren.get(i));
            newChildren.add(newChild);
            if (newChild != children.get(i)) {
                changed = true;
            }
        }
        Plan current = plan;
        if (changed) {
            try {
                current = plan.withChildren(newChildren);
            } catch (RuntimeException e) {
                current = plan;
            }
        }

        // adopt the user's limit / offset on the limit node itself
        if (current instanceof LogicalLimit && user instanceof LogicalLimit) {
            LogicalLimit<?> limit = (LogicalLimit<?>) current;
            LogicalLimit<?> userLimit = (LogicalLimit<?>) user;
            if (limit.getLimit() == userLimit.getLimit()
                    && limit.getOffset() == userLimit.getOffset()) {
                return current;
            }
            return new LogicalLimit<>(userLimit.getLimit(), userLimit.getOffset(),
                    limit.getPhase(), limit.child());
        }
        // LogicalTopN is a SEPARATE node (it does not extend LogicalLimit): a captured
        // ORDER BY ... LIMIT ... OFFSET ... baseline replays through its TopN node, and
        // without this branch the user's values never replaced the captured ones (a
        // matched query with a LARGER limit stayed capped at the capture-time limit).
        if (current instanceof LogicalTopN && user instanceof LogicalTopN) {
            LogicalTopN<?> topN = (LogicalTopN<?>) current;
            LogicalTopN<?> userTopN = (LogicalTopN<?>) user;
            if (topN.getLimit() == userTopN.getLimit()
                    && topN.getOffset() == userTopN.getOffset()) {
                return current;
            }
            return topN.withLimitChild(userTopN.getLimit(), userTopN.getOffset(), topN.child());
        }
        return current;
    }
}
