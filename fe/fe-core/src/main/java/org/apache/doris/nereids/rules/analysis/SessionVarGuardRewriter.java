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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NeedSessionVarGuard;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * The purpose of this class is to add session var guards to all expressions that require guarding
 * The purpose of the `rewritePlanTree()` method is to add session variable guards to all expressions
 * in a plan tree that require guarding
 * If you need to traverse and add to an expression, use AddSessionVarGuardRewriter
 * If you need to add a guard to the plan tree, use rewritePlanTree()
 * */
public class SessionVarGuardRewriter extends ExpressionRewrite {
    // Guard family masks: which session-variable dependency family must be guarded in a cache.
    // The mask is computed from the actual difference between the query session and the object creation
    // session, so a time-zone-only difference never disables rewrite of expressions that depend on other
    // variables (e.g. integer SUM over a decimal256 setting) and vice versa.
    public static final int GUARD_NONE = 0;
    public static final int GUARD_TIME_ZONE = 1;
    public static final int GUARD_OTHER = 2;

    /** The leading {@code major.minor.revision} of a Doris version string. */
    private static final java.util.regex.Pattern DORIS_RELEASE_VERSION =
            java.util.regex.Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

    private final List<Rule> rules;
    private final CascadesContext cascadesContext;

    public SessionVarGuardRewriter(Map<String, String> var, CascadesContext ctx) {
        this(var, currentAffectQueryResultInPlanVariables(), ctx);
    }

    /**
     * Creates a rewriter that guards the expressions of the plan owned by {@code var} against the current
     * session variables {@code currentVars}: time-zone sensitive expressions are guarded when the creation
     * zone differs, and NeedSessionVarGuard expressions are guarded when another affectQueryResult variable
     * differs.
     */
    public SessionVarGuardRewriter(Map<String, String> var, Map<String, String> currentVars, CascadesContext ctx) {
        // The executor rewrites every expression owned by a plan node (filter predicates, join conjuncts,
        // aggregate/group-by expressions, order keys, projects, ...), not just Alias children, so time-zone
        // sensitive expressions in any position of the plan are wrapped with a guard.
        super(new ExpressionRuleExecutor(ImmutableList.of(new AddGuardExpressionRewriteRule(
                new AddSessionVarGuardRewriter(var, currentVars)))));
        rules = buildRules();
        cascadesContext = ctx;
    }

    /**
     * Creates a rewriter that guards the families selected by {@code guardMask}. Used for the MTMV rewrite
     * caches: the mask is derived from the query session that first needs the guarded cache, and the guard
     * content is independent of the session the cache happens to be generated in (e.g. a background refresh
     * task runs in the creation zone and must not silently produce an unguarded "guarded" cache).
     */
    public SessionVarGuardRewriter(Map<String, String> var, int guardMask, CascadesContext ctx) {
        super(new ExpressionRuleExecutor(ImmutableList.of(new AddGuardExpressionRewriteRule(
                new AddSessionVarGuardRewriter(var, guardMask)))));
        rules = buildRules();
        cascadesContext = ctx;
    }

    /**rewrite all exprs in one plan node */
    private Plan rewritePlanNode(Plan plan) {
        for (Rule rule : rules) {
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, cascadesContext);
                Plan newPlan = newPlans.get(0);
                if (!newPlan.deepEquals(plan)) {
                    return newPlan;
                }
            }
        }
        return plan;
    }

    /**
     * Applies {@link AddSessionVarGuardRewriter} to the whole expression tree, so that non-Alias
     * expressions (e.g. filter predicates, join conjuncts) are guarded as well as alias children.
     */
    private static class AddGuardExpressionRewriteRule implements ExpressionRewriteRule<ExpressionRewriteContext> {
        private final AddSessionVarGuardRewriter addGuardRewriter;

        private AddGuardExpressionRewriteRule(AddSessionVarGuardRewriter addGuardRewriter) {
            this.addGuardRewriter = addGuardRewriter;
        }

        @Override
        public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
            return expr.accept(addGuardRewriter, Boolean.FALSE);
        }
    }

    /**
     * Wraps expressions whose value depends on session variables (or on the session time zone) in a
     * {@link SessionVarGuardExpr} when the relevant session variables differ from the ones persisted on
     * the object (view / materialized view / generated column) being processed.
     */
    public static class AddSessionVarGuardRewriter extends DefaultExpressionRewriter<Boolean> {
        private final Map<String, String> sessionVar;
        // Whether the time-zone family (time-zone sensitive expressions) must be guarded: the creation
        // time zone differs from the current one, or the persisted map does not carry time_zone at all
        // (pre-time_zone metadata), so the creation zone is unknown and must be treated as different.
        private final boolean timeZoneDiffersOrUnknown;
        // Whether the "other" guard family (NeedSessionVarGuard expressions, e.g. decimal256 dependent)
        // must be guarded: some affectQueryResult session variable other than time_zone differs.
        private final boolean otherSessionVarsDiffer;
        // True when the guards are added to a shared materialized-view rewrite cache (MTMVCache.from).
        // Cache-mismatch guards must stay structurally distinct from the guards BindRelation adds when
        // expanding a persisted object into the query, so that pre-RBO expression matching never equates a
        // query-side nested-object guard with the cache guard of an MTMV materialized in another zone.
        private final boolean cacheGuard;

        /**
         * Creates a guard rewriter that guards both dependency families unconditionally for the persisted
         * variables {@code var}. The guard decision must not depend on the current thread-local session:
         * the only production caller builds this rewriter inside an {@code AutoCloseSessionVariable} scope
         * where the current session already equals {@code var} (so deriving the decision from the
         * thread-local session would add no guard at all), while the wrapped expression is later
         * translated/executed in a different (load) session.
         *
         * @param var the persisted session variables of the object being processed
         */
        public AddSessionVarGuardRewriter(Map<String, String> var) {
            this(var, true, true, false);
        }

        /**
         * Creates a guard rewriter for the persisted session variables {@code var} against the current
         * query session variables {@code currentVars}.
         *
         * @param var the persisted session variables of the object being processed
         * @param currentVars the current query session's affectQueryResultInPlan variables
         */
        public AddSessionVarGuardRewriter(Map<String, String> var, Map<String, String> currentVars) {
            this(var,
                    // The creation zone is unknown when the persisted map is empty (pre-change metadata)
                    // or has no time_zone key (metadata written before time_zone became a plan variable);
                    // treat it as different so time-zone sensitive expressions are always guarded
                    // (compatibility fence). The empty captured map is transparent at execution.
                    var != null && (var.isEmpty() || !var.containsKey(SessionVariable.TIME_ZONE)
                            || !timeZonesEquivalent(var.get(SessionVariable.TIME_ZONE),
                                    currentVars == null ? null : currentVars.get(SessionVariable.TIME_ZONE))),
                    var != null && !var.isEmpty() && differsInNonTimeZoneVars(currentVars, var),
                    false);
        }

        /**
         * Creates a guard rewriter that guards the families selected by {@code guardMask} unconditionally.
         * Used when building a shared rewrite cache: the guarded cache must contain the guards regardless
         * of the session it is generated in, so a cache built in the creation zone (e.g. by a background
         * refresh task) is still effective for a query in a different zone. The produced guards are cache
         * guards (see {@link #cacheGuard}) so they never equal a query-side nested-object guard.
         *
         * @param var the persisted session variables of the object being processed
         * @param guardMask combination of {@link #GUARD_TIME_ZONE} and {@link #GUARD_OTHER}
         */
        public AddSessionVarGuardRewriter(Map<String, String> var, int guardMask) {
            this(var, (guardMask & GUARD_TIME_ZONE) != 0, (guardMask & GUARD_OTHER) != 0, true);
        }

        private AddSessionVarGuardRewriter(Map<String, String> var, boolean timeZoneDiffersOrUnknown,
                boolean otherSessionVarsDiffer, boolean cacheGuard) {
            this.sessionVar = var;
            this.timeZoneDiffersOrUnknown = timeZoneDiffersOrUnknown;
            this.otherSessionVarsDiffer = otherSessionVarsDiffer;
            this.cacheGuard = cacheGuard;
        }

        @Override
        public Expression visit(Expression expr, Boolean insideGuard) {
            Expression rewritten = rewriteChildren(this, expr, Boolean.FALSE);
            if (needsSessionVarGuard(rewritten) && !Boolean.TRUE.equals(insideGuard)) {
                if (sessionVar == null) {
                    return expr;
                }
                return new SessionVarGuardExpr(rewritten, sessionVar, cacheGuard);
            }
            return rewritten;
        }

        @Override
        public Expression visitSessionVarGuardExpr(SessionVarGuardExpr expr, Boolean context) {
            Expression child = expr.child().accept(this, Boolean.TRUE);
            Expression guarded = child != expr.child() ? expr.withChildren(ImmutableList.of(child)) : expr;
            // A cache-building rewriter must keep a cache-mismatch marker around an existing non-cache
            // guard, but ONLY for the guard family the cache is built for: the existing guard's dependency
            // family is re-derived from the expression it wraps (a NeedSessionVarGuard expression is the
            // "other" family, a time-zone sensitive one is the time-zone family), and an outer marker for a
            // family the cache does not guard (e.g. an "other"-family decimal guard under a time-zone-only
            // cache mask) would reject a safe cross-zone nested-view rewrite whose decimal semantics agree
            // through the view guard. Without the family scope an MTMV over a view carries the view's
            // query-side guard (cacheGuard=false) in its definition plan and the isCacheGuard() rejection
            // in AbstractMaterializedViewRule would never fire for the family that actually differs.
            if (cacheGuard && !expr.isCacheGuard() && sessionVar != null
                    && needsSessionVarGuard(guarded.child(0))) {
                return new SessionVarGuardExpr(guarded, sessionVar, true);
            }
            return guarded;
        }

        private boolean needsSessionVarGuard(Expression expr) {
            if (expr instanceof NeedSessionVarGuard) {
                return otherSessionVarsDiffer;
            }
            return timeZoneDiffersOrUnknown && isTimeZoneSensitive(expr);
        }

        /**
         * An expression is time-zone sensitive when its value is a session-time-zone dependent rendering of
         * a TIMESTAMPTZ value (stored as UTC), e.g. date_trunc/cast/floor on a timestamptz column, or a
         * string conversion of a TIMESTAMPTZ nested in a complex type (ARRAY&lt;timestamptz&gt;, MAP, STRUCT).
         * Zone-invariant operations must NOT be guarded so that safe rewrites keep working: plain
         * slot/literal passthroughs (a TIMESTAMPTZ slot holds the UTC instant), named expressions (their
         * children are guarded individually), aggregate functions such as COUNT/MIN/MAX (they preserve the
         * instant), IS (NOT) NULL checks, and comparisons / boolean combinations of them (equality,
         * ordering, BETWEEN and IN over instants compare the UTC instants and do not depend on the session
         * zone; any zone-dependent operand, e.g. a cast of an offset-free string into TIMESTAMPTZ, is
         * guarded on its own by the child visit).
         */
        private static boolean isTimeZoneSensitive(Expression expr) {
            if (expr instanceof Slot || expr instanceof Literal || expr instanceof NamedExpression
                    || expr instanceof AggregateFunction || expr instanceof IsNull
                    || expr instanceof ComparisonPredicate || expr instanceof Between
                    || expr instanceof InPredicate || expr instanceof And || expr instanceof Or
                    || expr instanceof Not
                    // Structural / subtype-constrained expressions must not be replaced by a guard: their
                    // owners rebuild them by casting (e.g. WindowExpression.withChildren casts ORDER BY keys
                    // back to OrderExpression and the frame to WindowFrame; GenerateExpressionRewrite casts
                    // the generator root back to Function). Wrapping them would throw a ClassCastException.
                    // Their value-producing children are still guarded individually by the visitor. A whole
                    // window expression is not guarded either: only the expressions it contains (partition
                    // keys, order keys, frame bounds) are visited, so a zone-invariant window function such
                    // as row_number() OVER (ORDER BY ts) keeps rewriting across zones.
                    || expr instanceof OrderExpression || expr instanceof WindowFrame
                    || expr instanceof WindowExpression || expr instanceof TableGeneratingFunction
                    // A lambda is a structural root as well: array_map/higher-order functions require their
                    // lambda argument to stay a Lambda, so wrapping it would break binding and re-analysis.
                    // The lambda body is still visited and guarded on its own.
                    || expr instanceof Lambda) {
                return false;
            }
            try {
                return containsTimeStampTz(expr);
            } catch (UnboundException e) {
                return false;
            }
        }

        private static boolean containsTimeStampTz(Expression expr) {
            return expr.anyMatch(e -> containsTimeStampTz(((Expression) e).getDataType()));
        }

        private static boolean containsTimeStampTz(DataType dataType) {
            if (dataType instanceof TimeStampTzType) {
                return true;
            }
            if (dataType instanceof ArrayType) {
                return containsTimeStampTz(((ArrayType) dataType).getItemType());
            }
            if (dataType instanceof MapType) {
                return containsTimeStampTz(((MapType) dataType).getKeyType())
                        || containsTimeStampTz(((MapType) dataType).getValueType());
            }
            if (dataType instanceof StructType) {
                for (StructField field : ((StructType) dataType).getFields()) {
                    if (containsTimeStampTz(field.getDataType())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /** rewrite plan tree */
    public static Plan rewritePlanTree(SessionVarGuardRewriter exprRewriter, Plan plan) {
        return plan.accept(new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visit(Plan plan, Void ctx) {
                plan = super.visit(plan, ctx);
                return exprRewriter.rewritePlanNode(plan);
            }
        }, null);
    }

    /**
     * Check if current query session variables match MV creation session variables.
     * Only compares variables that affect query results. The time zone is compared with its canonical
     * identity (UTC / Etc/UTC / GMT / +00:00 are the same zone) so equivalent spellings do not cause a
     * mismatch and a resulting unnecessary rewrite loss.
     */
    public static boolean checkSessionVariablesMatch(Map<String, String> currentSessionVars,
            Map<String, String> persistSessionVars) {
        if (persistSessionVars == null || persistSessionVars.isEmpty()) {
            // If no session variables saved, consider them matched
            return true;
        }
        for (Map.Entry<String, String> entry : persistSessionVars.entrySet()) {
            String key = entry.getKey();
            String persistedValue = entry.getValue();
            String currentValue = currentSessionVars.get(key);
            if (SessionVariable.TIME_ZONE.equals(key)) {
                if (!timeZonesEquivalent(persistedValue, currentValue)) {
                    return false;
                }
            } else if (!Objects.equals(persistedValue, currentValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the guard rewriter must be applied to the object owning {@code persistSessionVars}: either
     * some persisted affectQueryResult variable no longer matches the current session, or the persisted
     * map does not carry {@code time_zone} at all. The latter covers pre-change metadata whose creation
     * time zone is unknown, so time-zone sensitive expressions of such objects must always be guarded
     * (a conservative compatibility fence) to avoid cross-zone rewrite of stale materialized values.
     */
    public static boolean needsSessionVarGuard(Map<String, String> currentSessionVars,
            Map<String, String> persistSessionVars) {
        return computeGuardMask(currentSessionVars, persistSessionVars) != GUARD_NONE;
    }

    /**
     * Whether the persisted session-variable map identifies the creation time zone.
     *
     * <p>A missing, empty or time_zone-less map means the creation zone is unknown: the object was created
     * before time_zone was persisted (old FE), or its metadata was replayed by a new FE without the key.
     * Such an object is the old-metadata -> new-FE upgrade direction of the metadata compatibility fence:
     * its zone-sensitive expressions are guarded forever and it must be recreated on the new FE before it
     * can participate in a cross-zone rewrite. The reverse direction (new metadata read by an old FE)
     * cannot be repaired from the new code: an old read-serving FE neither registers this key nor
     * classifies TIMESTAMPTZ functions, so it keeps building its nominal-mismatch cache without the time
     * zone guard. New-metadata objects therefore have to be rebuilt/recreated before an old FE serves
     * their reads, or the old FE must not be used for reads until it is upgraded.
     */
    public static boolean isCreationSessionKnown(Map<String, String> persistSessionVars) {
        return persistSessionVars != null && !persistSessionVars.isEmpty()
                && persistSessionVars.containsKey(SessionVariable.TIME_ZONE);
    }

    /**
     * Computes which session-variable dependency families differ between the query session and the
     * persisted (creation) session, as a bitmask of {@link #GUARD_TIME_ZONE} and {@link #GUARD_OTHER}.
     * Per-family scoping lets a time-zone-only difference keep rewriting expressions that depend on other
     * variables (and vice versa).
     *
     * <p>An unknown creation session (see {@link #isCreationSessionKnown}) always selects the time-zone
     * family, never an unconditional match.
     */
    public static int computeGuardMask(Map<String, String> currentSessionVars,
            Map<String, String> persistSessionVars) {
        if (planSessionVarFenceVersion(persistSessionVars)
                > SessionVariable.CURRENT_PLAN_SESSION_VAR_FENCE_VERSION) {
            // The persisted plan-variable set was written by a newer FE (metadata produced before a
            // downgrade). This FE cannot interpret it, so it must not build a rewrite cache that any
            // session could consume: select both guard families.
            return GUARD_TIME_ZONE | GUARD_OTHER;
        }
        int mask = GUARD_NONE;
        // The creation zone is unknown (pre-change metadata, including an empty map) or differs from the
        // current zone.
        if (!isCreationSessionKnown(persistSessionVars)
                || !timeZonesEquivalent(persistSessionVars.get(SessionVariable.TIME_ZONE),
                        currentSessionVars == null ? null : currentSessionVars.get(SessionVariable.TIME_ZONE))) {
            mask |= GUARD_TIME_ZONE;
        }
        if (persistSessionVars != null && !persistSessionVars.isEmpty()
                && differsInNonTimeZoneVars(currentSessionVars, persistSessionVars)) {
            mask |= GUARD_OTHER;
        }
        return mask;
    }

    /**
     * The plan-variable fence version stamped into a persisted session-variable map, 0 when the map predates
     * the stamp. A value this FE cannot parse is reported as newer than anything it knows, so the read fence
     * fails closed instead of trusting unknown metadata.
     */
    public static int planSessionVarFenceVersion(Map<String, String> persistSessionVars) {
        if (persistSessionVars == null) {
            return 0;
        }
        String version = persistSessionVars.get(SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION);
        if (version == null) {
            return 0;
        }
        try {
            return Integer.parseInt(version.trim());
        } catch (NumberFormatException e) {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * Guard mask for the rewrite cache of a synchronous materialized view.
     *
     * <p>Sync MVs keep one deliberate exception: a difference confined to the strict-table sql_mode bits
     * does not change the semantics of the rewritten expressions, so it must not disable rewrite (see
     * {@link #checkSessionVariablesMatchForSyncMv}). The exception is only consulted when the mask selects
     * the non-time-zone family on its own, and it can therefore never suppress the time-zone fence: a
     * legacy sync MV whose persisted map is empty or time_zone-less is still guarded for the time zone
     * (the full mask is returned), so an empty map is never an unconditional match and the cache cannot be
     * rewritten in a different zone.
     */
    public static int computeSyncMvGuardMask(Map<String, String> currentSessionVars,
            Map<String, String> persistSessionVars) {
        int guardMask = computeGuardMask(currentSessionVars, persistSessionVars);
        if (guardMask == GUARD_OTHER
                && checkSessionVariablesMatchForSyncMv(currentSessionVars, persistSessionVars)) {
            return GUARD_NONE;
        }
        return guardMask;
    }

    /**
     * True if a non-time_zone affectQueryResult variable differs between the two maps.
     * The union of both key sets is compared: a variable that exists in only one map is a mismatch.
     * A current-only variable (e.g. enable_decimal256, added to the persisted plan-variable set after this
     * view/MV was created) means the object was materialized with the other (default) value, so the current
     * non-default setting must disable rewrite; a persist-only variable likewise differs because its value
     * in the other map is unknown. This restores the conservative whole-map comparison semantics.
     * time_zone and the fence version stamp are excluded: the zone difference is scoped to its own family,
     * and the stamp only identifies the persisted plan-variable set (it is validated by
     * {@link #planSessionVarFenceVersion}), it is not a setting that changes an expression's value.
     */
    private static boolean differsInNonTimeZoneVars(Map<String, String> currentVars, Map<String, String> persistVars) {
        if (currentVars == null || persistVars == null) {
            return false;
        }
        for (String key : Sets.union(currentVars.keySet(), persistVars.keySet())) {
            if (SessionVariable.TIME_ZONE.equals(key)
                    || SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION.equals(key)) {
                continue;
            }
            if (!Objects.equals(persistVars.get(key), currentVars.get(key))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether two time-zone spellings denote the same zone. UTC / Etc/UTC / GMT / +00:00 are all the
     * same instant-zone and must compare equal even though their persisted strings differ.
     */
    public static boolean timeZonesEquivalent(String tzA, String tzB) {
        if (Objects.equals(tzA, tzB)) {
            return true;
        }
        if (tzA == null || tzB == null) {
            return false;
        }
        try {
            return ZoneId.of(tzA).normalized().equals(ZoneId.of(tzB).normalized());
        } catch (DateTimeException e) {
            return false;
        }
    }

    private static Map<String, String> currentAffectQueryResultInPlanVariables() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || ctx.getSessionVariable() == null) {
            return ImmutableMap.of();
        }
        return ctx.getSessionVariable().getAffectQueryResultInPlanVariables();
    }

    /** The keys that describe a result-affecting setting, i.e. the map without the fence version stamp. */
    private static Set<String> planVariableKeySet(Map<String, String> sessionVars) {
        Set<String> keys = Sets.newHashSet(sessionVars.keySet());
        keys.remove(SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION);
        return keys;
    }

    /**
     * Whether any expression owned by the plan depends on the session time zone, i.e. whether materializing or
     * rewriting this plan needs the {@code time_zone} fence.
     */
    public static boolean containsTimeZoneSensitiveExpression(Plan plan) {
        if (plan == null) {
            return false;
        }
        for (Plan node : plan.<Plan>collectToList(p -> true)) {
            for (Expression expression : node.getExpressions()) {
                if (expression.anyMatch(e -> AddSessionVarGuardRewriter.isTimeZoneSensitive((Expression) e))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Fails unless every alive frontend can interpret the persisted plan-variable set of this FE.
     *
     * <p>The {@link SessionVariable#PLAN_SESSION_VAR_FENCE_VERSION} stamp lets a new FE refuse metadata
     * written by a newer one, but the opposite direction - an <em>old</em> FE serving new metadata - can only
     * be prevented where the metadata is created: an old FE neither registers {@code time_zone} as a plan
     * variable nor classifies TIMESTAMPTZ expressions, so it would refresh and rewrite such an object without
     * the time-zone guard. Any DDL that writes semantics only a fenced FE can honour therefore has to check
     * that no older FE is still alive in the cluster (a rolling upgrade must finish first).
     *
     * <p>Development builds report {@code 0.0.0}, which carries no release ordering, so the check is a no-op
     * there; an unparsable version string is also treated as "cannot prove it is older" instead of blocking
     * DDL, because vendor version schemes are not comparable with this FE's release.
     */
    public static void checkClusterSupportsPlanTimeZoneFence() throws AnalysisException {
        Env env = Env.getCurrentEnv();
        if (env == null) {
            return;
        }
        List<Frontend> frontends = env.getFrontends(null);
        String selfHost = env.getSelfNode() == null ? null : env.getSelfNode().getHost();
        DigitalVersion selfVersion = null;
        for (Frontend frontend : frontends) {
            if (selfHost != null && selfHost.equals(frontend.getHost())) {
                selfVersion = parseReleaseVersion(frontend.getVersion());
            }
        }
        if (selfVersion == null) {
            return;
        }
        for (Frontend frontend : frontends) {
            if (!frontend.isAlive()) {
                continue;
            }
            DigitalVersion version = parseReleaseVersion(frontend.getVersion());
            if (version != null && version.before(selfVersion)) {
                throw new AnalysisException("frontend " + frontend.getHost() + " runs " + frontend.getVersion()
                        + ", which predates this frontend's " + selfVersion.major + "." + selfVersion.minor
                        + "." + selfVersion.revision + ". It cannot interpret the persisted plan variables of an "
                        + "object whose semantics depend on the session time zone, so the object cannot be "
                        + "created safely now. Upgrade all frontends first and retry.");
            }
        }
    }

    private static DigitalVersion parseReleaseVersion(String version) {
        if (version == null) {
            return null;
        }
        Matcher matcher = DORIS_RELEASE_VERSION.matcher(version);
        if (!matcher.find()) {
            return null;
        }
        try {
            DigitalVersion parsed = DigitalVersion.fromString(matcher.group(0));
            // 0.0.0 is the development build marker, which carries no release ordering.
            return parsed.id == 0 ? null : parsed;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Whether a stored column expression (a generated column or a synchronous materialized view column
     * definition, including the separately stored WHERE clause) is time-zone sensitive: its materialized
     * value depends on the session time zone because a TIMESTAMPTZ operand (possibly nested in a complex
     * type) is rendered or interpreted in that zone.
     *
     * <p>Such expressions cannot be kept consistent: they are evaluated by BE in the write/load session
     * time zone, and the persisted session variables only drive FE binding/cache decisions, so an
     * expression created in one zone and evaluated in another materializes a different value (the
     * {@code SessionVarGuardExpr} wrapper is dropped by {@code MergeGuardExpr} and by expression to
     * thrift translation before execution). They are therefore rejected at creation time instead.
     *
     * <p>Every rendering of an instant into another representation is rejected, including STRING: the
     * string embeds the write-session offset, so the same instant is stored as different strings by
     * different write sessions and any later grouping/comparison observes that difference. Comparing two
     * TIMESTAMPTZ instants and copying an instant verbatim are zone invariant and stay allowed; the
     * comparison operands are still classified individually because they may contain a sensitive cast.
     */
    public static boolean isTimeZoneSensitiveStoredExpr(Expr expr) {
        if (expr == null) {
            return false;
        }
        if (expr instanceof SlotRef || expr instanceof LiteralExpr) {
            // A bare slot/literal only carries a value; whether that value is zone dependent is decided by
            // the operation that renders or interprets it (a TIMESTAMPTZ slot holds a zone-independent
            // instant, an offset-free string slot holds whatever the storage session wrote).
            return false;
        }
        if (expr instanceof IsNullPredicate) {
            // IS (NOT) NULL itself is zone invariant, but it only reads the null-ness of its operand: a
            // nullable CASE built from a zone-sensitive expression (e.g.
            // (CASE WHEN date_trunc(ts, 'day') = ts2 THEN NULL ELSE 1 END) IS NULL) therefore materializes
            // different booleans in different write sessions, so the operand must be classified as well.
            return isTimeZoneSensitiveStoredExpr(expr.getChild(0));
        }
        if (expr instanceof CastExpr) {
            return isTimeZoneSensitiveCast((CastExpr) expr);
        }
        if (expr instanceof BinaryPredicate && isInstantComparison((BinaryPredicate) expr)) {
            // Comparing two TIMESTAMPTZ instants is zone invariant regardless of the operator, but the
            // operands may themselves be zone-sensitive expressions (e.g. date_trunc(ts, 'day') = ts2), so
            // recurse. A comparison against an offset-free literal is NOT exempt: the literal is
            // interpreted in the write/load session zone and falls through to the check below.
            return isTimeZoneSensitiveStoredExpr(expr.getChild(0))
                    || isTimeZoneSensitiveStoredExpr(expr.getChild(1));
        }
        if (expr instanceof FunctionCallExpr && ((FunctionCallExpr) expr).isAggregateFunction()) {
            // aggregates preserve the UTC instant; only a time-zone conversion inside their argument
            // (e.g. MAX(date_trunc(ts, 'day'))) makes the value zone dependent
            for (Expr child : expr.getChildren()) {
                if (isTimeZoneSensitiveStoredExpr(child)) {
                    return true;
                }
            }
            return false;
        }
        for (Expr child : expr.getChildren()) {
            if (containsTimeStampTz(child.getType())) {
                return true;
            }
            if (isTimeZoneSensitiveStoredExpr(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Classify a cast by comparing its source and result types instead of only its children: casting INTO
     * TIMESTAMPTZ interprets an offset-free source representation (string/datetime) in the session zone,
     * and casting a TIMESTAMPTZ instant OUT of TIMESTAMPTZ renders it in the session zone - including into
     * STRING, whose rendering embeds the write-session offset and is therefore not canonical.
     * TIMESTAMPTZ &lt;-&gt; TIMESTAMPTZ is instant preserving and only recurses into the source.
     */
    private static boolean isTimeZoneSensitiveCast(CastExpr castExpr) {
        Expr source = castExpr.getChild(0);
        if (isTimeZoneSensitiveTypeConversion(source.getType(), castExpr.getType())) {
            return true;
        }
        return isTimeZoneSensitiveStoredExpr(source);
    }

    /**
     * Whether converting {@code from} into {@code to} renders or interprets a TIMESTAMPTZ instant in the
     * session zone.
     *
     * <p>Array, map and struct conversions are compared leaf by leaf. A presence-only check is not enough:
     * casting {@code STRUCT&lt;a:TIMESTAMPTZ, b:TIMESTAMPTZ&gt;} to
     * {@code STRUCT&lt;a:TIMESTAMPTZ, b:STRING&gt;} keeps a TIMESTAMPTZ field in both types while the
     * {@code b} leaf is rendered in the write session zone, so the same instant would be stored with
     * different strings by different write sessions.
     */
    private static boolean isTimeZoneSensitiveTypeConversion(org.apache.doris.catalog.Type from,
            org.apache.doris.catalog.Type to) {
        if (from.isArrayType() && to.isArrayType()) {
            return isTimeZoneSensitiveTypeConversion(((org.apache.doris.catalog.ArrayType) from).getItemType(),
                    ((org.apache.doris.catalog.ArrayType) to).getItemType());
        }
        if (from.isMapType() && to.isMapType()) {
            org.apache.doris.catalog.MapType fromMap = (org.apache.doris.catalog.MapType) from;
            org.apache.doris.catalog.MapType toMap = (org.apache.doris.catalog.MapType) to;
            return isTimeZoneSensitiveTypeConversion(fromMap.getKeyType(), toMap.getKeyType())
                    || isTimeZoneSensitiveTypeConversion(fromMap.getValueType(), toMap.getValueType());
        }
        if (from.isStructType() && to.isStructType()) {
            List<org.apache.doris.catalog.StructField> fromFields =
                    ((org.apache.doris.catalog.StructType) from).getFields();
            List<org.apache.doris.catalog.StructField> toFields =
                    ((org.apache.doris.catalog.StructType) to).getFields();
            // Equal-arity struct casts are positional and allow field renames (see CheckCast), so the
            // comparison must pair the fields by position and ignore their names: CAST(STRUCT<a:TS> AS
            // STRUCT<b:TS>) only relabels the field and stays instant preserving.
            if (fromFields.size() == toFields.size()) {
                for (int i = 0; i < fromFields.size(); i++) {
                    if (isTimeZoneSensitiveTypeConversion(fromFields.get(i).getType(),
                            toFields.get(i).getType())) {
                        return true;
                    }
                }
                return false;
            }
        }
        boolean fromHasTimeStampTz = containsTimeStampTz(from);
        boolean toHasTimeStampTz = containsTimeStampTz(to);
        if (fromHasTimeStampTz != toHasTimeStampTz) {
            return true;
        }
        // Both sides carry the same presence. Only a TIMESTAMPTZ -> TIMESTAMPTZ conversion of the same shape
        // is instant preserving; a complex-to-different-complex conversion is not castable, so treating it as
        // sensitive is the safe answer.
        return fromHasTimeStampTz && !(from.isTimeStampTz() && to.isTimeStampTz());
    }

    /**
     * Whether the predicate compares two TIMESTAMPTZ-typed operands, i.e. an instant with an instant.
     * Equality and ordering of instants are zone invariant; any other binary predicate (e.g. against an
     * offset-free literal) falls through to the generic child-type check.
     */
    private static boolean isInstantComparison(BinaryPredicate binaryPredicate) {
        return containsTimeStampTz(binaryPredicate.getChild(0).getType())
                && containsTimeStampTz(binaryPredicate.getChild(1).getType());
    }

    private static boolean containsTimeStampTz(org.apache.doris.catalog.Type type) {
        if (type.isTimeStampTz()) {
            return true;
        }
        if (type.isArrayType()) {
            return containsTimeStampTz(((org.apache.doris.catalog.ArrayType) type).getItemType());
        }
        if (type.isMapType()) {
            org.apache.doris.catalog.MapType mapType = (org.apache.doris.catalog.MapType) type;
            return containsTimeStampTz(mapType.getKeyType()) || containsTimeStampTz(mapType.getValueType());
        }
        if (type.isStructType()) {
            for (org.apache.doris.catalog.StructField field
                    : ((org.apache.doris.catalog.StructType) type).getFields()) {
                if (containsTimeStampTz(field.getType())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check session-variable compatibility for synchronous materialized views.
     *
     * Differences only in STRICT_TRANS_TABLES and STRICT_ALL_TABLES do not
     * affect the SQL expressions used for synchronous MV rewriting.
     *
     * Other SQL-mode bits and result-affecting session variables
     * must still match.
     */
    public static boolean checkSessionVariablesMatchForSyncMv(
            Map<String, String> currentSessionVars,
            Map<String, String> persistSessionVars) {

        // Preserve compatibility with older MVs that have no saved session variables.
        if (persistSessionVars == null || persistSessionVars.isEmpty()) {
            return true;
        }

        // A nonempty persisted configuration cannot match missing current state.
        if (currentSessionVars == null) {
            return false;
        }

        // The SQL-mode exception may only consider the SAME set of result-affecting variables. This must be
        // checked before the exact-match shortcut below: that shortcut walks only the persisted keys, so a
        // current-only variable (e.g. enable_decimal256, added to the plan-variable set after this sync MV
        // was created) would be ignored and an older map would be exempted even though its decimal
        // expressions may have been materialized with a different precision/overflow semantics. The fence
        // version stamp is metadata rather than a setting and is compared separately, so it is excluded.
        if (!planVariableKeySet(currentSessionVars).equals(planVariableKeySet(persistSessionVars))) {
            return false;
        }

        // Retain the existing exact-match behavior whenever possible.
        if (checkSessionVariablesMatch(currentSessionVars, persistSessionVars)) {
            return true;
        }

        for (Map.Entry<String, String> entry : currentSessionVars.entrySet()) {
            String name = entry.getKey();
            String currentValue = entry.getValue();
            String persistedValue = persistSessionVars.get(name);

            if (Objects.equals(currentValue, persistedValue)) {
                continue;
            }

            // Only sql_mode is eligible for the narrowly scoped exception.
            if (!SessionVariable.SQL_MODE.equals(name)) {
                return false;
            }

            try {
                long currentMode = Long.parseLong(currentValue);
                long persistedMode = Long.parseLong(persistedValue);

                // Neither strict-table SQL-mode bit affects synchronous MV rewrite semantics.
                long ignoredModes = SqlModeHelper.MODE_STRICT_TRANS_TABLES
                        | SqlModeHelper.MODE_STRICT_ALL_TABLES;
                long relevantModes = ~ignoredModes;

                if ((currentMode & relevantModes)
                        != (persistedMode & relevantModes)) {
                    return false;
                }
            } catch (NumberFormatException e) {
                // Malformed, missing, or nonnumeric SQL modes cannot use the exception.
                return false;
            }
        }

        return true;
    }
}
