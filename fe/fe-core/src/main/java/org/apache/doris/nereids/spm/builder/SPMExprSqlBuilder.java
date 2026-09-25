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

package org.apache.doris.nereids.spm.builder;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.spm.placeholder.SPMSubquerySupport;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.IsFalse;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.IsTrue;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Expression printer of the decompiler.
 *
 * When SPMPlan2SQLBuilder decompiles a physical plan into SQL, it must print the
 * expressions in the plan (SlotReference, predicates, function calls, etc.) as SQL text.
 * This class extends ExpressionVisitor, walks the expression tree recursively, and
 * replaces each SlotReference with the normalized reference name registered in
 * SQLRelation.getColumnNames() (e.g. c_5), so planSql column references never leak
 * internal ExprIds and never become ambiguous across subqueries.
 *
 * Design principles (design doc 6.2):
 *
 * - Explicitly print the common expression types (arithmetic, comparison, logic,
 *   functions, IN, BETWEEN, CASE WHEN, ...) recursively so the column mapping reaches
 *   the leaf SlotReferences.
 * - For types not explicitly covered, fall back to expr.toSql() (no column mapping;
 *   a known simplification that later milestones can complete).
 *
 * Note: in Doris the negative forms such as IS NOT NULL, NOT IN and NOT BETWEEN are
 * represented as Not(...) wrappers (not as boolean flags on the node), so each visit
 * method only outputs the positive form and visitNot handles negation uniformly.
 */
public class SPMExprSqlBuilder extends ExpressionVisitor<String, SQLRelation> {

    /**
     * Prints a single expression.
     *
     * @param expr     the expression to print
     * @param relation the SQLRelation carrying the columnNames mapping
     * @return the SQL text
     */
    public String print(Expression expr, SQLRelation relation) {
        return expr.accept(this, relation);
    }

    @Override
    public String visit(Expression expr, SQLRelation context) {
        // SPM placeholder: render as the id-only function call "_spm_const_var(id)" /
        // "_spm_const_list(id)" (the value does not participate in the digest / planSql
        // serialization). The id-only form keeps the digest value-independent AND makes
        // the frozen planSql re-parseable at rewrite time (aligned with SR: the frozen
        // text carries the placeholder id, the actual values are substituted by id when
        // the baseline is replayed).
        if (expr instanceof org.apache.doris.nereids.spm.placeholder.SpmConstVar) {
            org.apache.doris.nereids.spm.placeholder.SpmConstVar ph =
                    (org.apache.doris.nereids.spm.placeholder.SpmConstVar) expr;
            return "_spm_const_var(" + ph.getId() + ")";
        }
        if (expr instanceof org.apache.doris.nereids.spm.placeholder.SpmConstList) {
            org.apache.doris.nereids.spm.placeholder.SpmConstList list =
                    (org.apache.doris.nereids.spm.placeholder.SpmConstList) expr;
            // IN-list placeholder: render size-independently (_spm_const_list(id)) so
            // the digest / planSql stays value AND list-length independent: a baseline
            // "IN (1, 2, 3)" matches a user "IN (10, 20)" regardless of list length.
            return "_spm_const_list(" + list.getId() + ")";
        }
        // Default fallback: use toSql() of the expression itself (no column mapping).
        // toSql() bypasses the ExprId -> column mapping, so a composite shape whose
        // children need remapping (e.g. ~left.a above a join that renamed the colliding
        // a to c_N / qualified it) would freeze a stale, unresolvable column and - with
        // the default enable_spm_fallback = false - fail the rewritten query. Reject
        // such shapes so CREATE keeps the user planSql and the rewrite degrades to the
        // parameterized-tree path.
        ensureNoRemappedSlots(expr, context);
        return expr.toSql();
    }

    /**
     * Safety net of the generic toSql() fallback: rejects the expression when any slot
     * below it is registered under a reference that no longer denotes the slot's own
     * column (the collision-renaming / qualification case), because toSql() would emit
     * the STALE column name. A registered reference that still names the column (plain
     * or qualified, optionally backtick-quoted) is renderable. Public for tests.
     *
     * @param expr    the expression about to fall back to toSql()
     * @param context the relation carrying the ExprId -> column-reference mapping
     * @throws UnsupportedOperationException when a child slot needs remapping
     */
    public static void ensureNoRemappedSlots(Expression expr, SQLRelation context) {
        if (expr instanceof SlotReference) {
            SlotReference slot = (SlotReference) expr;
            String mapped = context.getColumnNames().get(slot.getExprId());
            if (mapped != null && !isSameReferenceTail(mapped, slot.getName())) {
                throw new UnsupportedOperationException(
                        "SPM decompile: expression " + expr.toSql() + " needs column remapping ("
                                + mapped + " -> " + slot.getName() + ") which toSql() cannot apply");
            }
        }
        for (Expression child : expr.children()) {
            ensureNoRemappedSlots(child, context);
        }
    }

    /** Whether a registered reference still denotes the given column name (possibly
     *  qualified by a relation alias and/or backtick-quoted). */
    private static boolean isSameReferenceTail(String reference, String columnName) {
        int dot = reference.lastIndexOf('.');
        String tail = dot >= 0 ? reference.substring(dot + 1) : reference;
        if (tail.length() >= 2 && tail.charAt(0) == '`' && tail.charAt(tail.length() - 1) == '`') {
            tail = tail.substring(1, tail.length() - 1).replace("``", "`");
        }
        return tail.equals(columnName);
    }

    // ==================== column references ====================

    @Override
    public String visitSlotReference(SlotReference slotReference, SQLRelation context) {
        String mapped = context.getColumnNames().get(slotReference.getExprId());
        if (mapped != null) {
            return mapped;
        }
        // Doris rollup materializes "GROUPING(col)" as an internal slot named
        // GROUPING_PREFIX_<col>; when such a slot was not registered by the repeat
        // decompiler (defensive fallback), render it back to GROUPING(col).
        String name = slotReference.getName();
        if (name != null && name.startsWith("GROUPING_PREFIX_")) {
            return "GROUPING(" + name.substring("GROUPING_PREFIX_".length()) + ")";
        }
        // An unregistered slot whose name is Doris's internal mark-slot naming
        // ("$c$N", e.g. the EXISTS marker of an uncorrelated subquery folded into a
        // constant CROSS JOIN) can never be resolved in the decompiled SQL - emitting
        // the raw name would freeze a planSql that fails on replay ("Unknown column
        // '$c$1'"). Such a shape has no faithful SQL rendering here, so fail the
        // decompile: CREATE BASELINE then falls back to the user-supplied planSql
        // (which is exactly the original query for these constant boolean outputs).
        if (name != null && name.startsWith("$c$")) {
            throw new UnsupportedOperationException(
                    "SPM decompile: unresolvable internal mark slot " + name);
        }
        // Unregistered column reference (e.g. a CTE output column) degrades to the raw
        // column name
        return slotReference.toSql();
    }

    // ==================== binary operations (arithmetic / comparison / logic) ====================

    @Override
    public String visitComparisonPredicate(ComparisonPredicate comparisonPredicate, SQLRelation context) {
        return "(" + comparisonPredicate.left().accept(this, context)
                + " " + operatorSymbol(comparisonPredicate)
                + " " + comparisonPredicate.right().accept(this, context) + ")";
    }

    @Override
    public String visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, SQLRelation context) {
        return "(" + binaryArithmetic.left().accept(this, context)
                + " " + operatorSymbol(binaryArithmetic)
                + " " + binaryArithmetic.right().accept(this, context) + ")";
    }

    /**
     * BitNot (~x): the generic fallback would print the expression through toSql() and
     * bypass the column mapping; above a join whose colliding column was renamed, that
     * freezes the stale slot text. Render the child recursively instead so the mapped
     * column reference stays mapped.
     */
    @Override
    public String visitBitNot(BitNot bitNot, SQLRelation context) {
        return "~(" + bitNot.child().accept(this, context) + ")";
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicate compoundPredicate, SQLRelation context) {
        // And / Or are n-ary in Nereids: a single node can hold more than two children,
        // so child(0)/child(1) alone would drop the rest. Flatten the same-type operators
        // into a list of operands with ExpressionUtils.extract (see extractConjunction /
        // extractDisjunction) and join them with the operator symbol. extract stops at a
        // different operator type, so e.g. (a or b) and c keeps the inner Or intact.
        //
        // CompoundPredicate is not a BinaryOperator (And/Or extend CompoundPredicate
        // directly), so resolve the symbol here instead of using operatorSymbol.
        String symbol = compoundPredicate instanceof And ? "AND" : "OR";
        String joined = ExpressionUtils.extract(compoundPredicate).stream()
                .map(operand -> operand.accept(this, context))
                .collect(Collectors.joining(" " + symbol + " "));
        // AND / OR need parentheses to keep the precedence
        return "(" + joined + ")";
    }

    @Override
    public String visitNot(Not not, SQLRelation context) {
        return "NOT (" + not.child().accept(this, context) + ")";
    }

    @Override
    public String visitIsNull(IsNull isNull, SQLRelation context) {
        return isNull.child().accept(this, context) + " IS NULL";
    }

    @Override
    public String visitIsTrue(IsTrue isTrue, SQLRelation context) {
        return isTrue.child().accept(this, context) + " IS TRUE";
    }

    @Override
    public String visitIsFalse(IsFalse isFalse, SQLRelation context) {
        return isFalse.child().accept(this, context) + " IS FALSE";
    }

    // ==================== predicates ====================

    @Override
    public String visitInPredicate(InPredicate inPredicate, SQLRelation context) {
        String compareExpr = inPredicate.getCompareExpr().accept(this, context);
        List<String> options = inPredicate.getOptions().stream()
                .map(o -> o.accept(this, context))
                .collect(Collectors.toList());
        // A literal-only IN option list may be stored in a set-backed collection whose
        // iteration order is NOT stable across runs / FE restarts (observed: TPCDS q75's
        // "d_year IN (2001, 2002)" decompiled as (2001, 2002) one run and (2002, 2001)
        // the next, flipping the frozen planSql and failing the .out comparison). IN
        // semantics are order-independent, so a literal-only list is emitted in sorted
        // order to keep the decompiled planSql deterministic.
        boolean allLiterals = inPredicate.getOptions().stream()
                .allMatch(o -> o instanceof Literal);
        if (allLiterals && options.size() > 1) {
            Collections.sort(options);
        }
        return compareExpr + " IN (" + String.join(", ", options) + ")";
    }

    @Override
    public String visitBetween(Between between, SQLRelation context) {
        String compareExpr = between.getCompareExpr().accept(this, context);
        String lower = between.getLowerBound().accept(this, context);
        String upper = between.getUpperBound().accept(this, context);
        return compareExpr + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public String visitLike(Like like, SQLRelation context) {
        String rendered = like.left().accept(this, context)
                + " LIKE " + like.right().accept(this, context);
        if (like.children().size() > 2) {
            // The third child is the ESCAPE character. Dropping it froze a three-argument
            // LIKE as a two-child predicate: parameterizing the escape literal fails
            // LIKE's literal-only legality check, so creation fell back to the raw plan and
            // persisted the ALTERED predicate - a column-pattern LIKE replayed without
            // ESCAPE can match different rows. Render the child recursively.
            rendered += " ESCAPE " + like.child(2).accept(this, context);
        }
        return rendered;
    }

    // ==================== function calls ====================

    @Override
    public String visitBoundFunction(BoundFunction boundFunction, SQLRelation context) {
        // The Nullable marker (nullable(x) wraps a column to make it nullable, e.g. on a
        // recursive CTE union branch) has no SQL equivalent: decompile the inner
        // expression only.
        if (boundFunction instanceof org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable) {
            return boundFunction.child(0).accept(this, context);
        }
        if (boundFunction instanceof GroupConcat || boundFunction instanceof MultiDistinctGroupConcat) {
            String rendered = renderGroupConcat((AggregateFunction) boundFunction, context);
            if (rendered == null) {
                throw new UnsupportedOperationException(
                        "SPM decompile: group_concat shape is not supported yet");
            }
            return rendered;
        }
        String args = boundFunction.children().stream()
                .map(a -> a.accept(this, context))
                .collect(Collectors.joining(", "));
        return functionName(boundFunction) + "(" + args + ")";
    }

    /**
     * Name of a bound function call for the frozen SQL. A user-defined function keeps its
     * database qualifier (Java / Python UDF, UDAF and UDTF nodes retain dbName): a frozen
     * db1.f(k) must not resolve to db2.f(k) when the replayed SQL runs under another
     * default database. Package-visible for tests.
     */
    @VisibleForTesting
    public static String functionName(Function function) {
        if (function instanceof Udf) {
            String dbName = ((Udf) function).getDbName();
            if (dbName != null && !dbName.isEmpty()) {
                return dbName + "." + function.getName();
            }
        }
        return function.getName();
    }

    @Override
    public String visitUnboundFunction(UnboundFunction unboundFunction, SQLRelation context) {
        // UnboundFunction (a raw parsed function call, e.g. substr(ca_zip, 1, 5) in a
        // WHERE clause before analysis). Render the children recursively so placeholder
        // values inside the arguments stay normalized (_spm_const_var(id)); the
        // default toSql() fallback would render them with their embedded values and make
        // the digest value-dependent.
        String args = unboundFunction.children().stream()
                .map(a -> a.accept(this, context))
                .collect(Collectors.joining(", "));
        // Keep the database qualifier: db1.f(k + 1) must not freeze as unqualified
        // f(...) and resolve to db2.f when replayed under USE db2.
        String dbName = unboundFunction.getDbName();
        String qualifiedName = (dbName == null || dbName.isEmpty())
                ? unboundFunction.getName() : dbName + "." + unboundFunction.getName();
        return qualifiedName + "(" + args + ")";
    }

    @Override
    public String visitAggregateExpression(AggregateExpression aggregateExpression, SQLRelation context) {
        AggregateFunction fn = aggregateExpression.getFunction();
        if (fn instanceof GroupConcat || fn instanceof MultiDistinctGroupConcat) {
            String rendered = renderGroupConcat(fn, context);
            if (rendered == null) {
                throw new UnsupportedOperationException(
                        "SPM decompile: group_concat shape is not supported yet");
            }
            return rendered;
        }
        // Explicitly handle DISTINCT aggregates: count(distinct col)
        String distinct = fn.isDistinct() ? "distinct " : "";
        String args = fn.children().stream()
                .map(a -> a.accept(this, context))
                .collect(Collectors.joining(", "));
        return functionName(fn) + "(" + distinct + args + ")";
    }

    @Override
    public String visitWindow(WindowExpression windowExpression, SQLRelation context) {
        // Hand-render the OVER clause so every slot reference (partition keys, order
        // keys, function arguments) goes through the column mapping - Doris's own
        // toSql() prints raw slot names such as "GROUPING_PREFIX_s_county" or a local
        // buffer name that is not referenceable in the decompiled SQL.
        Expression fn = windowExpression.getFunction();
        String fnSql;
        if (fn instanceof AggregateFunction) {
            AggregateFunction aggFn = (AggregateFunction) fn;
            if (aggFn instanceof GroupConcat || aggFn instanceof MultiDistinctGroupConcat) {
                String rendered = renderGroupConcat(aggFn, context);
                if (rendered == null) {
                    throw new UnsupportedOperationException(
                            "SPM decompile: group_concat shape is not supported yet");
                }
                fnSql = rendered;
            } else {
                String distinct = aggFn.isDistinct() ? "DISTINCT " : "";
                fnSql = functionName(aggFn) + "(" + distinct
                        + fn.children().stream().map(c -> print(c, context))
                                .collect(Collectors.joining(", ")) + ")";
            }
        } else if (fn instanceof BoundFunction) {
            fnSql = functionName((BoundFunction) fn) + "("
                    + fn.children().stream().map(c -> print(c, context))
                            .collect(Collectors.joining(", ")) + ")";
        } else {
            fnSql = print(fn, context);
        }
        StringBuilder sb = new StringBuilder(fnSql).append(" OVER (");
        List<String> parts = new ArrayList<>();
        if (!windowExpression.getPartitionKeys().isEmpty()) {
            parts.add("PARTITION BY " + windowExpression.getPartitionKeys().stream()
                    .map(p -> print(p, context)).collect(Collectors.joining(", ")));
        }
        if (!windowExpression.getOrderKeys().isEmpty()) {
            parts.add("ORDER BY " + windowExpression.getOrderKeys().stream()
                    .map(o -> print(o.child(), context) + (o.isAsc() ? " ASC" : " DESC")
                            + (o.isNullFirst() ? " NULLS FIRST" : " NULLS LAST"))
                    .collect(Collectors.joining(", ")));
        }
        // an explicit frame is only valid together with an ORDER BY clause; without one
        // the SQL default frame (RANGE UNBOUNDED PRECEDING .. CURRENT ROW) is identical
        if (!windowExpression.getOrderKeys().isEmpty()
                && windowExpression.getWindowFrame().isPresent()) {
            parts.add(windowExpression.getWindowFrame().get().computeToSql());
        }
        return sb.append(String.join(" ", parts)).append(")").toString();
    }

    // ==================== other common types ====================

    /**
     * Renders a GROUP_CONCAT / MULTI_DISTINCT_GROUP_CONCAT into the dedicated
     * GROUP_CONCAT grammar.
     *
     * Doris's analyzer admits ONE value expression next to an optional CONSTANT separator
     * (the second non-order argument must be constant - a non-constant one is rejected by
     * checkLegalityBeforeTypeCoercion), with an optional trailing ORDER BY. That covers
     * every SQL-reachable shape:
     *
     * - GROUP_CONCAT(DISTINCT v ORDER BY k): the physical plan carries the execution
     *   shape MultiDistinctGroupConcat(v, ORDER BY k) (GroupConcat.mustUseMultiDistinctAgg
     *   converts it). The DISTINCT form re-analyzes to exactly that node, so it is
     *   rendered - previously it returned null and forced the whole decompile to fall
     *   back to the user-supplied plan text;
     * - MULTI_DISTINCT_GROUP_CONCAT(v[, sep]): the function NAME carries the dedup
     *   contract while its isDistinct() flag is false, so the DISTINCT keyword must be
     *   re-emitted - GROUP_CONCAT(v) would keep the duplicates ("1,1,2" instead of
     *   "1,2").
     *
     * @param fn      the aggregate function (GroupConcat or MultiDistinctGroupConcat)
     * @param context the relation carrying the column mapping
     * @return the SQL text, or null when the shape has no faithful rendering
     */
    public String renderGroupConcat(AggregateFunction fn, SQLRelation context) {
        List<Expression> children = fn.children();
        int firstOrder = children.size();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof OrderExpression) {
                firstOrder = i;
                break;
            }
        }
        List<Expression> values = children.subList(0, firstOrder);
        List<Expression> orders = children.subList(firstOrder, children.size());
        // the dedicated grammar carries one value expression plus an optional CONSTANT
        // separator; two NON-constant value arguments cannot be written down at all (the
        // analyzer rejects them: "requires separator must be a constant"), so such a
        // programmatically-built shape keeps failing the decompile instead of freezing
        // SQL that cannot be re-parsed
        boolean separatorArgument = values.size() == 2 && values.get(1).isConstant();
        if (values.isEmpty() || (values.size() > 1 && !separatorArgument)) {
            return null;
        }
        // MultiDistinctGroupConcat is the execution shape of GROUP_CONCAT(DISTINCT ...):
        // its constructor keeps isDistinct() = false, but the dedup contract must survive
        // the freeze
        boolean distinct = fn.isDistinct() || fn instanceof MultiDistinctGroupConcat;
        StringBuilder sb = new StringBuilder("GROUP_CONCAT(");
        if (distinct) {
            sb.append("DISTINCT ");
        }
        sb.append(print(values.get(0), context));
        if (!orders.isEmpty()) {
            // GROUP_CONCAT(DISTINCT v ORDER BY k) re-analyzes to the very
            // MultiDistinctGroupConcat(v, k) execution shape it was rendered from, so the
            // ORDER BY form is faithful; a verifier may also compare it against the
            // alternative spelling MULTI_DISTINCT_GROUP_CONCAT(v ORDER BY k)
            sb.append(" ORDER BY ");
            sb.append(orders.stream().map(order -> {
                OrderExpression orderExpression = (OrderExpression) order;
                return print(orderExpression.child(), context)
                        + (orderExpression.isAsc() ? " ASC" : " DESC")
                        + (orderExpression.isNullFirst() ? " NULLS FIRST" : " NULLS LAST");
            }).collect(Collectors.joining(", ")));
        }
        if (separatorArgument) {
            sb.append(" SEPARATOR ").append(print(values.get(1), context));
        }
        return sb.append(")").toString();
    }

    @Override
    public String visitCast(Cast cast, SQLRelation context) {
        if (!cast.isExplicitType()) {
            // analyzer / optimizer inserted coercion CAST (type alignment, DECIMAL scale
            // adjustment for division, placeholder typing, ...) - an execution detail that
            // would otherwise pin the result type/scale in the frozen planSql. Strip it so
            // the replayed SQL analyzes the user-level expression and derives the same
            // types/coercions as the original query (e.g. DECIMAL division keeps its full
            // scale: "0.0286487413" instead of the coerced "0.02864874"). Explicit user
            // CAST (isExplicitType = true) is preserved verbatim.
            return cast.child().accept(this, context);
        }
        return "CAST(" + cast.child().accept(this, context) + " AS " + cast.getDataType().toSql() + ")";
    }

    @Override
    public String visitTryCast(TryCast tryCast, SQLRelation context) {
        // toSql() renders a grammar-parseable type name (e.g. STRUCT<name:type>);
        // toString() emits a diagnostic form (STRUCT<StructField[...]>) that cannot be
        // re-parsed by the frozen SQL after a reload.
        return "TRY_CAST(" + tryCast.child().accept(this, context) + " AS "
                + tryCast.getDataType().toSql() + ")";
    }

    @Override
    public String visitOrderExpression(OrderExpression orderExpression, SQLRelation context) {
        String child = orderExpression.child().accept(this, context);
        return child + (orderExpression.isAsc() ? " ASC" : " DESC");
    }

    @Override
    public String visitAlias(Alias alias, SQLRelation context) {
        return alias.child().accept(this, context);
    }

    @Override
    public String visitCaseWhen(CaseWhen caseWhen, SQLRelation context) {
        // CASE WHEN ... THEN ... ELSE ... END
        StringBuilder sb = new StringBuilder("CASE");
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            sb.append(" WHEN ").append(whenClause.getOperand().accept(this, context))
                    .append(" THEN ").append(whenClause.getResult().accept(this, context));
        }
        if (caseWhen.getDefaultValue().isPresent()) {
            sb.append(" ELSE ").append(caseWhen.getDefaultValue().get().accept(this, context));
        }
        sb.append(" END");
        return sb.toString();
    }

    @Override
    public String visitLiteral(Literal literal, SQLRelation context) {
        return literal.toSql();
    }

    /**
     * Returns the SQL symbol of a binary operator.
     *
     * The symbol of BinaryOperator in Doris is stored in the protected field "symbol",
     * which the SPM package cannot access directly, so we match the concrete subclass
     * and return the standard SQL symbol.
     */
    private String operatorSymbol(BinaryOperator op) {
        if (op instanceof EqualTo) {
            return "=";
        } else if (op instanceof NullSafeEqual) {
            return "<=>";
        } else if (op instanceof GreaterThan) {
            return ">";
        } else if (op instanceof GreaterThanEqual) {
            return ">=";
        } else if (op instanceof LessThan) {
            return "<";
        } else if (op instanceof LessThanEqual) {
            return "<=";
        } else if (op instanceof Add) {
            return "+";
        } else if (op instanceof Subtract) {
            return "-";
        } else if (op instanceof Multiply) {
            return "*";
        } else if (op instanceof Divide) {
            return "/";
        } else if (op instanceof Mod) {
            return "%";
        } else if (op instanceof IntegralDivide) {
            return "DIV";
        } else if (op instanceof BitAnd) {
            return "&";
        } else if (op instanceof BitOr) {
            return "|";
        } else if (op instanceof BitXor) {
            return "^";
        }
        return op.toSql();
    }

    // ==================== subqueries ====================

    @Override
    public String visitSubqueryExpr(SubqueryExpr subqueryExpr, SQLRelation context) {
        // Value-independent stable rendering of a subquery: its type (with the compare
        // expression for IN subqueries) plus its filter/having predicates, whose
        // placeholders are rendered as _spm_const_var(id) so the digest stays
        // value-independent for constants inside subqueries too.
        StringBuilder sb = new StringBuilder();
        if (subqueryExpr instanceof InSubquery) {
            InSubquery in = (InSubquery) subqueryExpr;
            sb.append("INSUBQUERY(").append(in.getCompareExpr().accept(this, context)).append(")");
        } else if (subqueryExpr instanceof Exists) {
            sb.append("EXISTS");
        } else {
            sb.append("SCALARSUBQUERY");
        }
        List<Expression> predicates =
                SPMSubquerySupport.collectFilterPredicates(subqueryExpr.getQueryPlan());
        sb.append("[");
        for (int i = 0; i < predicates.size(); i++) {
            if (i > 0) {
                sb.append(" & ");
            }
            sb.append(predicates.get(i).accept(this, context));
        }
        sb.append("]");
        return sb.toString();
    }

}
