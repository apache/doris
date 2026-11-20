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
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
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
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StringRegexPredicate;
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
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders supported expressions using the ExprId reference mapping of a SQLRelation. Marker payloads are omitted
 * in favor of id-only calls. Composite expressions recurse through the visitor; a generic toSql fallback rejects
 * registered slots that require remapping.
 *
 * Subquery expressions have only a compact diagnostic digest rendering here, not executable subquery SQL. The
 * physical-plan decompiler rejects residual subquery expressions before invoking this printer.
 */
public class SPMExprSqlBuilder extends ExpressionVisitor<String, SQLRelation> {

    /**
     * The INTERNAL pseudo function names visitSubqueryExpr emits for a subquery: the
     * structural DIGEST form, never SQL.
     *
     * They are reserved by SPM. A frozen SQL must never be readable as - or shadowed by - a user
     * function of one of these names: INSUBQUERY(x)[...] is an ordinary unreserved call, so
     * a compatible user alias UDF made a frozen text validate and every replay evaluated that UDF
     * (and its array access) instead of the original IN (SELECT...). Subqueries are
     * therefore refused before decompilation and a plan calling a function of these names is not
     * freezable either (see SPMPlan2SQLBuilder.rejectSubqueryExpressions /
     * #rejectReservedInternalFunctionNames).
     */
    public static final java.util.Set<String> INTERNAL_PSEUDO_FUNCTION_NAMES =
            java.util.Set.of("insubquery", "scalarsubquery");

    /**
     * Prints a single expression.
     *
     * Parameter expr: the expression to print
     * Parameter relation: the SQLRelation carrying the columnNames mapping
     * Returns: the SQL text
     */
    public String print(Expression expr, SQLRelation relation) {
        return expr.accept(this, relation);
    }

    /**
     * Renders one function's argument list.
     *
     * Parameter aggregate: whether the arguments belong to an AGGREGATE call
     * Parameter arguments: the arguments to render
     * Parameter context: the relation of the enclosing scope
     * Returns: the comma separated argument text
     */
    private String renderArguments(boolean aggregate, List<Expression> arguments,
            SQLRelation context) {
        return arguments.stream()
                .map(argument -> renderArgument(argument, context, aggregate))
                .collect(Collectors.joining(", "));
    }

    /**
     * Renders one function argument. At an aggregate argument root, preserves a recognized LARGEINT accumulator
     * widening; other casts follow visitCast. This root-only rule avoids retaining ordinary nested coercions that
     * would pin a captured operand type.
     */
    private String renderArgument(Expression argument, SQLRelation context, boolean aggregate) {
        if (aggregate && argument instanceof Cast && preservesLoweredAccumulator((Cast) argument)) {
            Cast cast = (Cast) argument;
            return "CAST(" + cast.child().accept(this, context) + " AS "
                    + cast.getDataType().toSql() + ")";
        }
        return argument.accept(this, context);
    }

    @Override
    public String visit(Expression expr, SQLRelation context) {
        // SPM placeholder: render as the id-only function call "_spm_const_var(id)" /
        // "_spm_const_list(id)" (the value does not participate in the digest / planSql
        // serialization). The id-only form keeps the digest value-independent AND makes
        // the frozen planSql parseable at replay: the text carries the id and the
        // replay substitutes the matched expression under that id.
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
        // Default fallback: use toSql of the expression itself (no column mapping).
        // toSql bypasses the ExprId -> column mapping, so a composite shape whose
        // children need remapping (e.g. ~left.a above a join that renamed the colliding
        // a to c_N / qualified it) would freeze a stale, unresolvable column and - with
        // the default enable_spm_fallback = false - fail the rewritten query. Reject
        // such shapes so CREATE keeps the user planSql and the rewrite degrades to the
        // parameterized-tree path.
        ensureNoRemappedSlots(expr, context);
        return expr.toSql();
    }

    /**
     * Safety net of the generic toSql fallback: rejects the expression when any slot
     * below it is registered under a reference that toSql would NOT reproduce (the
     * collision-renaming / quoting / qualification case), because toSql would then emit
     * a stale or differently-shaped name. Only a byte-identical registered reference is
     * renderable through the fallback. Public for tests.
     *
     * Parameter expr: the expression about to fall back to toSql
     * Parameter context: the relation carrying the ExprId -> column-reference mapping
     * Throws UnsupportedOperationException: when a child slot needs remapping
     */
    public static void ensureNoRemappedSlots(Expression expr, SQLRelation context) {
        if (expr instanceof SlotReference) {
            SlotReference slot = (SlotReference) expr;
            String mapped = context.getColumnNames().get(slot.getExprId());
            // toSql prints the BARE slot name, so a registered reference is only safe
            // when it is byte-identical to that rendering. A special-character column
            // maps to a QUOTED reference (a-b) and a disambiguated one to a QUALIFIED
            // reference (l.x): both still "name" the column, but freezing their unquoted /
            // unqualified toSql text produces SQL that no longer resolves after reload
            // ("a-b MATCH 'foo'" parses as a subtraction; a bare x may be ambiguous).
            // Reject the generic fallback for those shapes - CREATE keeps the user planSql
            // and the rewrite degrades to the parameterized-tree path.
            if (mapped != null && !mapped.equals(slot.getName())) {
                throw new UnsupportedOperationException(
                        "SPM decompile: expression " + expr.toSql() + " needs column remapping ("
                                + mapped + " -> " + slot.getName() + ") which toSql() cannot apply");
            }
        }
        for (Expression child : expr.children()) {
            ensureNoRemappedSlots(child, context);
        }
    }

    // column references.

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

    // binary operations (arithmetic / comparison / logic).

    @Override
    public String visitComparisonPredicate(ComparisonPredicate comparisonPredicate, SQLRelation context) {
        return "(" + renderValueOperand(comparisonPredicate.left(), context)
                + " " + operatorSymbol(comparisonPredicate)
                + " " + renderValueOperand(comparisonPredicate.right(), context) + ")";
    }

    @Override
    public String visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, SQLRelation context) {
        return "(" + renderValueOperand(binaryArithmetic.left(), context)
                + " " + operatorSymbol(binaryArithmetic)
                + " " + renderValueOperand(binaryArithmetic.right(), context) + ")";
    }

    /**
     * BitNot (~x): the generic fallback would print the expression through toSql and
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
        return "(NOT (" + not.child().accept(this, context) + "))";
    }

    @Override
    public String visitIsNull(IsNull isNull, SQLRelation context) {
        return renderValueOperand(isNull.child(), context) + " IS NULL";
    }

    @Override
    public String visitIsTrue(IsTrue isTrue, SQLRelation context) {
        return renderValueOperand(isTrue.child(), context) + " IS TRUE";
    }

    @Override
    public String visitIsFalse(IsFalse isFalse, SQLRelation context) {
        return renderValueOperand(isFalse.child(), context) + " IS FALSE";
    }

    // predicates.

    @Override
    public String visitInPredicate(InPredicate inPredicate, SQLRelation context) {
        String compareExpr = renderValueOperand(inPredicate.getCompareExpr(), context);
        List<String> options = inPredicate.getOptions().stream()
                .map(o -> o.accept(this, context))
                .collect(Collectors.toList());
        // A literal-only IN option list may be stored in a set-backed collection whose
        // iteration order is NOT stable across runs / FE restarts (observed: TPCDS q75's
        // "d_year IN (2001, 2002)" decompiled as (2001, 2002) one run and (2002, 2001)
        // the next, flipping the frozen planSql and failing the.out comparison). IN
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
        String compareExpr = renderValueOperand(between.getCompareExpr(), context);
        String lower = renderValueOperand(between.getLowerBound(), context);
        String upper = renderValueOperand(between.getUpperBound(), context);
        return compareExpr + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public String visitLike(Like like, SQLRelation context) {
        String rendered = renderValueOperand(like.left(), context)
                + " LIKE " + renderValueOperand(like.right(), context);
        if (like.children().size() > 2) {
            // The third child is the ESCAPE character. Dropping it froze a three-argument
            // LIKE as a two-child predicate: parameterizing the escape literal fails
            // LIKE's literal-only legality check, so creation fell back to the raw plan and
            // persisted the ALTERED predicate - a column-pattern LIKE replayed without
            // ESCAPE can match different rows. Render the child recursively.
            rendered += " ESCAPE " + renderValueOperand(like.child(2), context);
        }
        return rendered;
    }

    /** Parentheses let a predicate occupy a value-expression position in the parser. */
    private String renderValueOperand(Expression operand, SQLRelation context) {
        if (operand instanceof Nullable) {
            return renderValueOperand(operand.child(0), context);
        }
        String rendered = operand.accept(this, context);
        if (operand instanceof IsNull || operand instanceof IsTrue || operand instanceof IsFalse
                || operand instanceof InPredicate || operand instanceof Between
                || operand instanceof StringRegexPredicate || operand instanceof Match) {
            return "(" + rendered + ")";
        }
        return rendered;
    }

    // function calls.

    @Override
    public String visitBoundFunction(BoundFunction boundFunction, SQLRelation context) {
        // The Nullable marker (nullable(x) wraps a column to make it nullable, e.g. on a
        // recursive CTE union branch) has no SQL equivalent: decompile the inner
        // expression only.
        if (boundFunction instanceof Nullable) {
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
        String args = renderArguments(boundFunction instanceof AggregateFunction,
                boundFunction.children(), context);
        return functionName(boundFunction) + "(" + args + ")";
    }

    /**
     * Renders a function name, quoting and qualifying UDF names by their recorded database. UDF names that collide
     * with scalar or list markers are rejected so the caller can retain raw plan SQL. Builtins use their function
     * name directly.
     */
    public static String functionName(Function function) {
        if (function instanceof Udf) {
            String name = function.getName();
            if (SPMFrozenTreeReplacer.CONST_VAR_FUNC.equals(name)
                    || SPMFrozenTreeReplacer.CONST_LIST_FUNC.equals(name)) {
                throw new UnsupportedOperationException("SPM decompile: the user function "
                        + name + " collides with the SPM placeholder marker; keeping the"
                        + " original planSql");
            }
            String dbName = ((Udf) function).getDbName();
            if (dbName != null && !dbName.isEmpty()) {
                // Quote each component: a legal database name like my-db is NOT a
                // parser identifier. Quoting the database component preserves the call
                // when the stored SQL is parsed again.
                return SPMPlan2SQLBuilder.quoteIdentifier(dbName) + "."
                        + SPMPlan2SQLBuilder.quoteIdentifier(name);
            }
            // a GLOBAL UDF without a database: quote the (possibly non-plain) name
            return SPMPlan2SQLBuilder.quoteIdentifier(name);
        }
        return function.getName();
    }

    @Override
    public String visitUnboundFunction(UnboundFunction unboundFunction, SQLRelation context) {
        // UnboundFunction (a raw parsed function call, e.g. substr(ca_zip, 1, 5) in a
        // WHERE clause before analysis). Render the children recursively so placeholder
        // values inside the arguments stay normalized (_spm_const_var(id)); the
        // default toSql fallback would render them with their embedded values and make
        // the digest value-dependent.
        String args = unboundFunction.children().stream()
                .map(a -> a.accept(this, context))
                .collect(Collectors.joining(", "));
        // Keep the database qualifier: db1.f(k + 1) must not freeze as unqualified
        // f(...) and resolve to db2.f when replayed under USE db2.
        String dbName = unboundFunction.getDbName();
        String qualifiedName = (dbName == null || dbName.isEmpty())
                ? unboundFunction.getName()
                : SPMPlan2SQLBuilder.quoteIdentifier(dbName) + "."
                        + SPMPlan2SQLBuilder.quoteIdentifier(unboundFunction.getName());
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
        String args = renderArguments(true, fn.children(), context);
        return functionName(fn) + "(" + distinct + args + ")";
    }

    @Override
    public String visitWindow(WindowExpression windowExpression, SQLRelation context) {
        // Hand-render the OVER clause so every slot reference (partition keys, order
        // keys, function arguments) goes through the column mapping - Doris's own
        // toSql prints raw slot names such as "GROUPING_PREFIX_s_county" or a local
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
                        + renderArguments(true, fn.children(), context) + ")";
            }
        } else if (fn instanceof BoundFunction) {
            fnSql = functionName((BoundFunction) fn) + "("
                    + renderArguments(false, fn.children(), context) + ")";
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
        // the SQL default frame (RANGE UNBOUNDED PRECEDING.. CURRENT ROW) is identical.
        //
        // LAG / LEAD are the exception: their SQL text must NOT carry a frame
        // (WindowFunctionChecker.visitLag / #visitLead REJECT an explicit one and then
        // install an INTERNAL rows frame on the window expression), so every captured
        // plan carries a frame that the user never wrote. Emitting it froze SQL whose
        // own re-analysis throws "WindowFrame for LAG must be null" - the baseline
        // could never be replayed. The user-level absence of a frame is the faithful
        // rendering.
        if (!isFrameForbiddenWindowFunction(fn)
                && !windowExpression.getOrderKeys().isEmpty()
                && windowExpression.getWindowFrame().isPresent()) {
            parts.add(windowExpression.getWindowFrame().get().computeToSql());
        }
        return sb.append(String.join(" ", parts)).append(")").toString();
    }

    /**
     * Whether the window function's SQL text must not carry an explicit frame (see
     * the frame rendering in visitWindow): LAG / LEAD only.
     */
    private static boolean isFrameForbiddenWindowFunction(Expression fn) {
        if (fn instanceof Lag || fn instanceof Lead) {
            return true;
        }
        String name = fn instanceof UnboundFunction ? ((UnboundFunction) fn).getName() : null;
        return "lag".equalsIgnoreCase(name) || "lead".equalsIgnoreCase(name);
    }

    // other common types.

    /**
     * Renders a GROUP_CONCAT / MULTI_DISTINCT_GROUP_CONCAT into the dedicated
     * GROUP_CONCAT grammar.
     *
     * Doris's analyzer admits ONE value expression next to an optional CONSTANT separator
     * (the second non-order argument must be constant - a non-constant one is rejected by
     * checkLegalityBeforeTypeCoercion), with an optional trailing ORDER BY. That covers
     * every SQL-reachable shape:
     *
     * GROUP_CONCAT(DISTINCT v ORDER BY k): the physical plan carries the execution
     *   shape MultiDistinctGroupConcat(v, ORDER BY k) (GroupConcat.mustUseMultiDistinctAgg
     *   converts it). The DISTINCT form re-analyzes to exactly that node, so it is
     *   rendered - previously it returned null and forced the whole decompile to fall
     *   back to the user-supplied plan text;
     * MULTI_DISTINCT_GROUP_CONCAT(v[, sep]): the function NAME carries the dedup
     *   contract while its isDistinct flag is false, so the DISTINCT keyword must be
     *   re-emitted - GROUP_CONCAT(v) would keep the duplicates ("1,1,2" instead of
     *   "1,2").
     *
     * Parameter fn: the aggregate function (GroupConcat or MultiDistinctGroupConcat)
     * Parameter context: the relation carrying the column mapping
     * Returns: the SQL text, or null when the shape has no faithful rendering
     */
    public String renderGroupConcat(AggregateFunction fn, SQLRelation context) {
        return renderGroupConcat(fn, context, false);
    }

    /** Renders GROUP_CONCAT with DISTINCT restored from an eliminated aggregate stage. */
    public String renderGroupConcat(AggregateFunction fn, SQLRelation context, boolean restoredDistinct) {
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
        // MultiDistinctGroupConcat is the execution shape of GROUP_CONCAT(DISTINCT...):
        // its constructor keeps isDistinct = false, but the dedup contract must survive
        // the freeze
        boolean distinct = restoredDistinct || fn.isDistinct() || fn instanceof MultiDistinctGroupConcat;
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
        if (!cast.isExplicitType() && !preservesTypeConstructorFallback(cast)) {
            // analyzer / optimizer inserted coercion CAST (type alignment, DECIMAL scale
            // adjustment for division, placeholder typing,...) - an execution
            // detail that would otherwise pin the result type/scale in the frozen planSql.
            // Strip it so the replayed SQL analyzes the user-level expression and derives the
            // same types/coercions as the original query (e.g. DECIMAL division keeps its full
            // scale: "0.0286487413" instead of the coerced "0.02864874"). Explicit user
            // CAST (isExplicitType = true) is preserved verbatim. The LOWERING's accumulator
            // widening is preserved by renderArgument, which is the only place that knows the
            // cast is the ROOT of an aggregate argument (see preservesLoweredAccumulator).
            return cast.child().accept(this, context);
        }
        return "CAST(" + cast.child().accept(this, context) + " AS " + cast.getDataType().toSql() + ")";
    }

    /**
     * Whether an ANALYZER-inserted cast is part of the frozen plan's SEMANTICS rather than an
     * execution detail that may be stripped.
     *
     * The AVG lowering (AVG_DISTINCT_TO_SUM_DIV_COUNT) rewrites avg(DISTINCT k) into
     * sum(DISTINCT CAST(k AS LARGEINT)) / count(...), and that widening IS the LARGEINT
     * accumulator AVG computes in. Stripping it made the frozen SQL re-analyze
     * SUM(DISTINCT k) with a BIGINT accumulator, so two distinct values 5e18 and 6e18 - which
     * the original AVG averages to 5.5e18 - overflowed the frozen replay's sum, even for the
     * identical SQL (the CREATE probe only RE-PLANS the text, it never executes it).
     *
     * The accumulator guarantee only exists at the ROOT of an aggregate's argument
     * (see renderArgument): an operand of ordinary arithmetic
     * (k + 9223372036854775808) carries the same shape, but there it is plain type
     * coercion - keeping it pins the CAPTURED operand type and makes a literal of another type
     * replay under the wrong coercion (the + 0.25 variant truncated to an integer).
     *
     * Only an integral WIDENING over a REAL value qualifies: a cast over a substituted
     * placeholder / literal is exactly the type pinning the stripping exists to avoid, and a
     * narrowing or same-width cast carries no accumulator guarantee.
     *
     * Parameter cast: the analyzer-inserted cast
     * Returns: true when the cast must survive into the frozen SQL
     */
    private boolean preservesLoweredAccumulator(Cast cast) {
        Expression child = cast.child();
        try {
            DataType childType = child.getDataType();
            DataType targetType = cast.getDataType();
            // LARGEINT is the SUM / AVG accumulator: the lowering of avg(DISTINCT expr) writes
            // exactly this widening, and re-deriving SUM over the SMALLER type is what
            // overflows. An ordinary arithmetic widening (INT -> BIGINT and friends) is an
            // execution detail the strip exists for, so it is NOT kept - it would only pin
            // coercions the re-analyzed user expression derives by itself.
            //
            // The child must read at least one REAL column: AVG(DISTINCT k + 1) lowers to
            // sum(CAST((k + ph) AS LARGEINT)), and skipping the cast just because a placeholder
            // sits inside the arithmetic froze SUM over BIGINT again. A child built ONLY from
            // placeholders (ph1 + ph2 widened to INT) stays stripped - there the cast pins
            // the CAPTURED literals' width on the caller's values.
            return childType.isIntegralType() && targetType instanceof LargeIntType
                    && targetType.width() > childType.width()
                    && child.containsType(SlotReference.class);
        } catch (RuntimeException e) {
            // an unbound child has no type here: keeping the strip decision is the
            // historical behavior and cannot break a bound plan
            return false;
        }
    }

    /**
     * Whether the cast IS the parser's TYPE-CONSTRUCTOR fallback.
     *
     * DATE 'x' / TIMESTAMP 'x' folds to a typed literal when x PARSES, and to
     * CAST(x AS DATE|DATETIME) when it does not (LogicalPlanBuilder.visitTypeConstructor).
     * That cast carries the user's TYPE - SELECT DATE 'not-a-date' executes as a typed DATE
     * NULL - so stripping it froze Project(ph1 AS d) and the replay returned the raw STRING
     * 'not-a-date' (the CREATE probe only re-plans the text, so it accepted that projection).
     *
     * The detector is the fallback's own cause: the literal does NOT parse as the target type.
     * An ordinary implicit coercion of a VALID literal (d = '2020-01-01' also becomes
     * CAST('2020-01-01' AS DATE)) still parses, so it is still stripped and re-derived by the
     * replay's own analysis exactly as before - which keeps the frozen text of the ordinary
     * date-filter shape unchanged.
     *
     * Parameter cast: the parser-inserted cast
     * Returns: true when the cast must survive into the frozen SQL
     */
    private static boolean preservesTypeConstructorFallback(Cast cast) {
        String text = constantText(cast.child());
        if (text == null) {
            return false;
        }
        DataType target = cast.getDataType();
        if (target == null) {
            return false;
        }
        return !parsesAs(target, text);
    }

    /**
     * Returns the string value of a string literal, unwrapping a scalar marker payload when present. Returns null
     * for other expressions. Used to recognize capture-time date and time constructor fallbacks; a replay can
     * supply a different payload value.
     */
    private static String constantText(Expression expr) {
        if (expr instanceof org.apache.doris.nereids.spm.placeholder.SpmConstVar) {
            return constantText(
                    ((org.apache.doris.nereids.spm.placeholder.SpmConstVar) expr).getValue());
        }
        if (expr instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) expr).getStringValue();
        }
        return null;
    }

    /**
     * Whether the text is a LEGAL value of the target date / time type, i.e. whether the
     * parser would have folded the constructor instead of emitting the fallback cast.
     *
     * Parameter target: the cast's target type
     * Parameter text: the literal text
     * Returns: whether the text parses as that type (true for any other type)
     */
    private static boolean parsesAs(DataType target, String text) {
        try {
            if (target instanceof DateV2Type) {
                new DateV2Literal(text);
                return true;
            }
            if (target instanceof DateType) {
                new DateLiteral(text);
                return true;
            }
            if (target instanceof DateTimeV2Type) {
                new DateTimeV2Literal(text);
                return true;
            }
            if (target instanceof DateTimeType) {
                new DateTimeLiteral(text);
                return true;
            }
        } catch (RuntimeException e) {
            return false;
        }
        return true;
    }

    @Override
    public String visitTryCast(TryCast tryCast, SQLRelation context) {
        // toSql renders a grammar-parseable type name (e.g. STRUCT<name:type>);
        // toString emits a diagnostic form (STRUCT<StructField[...]>) that cannot be
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
        // CASE WHEN... THEN... ELSE... END
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
        if (literal instanceof StringLikeLiteral) {
            // Generator arguments (LATERAL VIEW / UNNEST) are intentionally NOT
            // parameterized and reach this method as concrete literals. Their toSql
            // (StringLikeLiteral.toString) only wraps the SEMANTIC value in quotes, so
            // a delimiter such as a'b froze invalid split(t.s, 'a'b'): the stored
            // baseline then had no parseable fallback tree and stopped applying after a
            // reload. Render through the default-mode-safe quote helper (stored text is
            // always re-parsed under MODE_DEFAULT).
            return SPMPlan2SQLBuilder.quoteSqlString(((StringLikeLiteral) literal).getStringValue());
        }
        DataType type = literal.getDataType();
        if (literal.isNullLiteral()) {
            return type.isNullType() ? "NULL" : typedLiteralSql("NULL", type);
        }
        if (type.isFloatLikeType()) {
            if (!Double.isFinite(literal.getDouble())) {
                throw new UnsupportedOperationException("SPM decompile: non-finite floating-point literal");
            }
            // A bare fraction parses as DECIMAL. Casting its string representation
            // directly to the captured type also preserves negative zero.
            return typedLiteralSql(SPMPlan2SQLBuilder.quoteSqlString(literal.getStringValue()), type);
        }
        if (literal instanceof IntegerLikeLiteral) {
            if (type.width() > 1) {
                Number number = ((IntegerLikeLiteral) literal).getNumber();
                BigInteger value = number instanceof BigInteger ? (BigInteger) number
                        : BigInteger.valueOf(number.longValue());
                // Add a cast when the parser would pick a smaller signed integer type.
                if (value.bitLength() < type.width() * Byte.SIZE / 2) {
                    return typedLiteralSql(literal.toSql(), type);
                }
            }
            return literal.toSql();
        }
        if (literal instanceof DateLiteral || literal instanceof TimeV2Literal || type.isDecimalLikeType()
                || type.isUuidType() || type.isIPv4Type() || type.isIPv6Type() || type.isJsonType()) {
            // Scalar spelling alone can lose the type or even become a different expression,
            // such as an integral decimal becoming an integer or a UUID becoming subtraction.
            return typedLiteralSql(SPMPlan2SQLBuilder.quoteSqlString(literal.getStringValue()), type);
        }
        if (type.isVarBinaryType()) {
            // The hex literal retains the bytes and the default binary type.
            String hex = literal.toSql();
            return type.equals(VarBinaryType.INSTANCE) ? hex : typedLiteralSql(hex, type);
        }
        if (type.isBooleanType()) {
            return literal.toSql();
        }
        throw new UnsupportedOperationException("SPM decompile: unsupported scalar literal type " + type.toSql());
    }

    private static String typedLiteralSql(String value, DataType type) {
        String typeSql = type.toSql();
        if (type instanceof DecimalV2Type) {
            DecimalV2Type decimal = (DecimalV2Type) type;
            typeSql = "DECIMALV2(" + decimal.getPrecision() + ", " + decimal.getScale() + ")";
        }
        return "CAST(" + value + " AS " + typeSql + ")";
    }

    @Override
    public String visitArrayLiteral(ArrayLiteral literal, SQLRelation context) {
        // ARRAY(...) permits expressions, including casts on temporal elements.
        String items = literal.getValue().stream()
                .map(item -> item.accept(this, context))
                .collect(Collectors.joining(", "));
        return renderTypedContainer(literal.getDataType(), "array(" + items + ")");
    }

    @Override
    public String visitMapLiteral(MapLiteral literal, SQLRelation context) {
        List<String> arguments = new ArrayList<>();
        for (Map.Entry<Literal, Literal> entry : literal.getValue().entrySet()) {
            arguments.add(entry.getKey().accept(this, context));
            arguments.add(entry.getValue().accept(this, context));
        }
        return renderTypedContainer(literal.getDataType(), "map(" + String.join(", ", arguments) + ")");
    }

    @Override
    public String visitStructLiteral(StructLiteral literal, SQLRelation context) {
        StructType type = (StructType) literal.getDataType();
        List<String> arguments = new ArrayList<>();
        for (int i = 0; i < literal.getValue().size(); i++) {
            arguments.add(SPMPlan2SQLBuilder.quoteSqlString(type.getFields().get(i).getName()));
            arguments.add(literal.getValue().get(i).accept(this, context));
        }
        return renderTypedContainer(type, "named_struct(" + String.join(", ", arguments) + ")");
    }

    private static String renderTypedContainer(DataType type, String constructor) {
        // SQL cannot spell NULL_TYPE in a cast. Inference could narrow another field of the same container.
        if (containsNullType(type)) {
            throw new UnsupportedOperationException("SPM decompile: container type cannot be expressed: "
                    + type.toSql());
        }
        return typedLiteralSql(constructor, type);
    }

    private static boolean containsNullType(DataType type) {
        if (type instanceof ArrayType) {
            return containsNullType(((ArrayType) type).getItemType());
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return containsNullType(map.getKeyType()) || containsNullType(map.getValueType());
        }
        if (type instanceof StructType) {
            return ((StructType) type).getFields().stream().map(StructField::getDataType)
                    .anyMatch(SPMExprSqlBuilder::containsNullType);
        }
        return type.isNullType();
    }

    @Override
    public String visitSessionVarGuardExpr(SessionVarGuardExpr guard, SQLRelation context) {
        // Alias-UDF expansion can wrap an arithmetic body in SessionVarGuardExpr (the
        // function's creation variables differ from the caller's); MergeGuardExpr keeps
        // the wrapper around the body. The guard only sets session variables while its
        // child is computed, so for SQL text the CHILD must go through this mapped
        // visitor: the guard's own computeToSql/toSql emits child.toSql, which
        // bypasses the ExprId -> column mapping and could freeze an ambiguous
        // unqualified name (e.g. id over a join whose both inputs carry that name).
        return guard.child().accept(this, context);
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

    // subqueries.

    @Override
    public String visitSubqueryExpr(SubqueryExpr subqueryExpr, SQLRelation context) {
        // Compact digest rendering of subquery kind, IN compare expression and
        // filter/having predicates. It omits other plan structure and negation; the
        // full-query plan digest and tree matcher provide the matching contract.
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
