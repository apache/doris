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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Expr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.TreeNode;
import org.apache.doris.common.io.Text;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ExprStats;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Root of the expr node hierarchy.
 */
public abstract class Expr extends TreeNode<Expr> implements Cloneable, ExprStats {

    public static final String AGG_STATE_SUFFIX = "_state";
    public static final String AGG_UNION_SUFFIX = "_union";
    public static final String AGG_MERGE_SUFFIX = "_merge";
    public static final String AGG_FOREACH_SUFFIX = "_foreach";
    public static final String DEFAULT_EXPR_NAME = "expr";

    protected boolean disableTableName = false;

    // to be used where we can't come up with a better estimate
    public static final double DEFAULT_SELECTIVITY = 0.1;

    public static final float FUNCTION_CALL_COST = 10;

    protected Optional<Boolean> nullableFromNereids = Optional.empty();

    // returns true if an Expr is a non-analytic aggregate.
    private static final com.google.common.base.Predicate<Expr> IS_AGGREGATE_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr
                            && ((FunctionCallExpr) arg).isAggregateFunction();
                }
            };

    // Returns true if an Expr is an OR CompoundPredicate.
    public static final com.google.common.base.Predicate<Expr> IS_OR_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof CompoundPredicate
                            && ((CompoundPredicate) arg).getOp() == CompoundPredicate.Operator.OR;
                }
            };


    public static final com.google.common.base.Predicate<Expr> IS_EQ_BINARY_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return BinaryPredicate.getEqSlots(arg) != null;
                }
            };

    public static final com.google.common.base.Predicate<Expr> IS_NULL_LITERAL =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof NullLiteral;
                }
            };

    public void setSelectivity() {
        selectivity = -1;
    }

    // id that's unique across the entire query statement and is assigned by
    // registerConjuncts(); only assigned for the top-level terms of a
    // conjunction, and therefore null for most Exprs
    protected ExprId id;

    // true if Expr is an auxiliary predicate that was generated by the plan generation
    // process to facilitate predicate propagation;
    // false if Expr originated with a query stmt directly
    private boolean isAuxExpr = false;

    @SerializedName("type")
    protected Type type;  // result of analysis

    protected boolean isOnClauseConjunct;

    protected boolean isAnalyzed = false;  // true after analyze() has been called

    @SerializedName("opcode")
    protected TExprOpcode opcode;  // opcode for this expr

    // estimated probability of a predicate evaluating to true;
    // set during analysis;
    // between 0 and 1 if valid: invalid: -1
    protected double selectivity;

    // estimated number of distinct values produced by Expr; invalid: -1
    // set during analysis
    protected long numDistinctValues;

    protected int outputScale = -1;

    protected boolean isFilter = false;

    // The function to call. This can either be a scalar or aggregate function.
    // Set in analyze().
    protected Function fn;

    // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
    private Supplier<Boolean> isConstant = Suppliers.memoize(() -> false);

    // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
    // Needed for properly capturing expr precedences in the SQL string.
    protected boolean printSqlInParens = false;
    protected Optional<String> exprName = Optional.empty();

    protected List<TupleId> boundTupleIds = null;

    protected Expr() {
        super();
        type = Type.INVALID;
        opcode = TExprOpcode.INVALID_OPCODE;
        selectivity = -1.0;
        numDistinctValues = -1;
    }

    protected Expr(Expr other) {
        super();
        id = other.id;
        isAuxExpr = other.isAuxExpr;
        type = other.type;
        isAnalyzed = other.isAnalyzed;
        selectivity = other.selectivity;
        numDistinctValues = other.numDistinctValues;
        opcode = other.opcode;
        outputScale = other.outputScale;
        isConstant = other.isConstant;
        fn = other.fn;
        printSqlInParens = other.printSqlInParens;
        children = Expr.cloneList(other.children);
    }

    public boolean isAnalyzed() {
        return isAnalyzed;
    }

    public void checkValueValid() throws AnalysisException {
    }

    public ExprId getId() {
        return id;
    }

    public void setId(ExprId id) {
        this.id = id;
    }

    // Name of expr, this is used by generating column name automatically when there is no
    // alias or is not slotRef
    public String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(this.getClass().getSimpleName(), DEFAULT_EXPR_NAME));
        }
        return this.exprName.get();
    }

    public Type getType() {
        return type;
    }

    // add by cmy. for restoring
    public void setType(Type type) {
        this.type = type;
    }

    public TExprOpcode getOpcode() {
        return opcode;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public boolean hasSelectivity() {
        return selectivity >= 0;
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public int getOutputScale() {
        return outputScale;
    }

    public boolean isFilter() {
        return isFilter;
    }

    public boolean isOnClauseConjunct() {
        return isOnClauseConjunct;
    }

    public boolean isAuxExpr() {
        return isAuxExpr;
    }

    public void setIsAuxExpr() {
        isAuxExpr = true;
    }

    public Function getFn() {
        return fn;
    }

    public void setPrintSqlInParens(boolean b) {
        printSqlInParens = b;
    }

    /**
     * Set the expr to be analyzed and computes isConstant_.
     */
    protected void analysisDone() {
        Preconditions.checkState(!isAnalyzed);
        // We need to compute the const-ness as the last step, since analysis may change
        // the result, e.g. by resolving function.
        isConstant = Suppliers.memoize(this::isConstantImpl);
        isAnalyzed = true;
    }

    /**
     * Collects the returns types of the child nodes in an array.
     */
    protected Type[] collectChildReturnTypes() {
        Type[] childTypes = new Type[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            childTypes[i] = children.get(i).type;
        }
        return childTypes;
    }

    protected Boolean[] collectChildReturnNullables() {
        Boolean[] childNullables = new Boolean[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            childNullables[i] = children.get(i).isNullable();
        }
        return childNullables;
    }

    public List<Expr> getChildrenWithoutCast() {
        List<Expr> result = new ArrayList<>();
        for (int i = 0; i < children.size(); ++i) {
            if (children.get(i) instanceof CastExpr) {
                CastExpr castExpr = (CastExpr) children.get(i);
                result.add(castExpr.getChild(0));
            } else {
                result.add(children.get(i));
            }
        }
        return result;
    }

    public Expr getChildWithoutCast(int i) {
        Preconditions.checkArgument(i < children.size(), "child index {0} out of range {1}", i, children.size());
        Expr child = children.get(i);
        return child instanceof CastExpr ? child.children.get(0) : child;
    }

    public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (Expr expr : exprs) {
            result.add(expr.treeToThrift());
        }
        return result;
    }

    public static String debugString(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(Strings.nullToEmpty(expr.debugString()));
        }
        return "(" + Joiner.on(" ").join(strings) + ")";
    }

    /**
     * Return true if l1 equals l2 when both lists are interpreted as sets.
     */
    public static <C extends Expr> boolean equalSets(List<C> l1, List<C> l2) {
        if (l1.size() != l2.size()) {
            return false;
        }
        Map cMap1 = toCountMap(l1);
        Map cMap2 = toCountMap(l2);
        if (cMap1.size() != cMap2.size()) {
            return false;
        }
        Iterator it = cMap1.keySet().iterator();
        while (it.hasNext()) {
            C obj = (C) it.next();
            Integer count1 = (Integer) cMap1.get(obj);
            Integer count2 = (Integer) cMap2.get(obj);
            if (count2 == null || count1 != count2) {
                return false;
            }
        }
        return true;
    }

    public static <C extends Expr> HashMap<C, Integer> toCountMap(List<C> list) {
        HashMap countMap = new HashMap<C, Integer>();
        for (int i = 0; i < list.size(); i++) {
            C obj = list.get(i);
            Integer count = (Integer) countMap.get(obj);
            if (count == null) {
                countMap.put(obj, 1);
            } else {
                countMap.put(obj, count + 1);
            }
        }
        return countMap;
    }

    /**
     * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
     * elements of l.
     */
    public static <C extends Expr> ArrayList<C> cloneList(List<C> l, ExprSubstitutionMap sMap) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) element.clone(sMap));
        }
        return result;
    }

    /**
     * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
     * elements of l.
     */
    public static <C extends Expr> ArrayList<C> cloneList(List<C> l) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) element.clone());
        }
        return result;
    }

    public static <C extends Expr> ArrayList<C> cloneAndResetList(List<C> l) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) element.clone().reset());
        }
        return result;
    }

    /**
     * Collect all unique Expr nodes of type 'cl' present in 'input' and add them to
     * 'output' if they do not exist in 'output'.
     * This can't go into TreeNode<>, because we'd be using the template param
     * NodeType.
     */
    public static <C extends Expr> void collectList(List<? extends Expr> input, Class<C> cl, List<C> output) {
        Preconditions.checkNotNull(input);
        for (Expr e : input) {
            e.collect(cl, output);
        }
    }

    /**
     * get the expr which in l1 and l2 in the same time.
     * Return the intersection of l1 and l2
     */
    public static <C extends Expr> List<C> intersect(List<C> l1, List<C> l2) {
        List<C> result = new ArrayList<C>();

        for (C element : l1) {
            if (l2.contains(element)) {
                result.add(element);
            }
        }

        return result;
    }

    public static void extractSlots(Expr root, Set<SlotId> slotIdSet) {
        if (root instanceof SlotRef) {
            slotIdSet.add(((SlotRef) root).getDesc().getId());
            return;
        }
        for (Expr child : root.getChildren()) {
            extractSlots(child, slotIdSet);
        }
    }

    /**
     * Removes duplicate exprs (according to equals()).
     */
    public static <C extends Expr> void removeDuplicates(List<C> l) {
        if (l == null) {
            return;
        }
        ListIterator<C> it1 = l.listIterator();
        while (it1.hasNext()) {
            C e1 = it1.next();
            ListIterator<C> it2 = l.listIterator();
            boolean duplicate = false;
            while (it2.hasNext()) {
                C e2 = it2.next();
                if (e1 == e2) {
                    // only check up to but excluding e1
                    break;
                }
                if (e1.equals(e2)) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) {
                it1.remove();
            }
        }
    }

    public String toSql() {
        if (disableTableName) {
            return toSqlWithoutTbl();
        }
        return (printSqlInParens) ? "(" + toSqlImpl() + ")" : toSqlImpl();
    }

    public String toSql(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        return (printSqlInParens) ? "(" + toSqlImpl(disableTableName, needExternalSql, tableType, table) + ")"
                : toSqlImpl(disableTableName, needExternalSql, tableType, table);
    }

    public void disableTableName() {
        disableTableName = true;
        for (Expr child : children) {
            child.disableTableName();
        }
    }

    public String toSqlWithoutTbl() {
        return toSql(true, false, null, null);
    }

    public String toDigest() {
        return (printSqlInParens) ? "(" + toDigestImpl() + ")" : toDigestImpl();
    }

    /**
     * Returns a SQL string representing this expr. Subclasses should override this method
     * instead of toSql() to ensure that parenthesis are properly added around the toSql().
     */
    protected abstract String toSqlImpl();

    protected abstract String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table);

    /**
     * !!!!!! Important !!!!!!
     * Subclasses should override this method if
     * sql digest should be represented different from tosqlImpl().
     */
    protected String toDigestImpl() {
        return toSqlImpl();
    }

    public String toExternalSql(TableType tableType, TableIf table) {
        return toSql(false, true, tableType, table);
    }

    /**
     * Return a column label for the expression
     */
    public String toColumnLabel() {
        return toSql();
    }

    // Convert this expr, including all children, to its Thrift representation.
    public TExpr treeToThrift() {
        TExpr result = new TExpr();
        treeToThriftHelper(result);
        return result;
    }

    public void setFn(Function fn) {
        this.fn = fn;
    }

    protected void treeToThriftHelper(TExpr container) {
        treeToThriftHelper(container, ((expr, exprNode) -> expr.toThrift(exprNode)));
    }

    // Append a flattened version of this expr, including all children, to 'container'.
    protected void treeToThriftHelper(TExpr container, ExprVisitor visitor) {
        TExprNode msg = new TExprNode();
        msg.type = type.toThrift();
        msg.num_children = children.size();
        if (fn != null) {
            msg.setFn(fn.toThrift(type, collectChildReturnTypes(), collectChildReturnNullables()));
            if (fn.hasVarArgs()) {
                msg.setVarargStartIdx(fn.getNumArgs() - 1);
            }
        }
        msg.output_scale = getOutputScale();
        msg.setIsNullable(nullableFromNereids.isPresent() ? nullableFromNereids.get() : isNullable());
        visitor.visit(this, msg);
        container.addToNodes(msg);
        for (Expr child : children) {
            child.treeToThriftHelper(container, visitor);
        }
    }

    public interface ExprVisitor {
        void visit(Expr expr, TExprNode exprNode);
    }

    public static Type getAssignmentCompatibleType(List<Expr> children) {
        Type assignmentCompatibleType = Type.INVALID;
        for (int i = 0; i < children.size()
                && (assignmentCompatibleType.isDecimalV3() || assignmentCompatibleType.isDatetimeV2()
                || assignmentCompatibleType.isInvalid()); i++) {
            if (children.get(i) instanceof NullLiteral) {
                continue;
            }
            assignmentCompatibleType = assignmentCompatibleType.isInvalid() ? children.get(i).type
                    : ScalarType.getAssignmentCompatibleType(assignmentCompatibleType, children.get(i).type,
                    true, SessionVariable.getEnableDecimal256());
        }
        return assignmentCompatibleType;
    }

    // Convert this expr into msg (excluding children), which requires setting
    // msg.op as well as the expr-specific field.
    protected abstract void toThrift(TExprNode msg);

    public boolean isAggregate() {
        return IS_AGGREGATE_PREDICATE.apply(this);
    }


    public String debugString() {
        return debugString(children);
    }

    /**
     * Resets the internal state of this expr produced by analyze().
     * Only modifies this expr, and not its child exprs.
     */
    protected void resetAnalysisState() {
        isAnalyzed = false;
    }

    /**
     * Resets the internal analysis state of this expr tree. Removes implicit casts.
     */
    public Expr reset() {
        if (isImplicitCast()) {
            return getChild(0).reset();
        }
        for (int i = 0; i < children.size(); ++i) {
            children.set(i, children.get(i).reset());
        }
        resetAnalysisState();
        return this;
    }

    public static ArrayList<Expr> resetList(ArrayList<Expr> l) {
        for (int i = 0; i < l.size(); ++i) {
            l.set(i, l.get(i).reset());
        }
        return l;
    }

    /**
     * Creates a deep copy of this expr including its analysis state. The method is
     * abstract in this class to force new Exprs to implement it.
     */
    @Override
    public abstract Expr clone();

    public boolean notCheckDescIdEquals(Object obj) {
        return equals(obj);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        // don't compare type, this could be called pre-analysis
        Expr expr = (Expr) obj;
        if (children.size() != expr.children.size()) {
            return false;
        }
        for (int i = 0; i < children.size(); ++i) {
            if (!children.get(i).equals(expr.children.get(i))) {
                return false;
            }
        }
        if (fn == null && expr.fn == null) {
            return true;
        }
        if (fn == null || expr.fn == null) {
            return false;
        }
        // Both fn_'s are not null
        return fn.equals(expr.fn);
    }

    @Override
    public int hashCode() {
        // in group by clause, group by list need to remove duplicate exprs, the expr may be not not analyzed, the id
        // may be null
        if (id == null) {
            int result = 31 * Objects.hashCode(type) + Objects.hashCode(opcode);
            for (Expr child : children) {
                result = 31 * result + Objects.hashCode(child);
            }
            return result;
        }
        return id.asInt();
    }

    /**
     * Gather conjuncts from this expr and return them in a list.
     * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
     * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
     */
    public List<Expr> getConjuncts() {
        List<Expr> list = Lists.newArrayList();
        if (this instanceof CompoundPredicate
                && ((CompoundPredicate) this).getOp() == CompoundPredicate.Operator.AND) {
            // TODO: we have to convert CompoundPredicate.AND to two expr trees for
            // conjuncts because NULLs are handled differently for CompoundPredicate.AND
            // and conjunct evaluation.  This is not optimal for jitted exprs because it
            // will result in two functions instead of one. Create a new CompoundPredicate
            // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
            // instead
            list.addAll((getChild(0)).getConjuncts());
            list.addAll((getChild(1)).getConjuncts());
        } else {
            list.add(this);
        }
        return list;
    }

    /**
     * Create a deep copy of 'this'. If sMap is non-null,
     * use it to substitute 'this' or its subnodes.
     * <p/>
     * Expr subclasses that add non-value-type members must override this.
     */
    public Expr clone(ExprSubstitutionMap sMap) {
        if (sMap != null) {
            for (int i = 0; i < sMap.getLhs().size(); ++i) {
                if (this.equals(sMap.getLhs().get(i))) {
                    return sMap.getRhs().get(i).clone(null);
                }
            }
        }
        Expr result = (Expr) this.clone();
        result.children = Lists.newArrayList();
        for (Expr child : children) {
            result.children.add(((Expr) child).clone(sMap));
        }
        return result;
    }

    /**
     * Return 'this' with all sub-exprs substituted according to
     * sMap. Ids of 'this' and its children are retained.
     */
    public Expr substitute(ExprSubstitutionMap sMap) {
        Preconditions.checkNotNull(sMap);
        for (int i = 0; i < sMap.getLhs().size(); ++i) {
            if (this.equals(sMap.getLhs().get(i))) {
                Expr result = sMap.getRhs().get(i).clone(null);
                if (id != null) {
                    result.id = id;
                }
                return result;
            }
        }
        for (int i = 0; i < children.size(); ++i) {
            children.set(i, children.get(i).substitute(sMap));
        }
        return this;
    }

    /**
     * Returns true if expr is fully bound by tids, otherwise false.
     */
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        if (boundTupleIds != null && !boundTupleIds.isEmpty()) {
            return boundTupleIds.stream().anyMatch(id -> tids.contains(id));
        }
        for (Expr child : children) {
            if (!child.isBoundByTupleIds(tids)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if expr is fully bound by slotId, otherwise false.
     */
    public boolean isBound(SlotId slotId) {
        for (Expr child : children) {
            if (!child.isBound(slotId)) {
                return false;
            }
        }
        return true;
    }

    // Get all the slotRefs that are bound to the tupleIds.
    // As long as it is bound to one tuple
    // Recursively call all levels of expr
    public void getSlotRefsBoundByTupleIds(List<TupleId> tupleIds, Set<SlotRef> boundSlotRefs) {
        for (Expr child : children) {
            child.getSlotRefsBoundByTupleIds(tupleIds, boundSlotRefs);
        }
    }

    public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
        for (Expr child : children) {
            child.getIds(tupleIds, slotIds);
        }
    }

    public Map<Long, Set<String>> getTableIdToColumnNames() {
        Map<Long, Set<String>> tableIdToColumnNames = new HashMap<Long, Set<String>>();
        getTableIdToColumnNames(tableIdToColumnNames);
        return tableIdToColumnNames;
    }

    public void getTableIdToColumnNames(Map<Long, Set<String>> tableIdToColumnNames) {
        Preconditions.checkState(tableIdToColumnNames != null);
        for (Expr child : children) {
            child.getTableIdToColumnNames(tableIdToColumnNames);
        }
    }

    /**
     * @return true if this is an instance of LiteralExpr
     */
    public boolean isLiteral() {
        return this instanceof LiteralExpr;
    }

    /**
     * Returns true if this expression should be treated as constant. I.e. if the frontend
     * and backend should assume that two evaluations of the expression within a query will
     * return the same value. Examples of constant expressions include:
     * - Literal values like 1, "foo", or NULL
     * - Deterministic operators applied to constant arguments, e.g. 1 + 2, or
     *   concat("foo", "bar")
     * - Functions that should be always return the same value within a query but may
     *   return different values for different queries. E.g. now(), which we want to
     *   evaluate only once during planning.
     * May incorrectly return true if the expression is not analyzed.
     * TODO: isAnalyzed_ should be a precondition for isConstant(), since it is not always
     * possible to correctly determine const-ness before analysis (e.g. see
     * FunctionCallExpr.isConstant()).
     */
    public final boolean isConstant() {
        if (isAnalyzed) {
            return isConstant.get();
        }
        return isConstantImpl();
    }

    /**
     * Implements isConstant() - computes the value without using 'isConstant_'.
     */
    protected boolean isConstantImpl() {
        for (Expr expr : children) {
            if (!expr.isConstant()) {
                return false;
            }
        }
        return true;
    }

    protected void compactForLiteral(Type type) throws AnalysisException {
        for (Expr expr : children) {
            expr.compactForLiteral(type);
        }
    }

    /**
     * Checks validity of cast, and
     * calls uncheckedCastTo() to
     * create a cast expression that casts
     * this to a specific type.
     *
     * @param targetType type to be cast to
     * @return cast expression, or converted literal,
     * should never return null
     * @throws AnalysisException when an invalid cast is asked for, for example,
     *                           failure to convert a string literal to a date literal
     */
    public final Expr castTo(Type targetType) throws AnalysisException {
        if (this instanceof PlaceHolderExpr && this.type.isUnsupported()) {
            return this;
        }
        // If the targetType is NULL_TYPE then ignore the cast because NULL_TYPE
        // is compatible with all types and no cast is necessary.
        if (targetType.isNull()) {
            return this;
        }

        if (this.type.isAggStateType()) {
            List<Type> subTypes = ((AggStateType) targetType).getSubTypes();
            List<Boolean> subNullables = ((AggStateType) targetType).getSubTypeNullables();

            if (this instanceof FunctionCallExpr) {
                if (subTypes.size() != getChildren().size()) {
                    throw new AnalysisException("AggState's subTypes size not euqal to children number");
                }
                for (int i = 0; i < subTypes.size(); i++) {
                    setChild(i, getChild(i).castTo(subTypes.get(i)));
                    if (getChild(i).isNullable() && !subNullables.get(i)) {
                        FunctionCallExpr newChild = new FunctionCallExpr("non_nullable",
                                Lists.newArrayList(getChild(i)));
                        newChild.analyzeImplForDefaultValue(subTypes.get(i));
                        setChild(i, newChild);
                    } else if (!getChild(i).isNullable() && subNullables.get(i)) {
                        FunctionCallExpr newChild = new FunctionCallExpr("nullable", Lists.newArrayList(getChild(i)));
                        newChild.analyzeImplForDefaultValue(subTypes.get(i));
                        setChild(i, newChild);
                    }
                }
                type = targetType;
            } else {
                List<Type> selfSubTypes = ((AggStateType) type).getSubTypes();
                if (subTypes.size() != selfSubTypes.size()) {
                    throw new AnalysisException("AggState's subTypes size did not match");
                }
                for (int i = 0; i < subTypes.size(); i++) {
                    if (subTypes.get(i) != selfSubTypes.get(i)) {
                        throw new AnalysisException("AggState's subType did not match");
                    }
                }
            }
        } else {
            if (this.type.equals(targetType)) {
                return this;
            }
        }

        if (targetType.getPrimitiveType() == PrimitiveType.DECIMALV2
                && this.type.getPrimitiveType() == PrimitiveType.DECIMALV2) {
            this.type = targetType;
            return this;
        }

        if (this.type.isStringType() && targetType.isStringType()) {
            return this;
        }

        // Preconditions.checkState(PrimitiveType.isImplicitCast(type, targetType),
        // "cast %s to %s", this.type, targetType);
        // TODO(zc): use implicit cast
        if (!Type.canCastTo(this.type, targetType)) {
            throw new AnalysisException("can not cast from origin type " + this.type
                    + " to target type=" + targetType);

        }
        return uncheckedCastTo(targetType);
    }

    /**
     * Create an expression equivalent to 'this' but returning targetType;
     * possibly by inserting an implicit cast,
     * or by returning an altogether new expression
     *
     * @param targetType type to be cast to
     * @return cast expression, or converted literal,
     * should never return null
     * @throws AnalysisException when an invalid cast is asked for, for example,
     *                           failure to convert a string literal to a date literal
     */
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        return new CastExpr(targetType, this);
    }

    /**
     * Add a cast expression above child.
     * If child is a literal expression, we attempt to
     * convert the value of the child directly, and not insert a cast node.
     *
     * @param targetType type to be cast to
     * @param childIndex index of child to be cast
     */
    public void castChild(Type targetType, int childIndex) throws AnalysisException {
        Expr child = getChild(childIndex);
        Expr newChild = child.castTo(targetType);
        setChild(childIndex, newChild);
    }

    /**
     * Assuming it can cast to the targetType, use for convert literal value to
     * target type without a cast node.
     */
    public static Expr convertLiteral(Expr expr, Type targetType) throws AnalysisException {
        if (!(expr instanceof LiteralExpr)) {
            return expr;
        }
        Expr newExpr = expr.uncheckedCastTo(targetType);
        if (newExpr instanceof CastExpr) {
            return ((LiteralExpr) newExpr.getChild(0)).convertTo(targetType);
        }
        return newExpr;
    }

    /**
     * Add a cast expression above child.
     * If child is a literal expression, we attempt to
     * convert the value of the child directly, and not insert a cast node.
     *
     * @param targetType type to be cast to
     * @param childIndex index of child to be cast
     */
    public void uncheckedCastChild(Type targetType, int childIndex)
            throws AnalysisException {
        Expr child = getChild(childIndex);
        //avoid to generate Expr like cast (cast(... as date) as date)
        if (!child.getType().equals(targetType)) {
            Expr newChild = child.uncheckedCastTo(targetType);
            setChild(childIndex, newChild);
        }
    }

    /**
     * Cast the operands of a binary operation as necessary,
     * give their compatible type.
     * String literals are converted first, to enable casting of the
     * the other non-string operand.
     *
     * @param compatibleType
     * @return The possibly changed compatibleType
     * (if a string literal forced casting the other operand)
     */
    public Type castBinaryOp(Type compatibleType) throws AnalysisException {
        Preconditions.checkState(this instanceof BinaryPredicate || this instanceof ArithmeticExpr);
        Type t1 = getChild(0).getType();
        Type t2 = getChild(1).getType();
        // add operand casts
        Preconditions.checkState(compatibleType.isValid());
        if (t1.isDecimalV3() || t1.isDecimalV2()) {
            if (!t1.equals(compatibleType)) {
                castChild(compatibleType, 0);
            }
        } else if (t1.getPrimitiveType() != compatibleType.getPrimitiveType()) {
            castChild(compatibleType, 0);
        }

        if (t2.isDecimalV3() || t2.isDecimalV2()) {
            if (!t2.equals(compatibleType)) {
                castChild(compatibleType, 1);
            }
        } else if (t2.getPrimitiveType() != compatibleType.getPrimitiveType()) {
            castChild(compatibleType, 1);
        }
        return compatibleType;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("id", id).add("type", type).add("sel",
                selectivity).add("#distinct", numDistinctValues).add("scale", outputScale).toString();
    }

    /**
     * This method is mainly used to find the original column corresponding to the current expr.
     * Find the initial slotRef from the current slot ref.
     *
     * If the initial expr is not a slotRef, it returns null directly.
     * If the current slotRef comes from another expression transformation,
     *   rather than directly from another slotRef, null will also be returned.
     */
    public SlotRef getSrcSlotRef() {
        SlotRef unwrapSloRef = this.unwrapSlotRef();
        if (unwrapSloRef == null) {
            return null;
        }
        SlotDescriptor slotDescriptor = unwrapSloRef.getDesc();
        if (slotDescriptor == null) {
            return null;
        }
        List<Expr> sourceExpr = slotDescriptor.getSourceExprs();
        if (sourceExpr == null || sourceExpr.isEmpty()) {
            return unwrapSloRef;
        }
        if (sourceExpr.size() > 1) {
            return null;
        }
        return sourceExpr.get(0).getSrcSlotRef();
    }

    /**
     * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
     * Otherwise returns null.
     */
    public SlotRef unwrapSlotRef() {
        if (this instanceof SlotRef) {
            return (SlotRef) this;
        } else if (this instanceof CastExpr && getChild(0) instanceof SlotRef) {
            return (SlotRef) getChild(0);
        } else {
            return null;
        }
    }

    /**
     * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
     * Otherwise returns null.
     */
    public SlotRef unwrapSlotRef(boolean implicitOnly) {
        Expr unwrappedExpr = unwrapExpr(implicitOnly);
        if (unwrappedExpr instanceof SlotRef) {
            return (SlotRef) unwrappedExpr;
        }
        return null;
    }

    /**
     * Returns the first child if this Expr is a CastExpr. Otherwise, returns 'this'.
     */
    public Expr unwrapExpr(boolean implicitOnly) {
        if (this instanceof CastExpr
                && (!implicitOnly || ((CastExpr) this).isImplicit())) {
            return children.get(0);
        }
        return this;
    }

    /**
     * Returns the descriptor of the scan slot that directly or indirectly produces
     * the values of 'this' SlotRef. Traverses the source exprs of intermediate slot
     * descriptors to resolve materialization points (e.g., aggregations).
     * Returns null if 'e' or any source expr of 'e' is not a SlotRef or cast SlotRef.
     */
    public SlotDescriptor findSrcScanSlot() {
        SlotRef slotRef = unwrapSlotRef(false);
        if (slotRef == null) {
            return null;
        }
        SlotDescriptor slotDesc = slotRef.getDesc();
        if (slotDesc.isScanSlot()) {
            return slotDesc;
        }
        if (slotDesc.getSourceExprs().size() == 1) {
            return slotDesc.getSourceExprs().get(0).findSrcScanSlot();
        }
        // No known source expr, or there are several source exprs meaning the slot is
        // has no single source table.
        return null;
    }

    public boolean isImplicitCast() {
        return this instanceof CastExpr && ((CastExpr) this).isImplicit();
    }

    public boolean contains(Expr expr) {
        if (this.equals(expr)) {
            return true;
        }

        for (Expr child : getChildren()) {
            if (child.contains(expr)) {
                return true;
            }
        }

        return false;
    }

    public Expr findEqual(List<Expr> exprs) {
        if (exprs.isEmpty()) {
            return null;
        }
        for (Expr expr : exprs) {
            if (contains(expr)) {
                return expr;
            }
        }
        return null;
    }

    /**
     * Looks up in the catalog the builtin for 'name' and 'argTypes'.
     * Returns null if the function is not found.
     */
    public Function getBuiltinFunction(String name, Type[] argTypes, Function.CompareMode mode)
            throws AnalysisException {
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, Arrays.asList(getActualArgTypes(argTypes)), Type.INVALID, false,
                true);
        Function f = Env.getCurrentEnv().getFunction(searchDesc, mode);
        if (f != null && fnName.getFunction().equalsIgnoreCase("rand")) {
            if (this.children.size() == 1 && !(this.children.get(0) instanceof LiteralExpr)) {
                throw new AnalysisException("The param of rand function must be literal");
            }
        }
        if (f != null) {
            return f;
        }

        boolean isUnion = name.toLowerCase().endsWith(AGG_UNION_SUFFIX);
        boolean isMerge = name.toLowerCase().endsWith(AGG_MERGE_SUFFIX);
        boolean isState = name.toLowerCase().endsWith(AGG_STATE_SUFFIX);
        if (isUnion || isMerge || isState) {
            if (isUnion) {
                name = name.substring(0, name.length() - AGG_UNION_SUFFIX.length());
            }
            if (isMerge) {
                name = name.substring(0, name.length() - AGG_MERGE_SUFFIX.length());
            }
            if (isState) {
                name = name.substring(0, name.length() - AGG_STATE_SUFFIX.length());
            }

            List<Type> argList = Arrays.asList(getActualArgTypes(argTypes));
            List<Type> nestedArgList;
            if (isState) {
                nestedArgList = argList;
            } else {
                if (argList.size() != 1 || !argList.get(0).isAggStateType()) {
                    throw new AnalysisException("merge/union function must input one agg_state");
                }
                AggStateType aggState = (AggStateType) argList.get(0);
                nestedArgList = aggState.getSubTypes();
            }

            searchDesc = new Function(new FunctionName(name), nestedArgList, Type.INVALID, false, true);

            f = Env.getCurrentEnv().getFunction(searchDesc, mode);
            if (f == null || !(f instanceof AggregateFunction)) {
                return null;
            }

            if (isState) {
                f = new ScalarFunction(new FunctionName(name + AGG_STATE_SUFFIX), Arrays.asList(f.getArgs()),
                        Expr.createAggStateType(name, nestedArgList,
                                nestedArgList.stream().map(e -> true).collect(Collectors.toList())),
                        f.hasVarArgs(), f.isUserVisible());
                f.setNullableMode(NullableMode.ALWAYS_NOT_NULLABLE);
            } else {
                Function original = f;
                f = f.clone();
                f.setArgs(argList);
                if (isUnion) {
                    f.setName(new FunctionName(name + AGG_UNION_SUFFIX));
                    f.setReturnType(argList.get(0));
                    f.setNullableMode(NullableMode.ALWAYS_NOT_NULLABLE);
                }
                if (isMerge) {
                    f.setName(new FunctionName(name + AGG_MERGE_SUFFIX));
                    f.setNullableMode(NullableMode.CUSTOM);
                    f.setNestedFunction(original);
                }
            }
            f.setBinaryType(TFunctionBinaryType.AGG_STATE);
        }

        return f;
    }

    /**
     * Negates a boolean Expr.
     */
    public Expr negate() {
        Preconditions.checkState(type.equals(Type.BOOLEAN));
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, this, null);
    }

    public static void writeTo(Expr expr, DataOutput output) throws IOException {
        if (expr.supportSerializable()) {
            Text.writeString(output, GsonUtils.GSON.toJson(expr));
        } else {
            throw new IOException("Unsupported writable expr " + expr.toSql());
        }
    }

    // If this expr can serialize and deserialize.
    // If one expr NOT implement Gson annotation, must override this function by return false
    public boolean supportSerializable() {
        for (Expr child : children) {
            if (!child.supportSerializable()) {
                return false;
            }
        }
        return true;
    }


    protected void recursiveResetChildrenResult(boolean forPushDownPredicatesToView) throws AnalysisException {
        for (int i = 0; i < children.size(); i++) {
            final Expr child = children.get(i);
            final Expr newChild = child.getResultValue(forPushDownPredicatesToView);
            if (newChild != child) {
                setChild(i, newChild);
            }
        }
    }

    /**
     * For calculating expr.
     * @return value returned can't be null, if this and it's children are't constant expr, return this.
     * @throws AnalysisException
     */
    public Expr getResultValue(boolean forPushDownPredicatesToView) throws AnalysisException {
        recursiveResetChildrenResult(forPushDownPredicatesToView);
        final Expr newExpr = ExpressionFunctions.INSTANCE.evalExpr(this);
        return newExpr != null ? newExpr : this;
    }

    public String getStringValue() {
        return "";
    }

    /**
     * This method is used for constant fold of query in FE,
     * for different serde dialect(hive, presto, doris).
     */
    public String getStringValueForQuery(FormatOptions options) {
        return getStringValue();
    }

    /**
     * This method is to return the string value of this expr in a complex type for query
     * It is only used for "getStringValueForQuery()"
     * For most of the integer types, it is same as getStringValueForQuery().
     * But for others like StringLiteral and DateLiteral, it should be wrapped with quotations.
     * eg: 1,2,abc,[1,2,3],["abc","def"],{10:20},{"abc":20}
     */
    protected String getStringValueInComplexTypeForQuery(FormatOptions options) {
        return getStringValueForQuery(options);
    }

    /**
     * This method is to return the string value of this expr for stream load.
     * so there is a little different from "getStringValueForQuery()".
     * eg, for NullLiteral, it should be "\N" for stream load, but "null" for FE constant
     * for StructLiteral, the value should not contain sub column's name.
     */
    public String getStringValueForStreamLoad(FormatOptions options) {
        return getStringValueForQuery(options);
    }

    public final TExpr normalize(Normalizer normalizer) {
        TExpr result = new TExpr();
        treeToThriftHelper(result, (expr, texprNode) -> expr.normalize(texprNode, normalizer));
        return result;
    }

    protected void normalize(TExprNode msg, Normalizer normalizer) {
        this.toThrift(msg);
    }

    protected boolean hasNullableChild() {
        return hasNullableChild(children);
    }

    protected static boolean hasNullableChild(List<Expr> children) {
        for (Expr expr : children) {
            if (expr.isNullable()) {
                return true;
            }
        }
        return false;
    }

    /**
     * For excute expr the result is nullable
     * TODO: Now only SlotRef and LiteralExpr overwrite the method, each child of Expr should
     * overwrite this method to plan correct
     */
    public boolean isNullable() {
        return isNullable(fn, children);
    }

    public static boolean isNullable(Function fn, List<Expr> children) {
        if (fn == null) {
            return true;
        }
        switch (fn.getNullableMode()) {
            case DEPEND_ON_ARGUMENT:
                return hasNullableChild(children);
            case ALWAYS_NOT_NULLABLE:
                return false;
            case CUSTOM:
                return customNullableAlgorithm(fn, children);
            case ALWAYS_NULLABLE:
            default:
                return true;
        }
    }

    private static boolean customNullableAlgorithm(Function fn, List<Expr> children) {
        Preconditions.checkState(fn.getNullableMode() == Function.NullableMode.CUSTOM);

        if (fn.functionName().endsWith(AGG_MERGE_SUFFIX)) {
            AggStateType type = (AggStateType) fn.getArgs()[0];
            return isNullable(fn.getNestedFunction(), getMockedExprs(type));
        }

        if (fn.functionName().equalsIgnoreCase("if")) {
            Preconditions.checkState(children.size() == 3);
            for (int i = 1; i < children.size(); i++) {
                if (children.get(i).isNullable()) {
                    return true;
                }
            }
            return false;
        }
        if (fn.functionName().equalsIgnoreCase("ifnull") || fn.functionName().equalsIgnoreCase("nvl")) {
            Preconditions.checkState(children.size() == 2);
            if (children.get(0).isNullable()) {
                return children.get(1).isNullable();
            }
            return false;
        }
        if (fn.functionName().equalsIgnoreCase("coalesce")) {
            for (Expr expr : children) {
                if (!expr.isNullable()) {
                    return false;
                }
            }
            return true;
        }
        if (fn.functionName().equalsIgnoreCase("concat_ws")) {
            return children.get(0).isNullable();
        }
        if (fn.functionName().equalsIgnoreCase(Operator.MULTIPLY.getName())
                && fn.getReturnType().isDecimalV3()) {
            return hasNullableChild(children);
        }
        if ((fn.functionName().equalsIgnoreCase(Operator.ADD.getName())
                || fn.functionName().equalsIgnoreCase(Operator.SUBTRACT.getName()))
                && fn.getReturnType().isDecimalV3()) {
            return hasNullableChild(children);
        }
        if (fn.functionName().equalsIgnoreCase("group_concat")) {
            int size = Math.min(fn.getNumArgs(), children.size());
            for (int i = 0; i < size; ++i) {
                if (children.get(i).isNullable()) {
                    return true;
                }
            }
            return false;
        }
        if (fn.functionName().equalsIgnoreCase("array_sortby")) {
            return children.get(0).isNullable();
        }
        if (fn.functionName().equalsIgnoreCase("array_map")) {
            for (int i = 1; i < children.size(); ++i) {
                if (children.get(i).isNullable()) {
                    return true;
                }
            }
            return false;
        }
        if (fn.functionName().equalsIgnoreCase("array_contains") || fn.functionName().equalsIgnoreCase("array_position")
                || fn.functionName().equalsIgnoreCase("countequal")
                || fn.functionName().equalsIgnoreCase("map_contains_key")
                || fn.functionName().equalsIgnoreCase("map_contains_value")) {
            return children.get(0).isNullable();
        }
        return true;
    }

    public static Boolean getResultIsNullable(String name, List<Type> typeList, List<Boolean> nullableList) {
        if (name == null || typeList == null || nullableList == null) {
            return false;
        }
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, typeList, Type.INVALID, false, true);
        List<Expr> mockedExprs = getMockedExprs(typeList, nullableList);
        Function f = Env.getCurrentEnv().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return isNullable(f, mockedExprs);
    }

    public static AggStateType createAggStateType(String name, List<Type> typeList, List<Boolean> nullableList) {
        return new AggStateType(name, Expr.getResultIsNullable(name, typeList, nullableList), typeList, nullableList);
    }

    public static List<Expr> getMockedExprs(List<Type> typeList, List<Boolean> nullableList) {
        List<Expr> mockedExprs = Lists.newArrayList();
        for (int i = 0; i < typeList.size(); i++) {
            mockedExprs.add(new SlotRef(typeList.get(i), nullableList.get(i)));
        }
        return mockedExprs;
    }

    public static List<Expr> getMockedExprs(AggStateType type) {
        return getMockedExprs(type.getSubTypes(), type.getSubTypeNullables());
    }

    public void materializeSrcExpr() {
        if (this instanceof SlotRef) {
            SlotRef thisRef = (SlotRef) this;
            SlotDescriptor slotDesc = thisRef.getDesc();
            slotDesc.setIsMaterialized(true);
            slotDesc.getSourceExprs().forEach(Expr::materializeSrcExpr);
        }
        for (Expr child : children) {
            child.materializeSrcExpr();
        }
    }

    // This is only for transactional insert operation,
    // to check it the given value in insert stmt is LiteralExpr.
    // And if we write "1" to a boolean column, there will be a cast(1 as boolean) expr,
    // which is also accepted.
    public boolean isLiteralOrCastExpr() {
        if (this instanceof CastExpr) {
            return children.get(0) instanceof LiteralExpr;
        } else {
            return this instanceof LiteralExpr;
        }
    }

    public Expr replaceSubPredicate(Expr subExpr) {
        if (toSqlWithoutTbl().equals(subExpr.toSqlWithoutTbl())) {
            return null;
        }
        return this;
    }

    protected Type getActualType(Type originType) {
        if (originType == null) {
            return null;
        }
        if (originType.isScalarType()) {
            return getActualScalarType(originType);
        } else if (originType.getPrimitiveType() == PrimitiveType.ARRAY) {
            return getActualArrayType((ArrayType) originType);
        } else if (originType.getPrimitiveType().isMapType()) {
            return getActualMapType((MapType) originType);
        } else {
            return originType;
        }
    }

    protected Type getActualScalarType(Type originType) {
        if (originType.getPrimitiveType() == PrimitiveType.DECIMAL32) {
            return Type.DECIMAL32;
        } else if (originType.getPrimitiveType() == PrimitiveType.DECIMAL64) {
            return Type.DECIMAL64;
        } else if (originType.getPrimitiveType() == PrimitiveType.DECIMAL128) {
            return Type.DECIMAL128;
        } else if (originType.getPrimitiveType() == PrimitiveType.DECIMAL256) {
            return Type.DECIMAL256;
        } else if (originType.getPrimitiveType() == PrimitiveType.DATETIMEV2) {
            return Type.DATETIMEV2;
        } else if (originType.getPrimitiveType() == PrimitiveType.DATEV2) {
            return Type.DATEV2;
        } else if (originType.getPrimitiveType() == PrimitiveType.VARCHAR) {
            return Type.VARCHAR;
        } else if (originType.getPrimitiveType() == PrimitiveType.CHAR) {
            return Type.CHAR;
        } else if (originType.getPrimitiveType() == PrimitiveType.DECIMALV2) {
            return Type.MAX_DECIMALV2_TYPE;
        } else if (originType.getPrimitiveType() == PrimitiveType.TIMEV2) {
            return Type.TIMEV2;
        }
        return originType;
    }

    protected Type[] getActualArgTypes(Type[] originType) {
        return Arrays.stream(originType).map(this::getActualType).toArray(Type[]::new);
    }

    private MapType getActualMapType(MapType originMapType) {
        return new MapType(originMapType.getKeyType(), originMapType.getValueType());
    }

    private ArrayType getActualArrayType(ArrayType originArrayType) {
        return new ArrayType(getActualType(originArrayType.getItemType()));
    }

    public boolean isNullLiteral() {
        return this instanceof NullLiteral;
    }

    public void setNullableFromNereids(boolean nullable) {
        nullableFromNereids = Optional.of(nullable);
    }

    public Optional<Boolean> getNullableFromNereids() {
        return nullableFromNereids;
    }

    public void clearNullableFromNereids() {
        nullableFromNereids = Optional.empty();
    }

    public Set<SlotRef> getInputSlotRef() {
        Set<SlotRef> slots = new HashSet<>();
        if (this instanceof SlotRef) {
            slots.add((SlotRef) this);
            return slots;
        } else {
            for (Expr expr : children) {
                slots.addAll(expr.getInputSlotRef());
            }
        }
        return slots;
    }
}

