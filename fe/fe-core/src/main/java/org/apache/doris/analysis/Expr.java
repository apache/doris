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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.TreeNode;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * Root of the expr node hierarchy.
 */
abstract public class Expr extends TreeNode<Expr> implements ParseNode, Cloneable, Writable {
    private static final Logger LOG = LogManager.getLogger(Expr.class);

    // Name of the function that needs to be implemented by every Expr that
    // supports negation.
    private static final String NEGATE_FN = "negate";

    // to be used where we can't come up with a better estimate
    public static final double DEFAULT_SELECTIVITY = 0.1;

    public final static float FUNCTION_CALL_COST = 10;

    // returns true if an Expr is a non-analytic aggregate.
    private final static com.google.common.base.Predicate<Expr> IS_AGGREGATE_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr &&
                            ((FunctionCallExpr)arg).isAggregateFunction();
                }
            };

    // Returns true if an Expr is a NOT CompoundPredicate.
    public final static com.google.common.base.Predicate<Expr> IS_NOT_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof CompoundPredicate &&
                            ((CompoundPredicate)arg).getOp() == CompoundPredicate.Operator.NOT;
                }
            };

    // Returns true if an Expr is an OR CompoundPredicate.
    public final static com.google.common.base.Predicate<Expr> IS_OR_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof CompoundPredicate &&
                            ((CompoundPredicate)arg).getOp() == CompoundPredicate.Operator.OR;
                }
            };

    // Returns true if an Expr is a scalar subquery
    public final static com.google.common.base.Predicate<Expr> IS_SCALAR_SUBQUERY =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg.isScalarSubquery();
                }
            };

    // Returns true if an Expr is an aggregate function that returns non-null on
    // an empty set (e.g. count).
    public final static com.google.common.base.Predicate<Expr>
            NON_NULL_EMPTY_AGG = new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
            return arg instanceof FunctionCallExpr &&
                    ((FunctionCallExpr)arg).returnsNonNullOnEmpty();
        }
    };

    /* TODO(zc)
    // Returns true if an Expr is a builtin aggregate function.
    public final static com.google.common.base.Predicate<Expr> IS_BUILTIN_AGG_FN =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr &&
                            ((FunctionCallExpr)arg).getFnName().isBuiltin();
                }
            };
    */
    // Returns true if an Expr is a builtin aggregate function.
    public final static com.google.common.base.Predicate<Expr> CORRELATED_SUBQUERY_SUPPORT_AGG_FN =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    if (arg instanceof FunctionCallExpr) {
                        String fnName = ((FunctionCallExpr)arg).getFnName().getFunction();
                        return  (fnName.equalsIgnoreCase("sum")
                                || fnName.equalsIgnoreCase("max")
                                || fnName.equalsIgnoreCase("min")
                                || fnName.equalsIgnoreCase("avg")
                                || fnName.equalsIgnoreCase(FunctionSet.COUNT));
                    } else {
                        return false;
                    }
                }
            };


    public final static com.google.common.base.Predicate<Expr> IS_TRUE_LITERAL =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof BoolLiteral && ((BoolLiteral)arg).getValue();
                }
            };

    public final static com.google.common.base.Predicate<Expr> IS_FALSE_LITERAL =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof BoolLiteral && !((BoolLiteral)arg).getValue();
                }
            };

    public final static com.google.common.base.Predicate<Expr> IS_EQ_BINARY_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) { return BinaryPredicate.getEqSlots(arg) != null; }
            };

    public final static com.google.common.base.Predicate<Expr> IS_BINARY_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) { return arg instanceof BinaryPredicate; }
            };

    public static final com.google.common.base.Predicate<Expr> IS_NULL_LITERAL =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) { return arg instanceof NullLiteral; }
            };

    public void setSelectivity() {
        selectivity = -1;
    }

    /* TODO(zc)
    public final static com.google.common.base.Predicate<Expr>
            IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr
                            && ((FunctionCallExpr) arg).isNondeterministicBuiltinFn();
                }
            };

    public final static com.google.common.base.Predicate<Expr> IS_UDF_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr
                            && !((FunctionCallExpr) arg).getFnName().isBuiltin();
                }
            };
    */

    // id that's unique across the entire query statement and is assigned by
    // Analyzer.registerConjuncts(); only assigned for the top-level terms of a
    // conjunction, and therefore null for most Exprs
    protected ExprId id;

    // true if Expr is an auxiliary predicate that was generated by the plan generation
    // process to facilitate predicate propagation;
    // false if Expr originated with a query stmt directly
    private boolean isAuxExpr = false;

    protected Type type;  // result of analysis

    protected boolean isOnClauseConjunct_; // set by analyzer

    protected boolean isAnalyzed = false;  // true after analyze() has been called

    protected TExprOpcode opcode;  // opcode for this expr
    protected TExprOpcode vectorOpcode;  // vector opcode for this expr

    // estimated probability of a predicate evaluating to true;
    // set during analysis;
    // between 0 and 1 if valid: invalid: -1
    protected double selectivity;

    // estimated number of distinct values produced by Expr; invalid: -1
    // set during analysis
    protected long numDistinctValues;

    protected int outputScale = -1;

    protected int outputColumn = -1;

    protected boolean isFilter = false;

    // The function to call. This can either be a scalar or aggregate function.
    // Set in analyze().
    protected Function fn;

    // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
    private boolean isConstant_;

    // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
    // Needed for properly capturing expr precedences in the SQL string.
    protected boolean printSqlInParens = false;

    protected Expr() {
        super();
        type = Type.INVALID;
        opcode = TExprOpcode.INVALID_OPCODE;
        vectorOpcode = TExprOpcode.INVALID_OPCODE;
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
        isConstant_ = other.isConstant_;
        fn = other.fn;
        printSqlInParens = other.printSqlInParens;
        children = Expr.cloneList(other.children);
    }

    public boolean isAnalyzed() {
        return isAnalyzed;
    }

    public void checkValueValid() throws AnalysisException {}

    public ExprId getId() {
        return id;
    }

    public void setId(ExprId id) {
        this.id = id;
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

    public boolean hasSelectivity() { return selectivity >= 0; }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public int getOutputScale() {
        return outputScale;
    }

    public void setOutputScale(int scale) {
        if (scale > 0 && scale < 10) {
            outputScale = scale;
        }
    }

    public int getOutputColumn() {
        return outputColumn;
    }

    public boolean isFilter() {
        return isFilter;
    }

    public void setIsFilter(boolean v) {
        isFilter = v;
    }

    public boolean isOnClauseConjunct() { return isOnClauseConjunct_; }
    public void setIsOnClauseConjunct(boolean b) { isOnClauseConjunct_ = b; }
    public boolean isAuxExpr() { return isAuxExpr; }
    public void setIsAuxExpr() { isAuxExpr = true; }
    public Function getFn() {
        return fn;
    }

    public boolean getPrintSqlInParens() {
        return printSqlInParens;
    }

    public void setPrintSqlInParens(boolean b) {
        printSqlInParens = b;
    }

    /**
     * Perform semantic analysis of node and all of its children.
     * Throws exception if any errors found.
     */
    public final void analyze(Analyzer analyzer) throws AnalysisException {
        if (isAnalyzed()) return;

        // Check the expr child limit.
        if (children.size() > Config.expr_children_limit) {
            throw new AnalysisException(String.format("Exceeded the maximum number of child " +
                    "expressions (%d).", Config.expr_children_limit));
        }

        // analyzer may be null for certain literal constructions (e.g. IntLiteral).
        if (analyzer != null) {
            analyzer.incrementCallDepth();
            // Check the expr depth limit. Do not print the toSql() to not overflow the stack.
            if (analyzer.getCallDepth() > Config.expr_depth_limit) {
                throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
                        "expression tree (%s).", Config.expr_depth_limit));
            }
        } else {
            throw new AnalysisException("analyzer is null.");
        }

        for (Expr child: children) {
            child.analyze(analyzer);
        }
        if (analyzer != null) analyzer.decrementCallDepth();
        computeNumDistinctValues();

        // Do all the analysis for the expr subclass before marking the Expr analyzed.
        analyzeImpl(analyzer);
        if (analyzer.safeIsEnableJoinReorderBasedCost()) {
            setSelectivity();
        }
        analysisDone();
    }

    /**
     * Does subclass-specific analysis. Subclasses should override analyzeImpl().
     */
    abstract protected void analyzeImpl(Analyzer analyzer) throws AnalysisException;

    /**
     * Set the expr to be analyzed and computes isConstant_.
     */
    protected void analysisDone() {
        Preconditions.checkState(!isAnalyzed);
        // We need to compute the const-ness as the last step, since analysis may change
        // the result, e.g. by resolving function.
        isConstant_ = isConstantImpl();
        isAnalyzed = true;
    }

    protected void computeNumDistinctValues() {
        if (isConstant()) {
            numDistinctValues = 1;
        } else {
            // if this Expr contains slotrefs, we estimate the # of distinct values
            // to be the maximum such number for any of the slotrefs;
            // the subclass analyze() function may well want to override this, if it
            // knows better
            List<SlotRef> slotRefs = Lists.newArrayList();
            this.collect(SlotRef.class, slotRefs);
            numDistinctValues = -1;
            for (SlotRef slotRef: slotRefs) {
                numDistinctValues = Math.max(numDistinctValues, slotRef.numDistinctValues);
            }
        }
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

    /**
     * Helper function: analyze list of exprs
     *
     * @param exprs
     * @param analyzer
     * @throws AnalysisException
     */
    public static void analyze(List<? extends Expr> exprs, Analyzer analyzer) throws AnalysisException {
        for (Expr expr : exprs) {
            expr.analyze(analyzer);
        }
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
     * Return true if l1[i].equals(l2[i]) for all i.
     */
    public static <C extends Expr> boolean equalLists(List<C> l1, List<C> l2) {
        if (l1.size() != l2.size()) {
            return false;
        }
        Iterator<C> l1Iter = l1.iterator();
        Iterator<C> l2Iter = l2.iterator();
        while (l1Iter.hasNext()) {
            if (!l1Iter.next().equals(l2Iter.next())) {
                return false;
            }
        }
        return true;
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
        HashMap countMap = new HashMap<C,Integer>();
        for (int i = 0; i < list.size(); i++) {
            C obj = list.get(i);
            Integer count = (Integer) countMap.get(obj);
            if (count == null) {
                countMap.put(obj, 1);
            } else {
                countMap.put(obj, count+1);
            }
        }
        return countMap;
    }

    public void analyzeNoThrow(Analyzer analyzer) {
        try {
            analyze(analyzer);
        } catch (AnalysisException e) {
            throw new IllegalStateException(e);
        }
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
    public static <C extends Expr> void collectList(List<? extends Expr> input, Class<C> cl,
      List<C> output) {
        Preconditions.checkNotNull(input);
        for (Expr e : input) {
            e.collect(cl, output);
        }
    }

    public static <C extends Expr> void collectAggregateExprs(List<? extends Expr> input,
      List<C> output) {
        Preconditions.checkNotNull(input);
        for (Expr e : input) {
            e.collectAggregateExprs(output);
        }
    }

    /**
     * get the expr which in l1 and l2 in the same time.
     * Return the intersection of l1 and l2
     */
    public static <C extends Expr> List<C> intersect(List<C> l1, List<C> l2) {
        List<C> result = new ArrayList<C>();

        for (C element: l1) {
            if (l2.contains(element)) {
                result.add(element);
            }
        }

        return result;
    }

    /**
     * Compute the intersection of l1 and l2, given the smap, and
     * return the intersecting l1 elements in i1 and the intersecting l2 elements in i2.
     * @throws AnalysisException
     */
    public static void intersect(Analyzer analyzer,
            List<Expr> l1, List<Expr> l2, ExprSubstitutionMap smap,
            List<Expr> i1, List<Expr> i2) throws AnalysisException {
        i1.clear();
        i2.clear();
        List<Expr> s1List = Expr.trySubstituteList(l1, smap, analyzer, false);
        Preconditions.checkState(s1List.size() == l1.size());
        List<Expr> s2List = Expr.trySubstituteList(l2, smap, analyzer, false);
        Preconditions.checkState(s2List.size() == l2.size());

        for (int i = 0; i < s1List.size(); ++i) {
            Expr s1 = s1List.get(i);

            for (int j = 0; j < s2List.size(); ++j) {
                Expr s2 = s2List.get(j);

                if (s1.equals(s2)) {
                    i1.add(l1.get(i));
                    i2.add(l2.get(j));
                    break;
                }
            }
        }
    }
    /**
     * Returns true if the list contains an aggregate expr.
     */
    public static <C extends Expr> boolean containsAggregate(List<? extends Expr> input) {
        for (Expr e : input) {
            if (e.containsAggregate()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an analyzed clone of 'this' with exprs substituted according to smap.
     * Removes implicit casts and analysis state while cloning/substituting exprs within
     * this tree, such that the returned result has minimal implicit casts and types.
     * Throws if analyzing the post-substitution expr tree failed.
     * If smap is null, this function is equivalent to clone().
     * If preserveRootType is true, the resulting expr tree will be cast if necessary to
     * the type of 'this'.
     */
    public Expr trySubstitute(ExprSubstitutionMap smap, Analyzer analyzer,
                              boolean preserveRootType) throws AnalysisException {
        Expr result = clone();
        // Return clone to avoid removing casts.
        if (smap == null) return result;
        result = result.substituteImpl(smap, analyzer);
        result.analyze(analyzer);
        if (preserveRootType && !type.equals(result.getType())) result = result.castTo(type);
        return result;
    }

    /**
     * Returns an analyzed clone of 'this' with exprs substituted according to smap.
     * Removes implicit casts and analysis state while cloning/substituting exprs within
     * this tree, such that the returned result has minimal implicit casts and types.
     * Expects the analysis of the post-substitution expr to succeed.
     * If smap is null, this function is equivalent to clone().
     * If preserveRootType is true, the resulting expr tree will be cast if necessary to
     * the type of 'this'.
     *
     * @throws AnalysisException
     */
    public Expr substitute(ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootType)
            throws AnalysisException {
        try {
            return trySubstitute(smap, analyzer, preserveRootType);
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed analysis after expr substitution.", e);
        }
    }

    public static ArrayList<Expr> trySubstituteList(Iterable <? extends Expr > exprs,
            ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootTypes)
            throws AnalysisException {
        if (exprs == null) {
            return null;
        }
        ArrayList<Expr> result = new ArrayList<Expr>();
        for (Expr e: exprs) {
            result.add(e.trySubstitute(smap, analyzer, preserveRootTypes));
        }
        return result;
    }

    public static ArrayList<Expr> substituteList(
            Iterable<? extends Expr> exprs,
            ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootTypes) {
        try {
            return trySubstituteList(exprs, smap, analyzer, preserveRootTypes);
        } catch (Exception e) {
            throw new IllegalStateException("Failed analysis after expr substitution.", e);
        }
    }

    /**
     * Recursive method that performs the actual substitution for try/substitute() while
     * removing implicit casts. Resets the analysis state in all non-SlotRef expressions.
     * Exprs that have non-child exprs which should be affected by substitutions must
     * override this method and apply the substitution to such exprs as well.
     */
    protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer)
            throws AnalysisException {
        if (isImplicitCast()) return getChild(0).substituteImpl(smap, analyzer);
        if (smap != null) {
            Expr substExpr = smap.get(this);
            if (substExpr != null) return substExpr.clone();
        }
        for (int i = 0; i < children.size(); ++i) {
            children.set(i, children.get(i).substituteImpl(smap, analyzer));
        }
        // SlotRefs must remain analyzed to support substitution across query blocks. All
        // other exprs must be analyzed again after the substitution to add implicit casts
        // and for resolving their correct function signature.
        if (!(this instanceof SlotRef)) resetAnalysisState();
        return this;
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

    public static boolean isBound(List<? extends Expr> exprs, List<TupleId> tids) {
        for (Expr expr : exprs) {
            if (!expr.isBoundByTupleIds(tids)) {
                return false;
            }
        }
        return true;
    }

    public static <C extends Expr> void getIds(List<? extends Expr> exprs, List<TupleId> tupleIds,
      List<SlotId> slotIds) {
        if (exprs == null) {
            return;
        }
        for (Expr e : exprs) {
            e.getIds(tupleIds, slotIds);
        }
    }

    public void markAgg() {
        for (Expr child : children) {
            child.markAgg();
        }
    }
    /**
     * Returns the product of the given exprs' number of distinct values or -1 if any of
     * the exprs have an invalid number of distinct values.
     */
    public static long getNumDistinctValues(List<Expr> exprs) {
      if (exprs == null || exprs.isEmpty()) return 0;
      long numDistinctValues = 1;
      for (Expr expr: exprs) {
        if (expr.getNumDistinctValues() == -1) {
          numDistinctValues = -1;
          break;
        }
        numDistinctValues *= expr.getNumDistinctValues();
      }
      return numDistinctValues;
    }

    public void vectorizedAnalyze(Analyzer analyzer) {
        for (Expr child: children) {
            child.vectorizedAnalyze(analyzer);
        }
    }

    public void computeOutputColumn(Analyzer analyzer) {
        for (Expr child : children) {
            child.computeOutputColumn(analyzer);
            LOG.info("child " + child.debugString() + " outputColumn: " + child.getOutputColumn());
        }

        if (!isConstant() && !isFilter) {
            List<TupleId> tupleIds = Lists.newArrayList();
            getIds(tupleIds, null);
            Preconditions.checkArgument(tupleIds.size() == 1);

            //List<Expr> reuseExprs = analyzer.getBufferReuseConjuncts(tupleIds.get(0));
            //for (Expr child : children) {
                //if (child instanceof SlotRef) {
                    //if (!((SlotRef) child).getDesc().isMultiRef()) {
                        //LOG.debug("add " + child.debugString() + " to reuse exprs.");
                        //reuseExprs.add(child);
                    //}
                //} else {
                    //LOG.debug("add " + child.debugString() + " to reuse exprs.");
                    //reuseExprs.add(child);
                //}
            //}

            //for (Expr reuseExpr : reuseExprs) {
                //if (reuseExpr.getType() == PrimitiveType.getAssignmentCompatibleType(getType(),
                  //reuseExpr.getType())) {
                    //LOG.debug(
                      //"reuse " + reuseExpr.debugString() + " buffer for " + this.debugString());
                    //outputColumn = reuseExpr.getOutputColumn();
                    //Preconditions.checkArgument(outputColumn >= 0);
                    //reuseExprs.remove(reuseExpr);
                    //return;
                //}
            //}

            int currentOutputColumn = analyzer.getCurrentOutputColumn(tupleIds.get(0));
            this.outputColumn = currentOutputColumn;
            LOG.info(debugString() + " outputColumn: " + this.outputColumn);
            ++currentOutputColumn;
            analyzer.setCurrentOutputColumn(tupleIds.get(0), currentOutputColumn);
        }
    }

    public String toSql() {
        return (printSqlInParens) ? "(" + toSqlImpl() + ")" : toSqlImpl();
    }

    /**
     * Returns a SQL string representing this expr. Subclasses should override this method
     * instead of toSql() to ensure that parenthesis are properly added around the toSql().
     */
    protected abstract String toSqlImpl();

    public String toMySql() {
        return toSql();
    }

    /**
     * Return a column label for the expression
     */
    public String toColumnLabel() {
        return toSql();
    }

    // Convert this expr, including all children, to its Thrift representation.
    public TExpr treeToThrift() {
        if (type.isNull()) {
            // Hack to ensure BE never sees TYPE_NULL. If an expr makes it this far without
            // being cast to a non-NULL type, the type doesn't matter and we can cast it
            // arbitrarily.
            Preconditions.checkState(this instanceof NullLiteral || this instanceof SlotRef);
            return NullLiteral.create(ScalarType.BOOLEAN).treeToThrift();
        }
        TExpr result = new TExpr();
        treeToThriftHelper(result);
        return result;
    }

    public void setFn(Function fn) {
        this.fn = fn;
    }

    // Append a flattened version of this expr, including all children, to 'container'.
    protected void treeToThriftHelper(TExpr container) {
        TExprNode msg = new TExprNode();
        msg.type = type.toThrift();
        msg.num_children = children.size();
        if (fn != null) {
            msg.setFn(fn.toThrift());
            if (fn.hasVarArgs()) {
                msg.setVarargStartIdx(fn.getNumArgs() - 1);
            }
        }
        msg.output_scale = getOutputScale();
        msg.setIsNullable(isNullable());
        toThrift(msg);
        container.addToNodes(msg);
        for (Expr child : children) {
            child.treeToThriftHelper(container);
        }
    }

    // Convert this expr into msg (excluding children), which requires setting
    // msg.op as well as the expr-specific field.
    protected abstract void toThrift(TExprNode msg);

    public List<String> childrenToSql() {
        List<String> result = Lists.newArrayList();
        for (Expr child : children) {
            result.add(child.toSql());
        }
        return result;
    }

    //    public void printChild() {
    //        LOG.debug("PALOTEST toSql={} debugString={}",
    //                  this.toSql(),
    //                  this.debugString());
    //        if (this instanceof SlotRef) {
    //            LOG.debug("dhc slot slotid={} tupleid={}",
    //                      ((SlotRef) this).getSlotId(),
    //                      ((SlotRef) this).getDesc().getParent().getId());
    //        }
    //    }

    public static com.google.common.base.Predicate<Expr> isAggregatePredicate() {
        return IS_AGGREGATE_PREDICATE;
    }

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
    protected void resetAnalysisState() { isAnalyzed = false; }

    /**
     * Resets the internal analysis state of this expr tree. Removes implicit casts.
     */
    public Expr reset() {
      if (isImplicitCast()) return getChild(0).reset();
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
        if (this instanceof CompoundPredicate && ((CompoundPredicate) this).getOp() ==
          CompoundPredicate.Operator.AND) {
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

    // Identical behavior to TreeNode.collect() except it matches expr that are aggregates
    public <C extends Expr> void collectAggregateExprs(List<C> output) {
        if (isAggregate() && !output.contains((C) this)) {
            output.add((C) this);
            return;
        }
        for (Expr child : children) {
            child.collectAggregateExprs(output);
        }
    }

    public boolean containsAggregate() {
        if (isAggregate()) {
            return true;
        }
        return containsAggregate(children);
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
     * Returns true if expr is fully bound by tid, otherwise false.
     */
    public boolean isBound(TupleId tid) {
        return isBoundByTupleIds(Lists.newArrayList(tid));
    }

    /**
     * Returns true if expr is fully bound by tids, otherwise false.
     */
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        for (Expr child: children) {
            if (!child.isBoundByTupleIds(tids)) return false;
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

    public boolean isBound(List<SlotId> slotIds) {
        final List<TupleId> exprTupleIds = Lists.newArrayList();
        final List<SlotId> exprSlotIds = Lists.newArrayList();
        getIds(exprTupleIds, exprSlotIds);
        return !exprSlotIds.retainAll(slotIds);
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
        if (isAnalyzed) return isConstant_;
        return isConstantImpl();
    }

    /**
     * Implements isConstant() - computes the value without using 'isConstant_'.
     */
    protected boolean isConstantImpl() {
        for (Expr expr : children) {
            if (!expr.isConstant()) return false;
        }
        return true;
    }

    /**
     * Return true if this expr is a scalar subquery.
     */
    public boolean isScalarSubquery() {
        Preconditions.checkState(isAnalyzed);
        return this instanceof Subquery && getType().isScalarType();
    }

    /**
     * Returns true if expr is can use vectorized process, otherwise false.
     */
    public boolean isVectorized() {
        for (Expr child : children) {
            if (!child.isVectorized()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether this expr returns a boolean type or NULL type.
     * If not, throws an AnalysisException with an appropriate error message using
     * 'name' as a prefix. For example, 'name' could be "WHERE clause".
     * The error message only contains this.toSql() if printExpr is true.
     */
    public void checkReturnsBool(String name, boolean printExpr) throws AnalysisException {
        if (!type.isBoolean() && !type.isNull()) {
            if (this instanceof BoolLiteral) {
                return;
            }
            throw new AnalysisException(
              String.format("%s%s requires return type 'BOOLEAN'. " + "Actual type is '%s'.", name,
                (printExpr) ? " '" + toSql() + "'" : "", type.toString()));
        }
    }

    public Expr checkTypeCompatibility(Type targetType) throws AnalysisException {
        if (targetType.getPrimitiveType().equals(type.getPrimitiveType())) {
            return this;
        }
        // bitmap must match exactly
        if (targetType.getPrimitiveType() == PrimitiveType.BITMAP) {
            throw new AnalysisException("bitmap column require the function return type is BITMAP");
        }
        // TargetTable's hll column must be hll_hash's result
        if (targetType.getPrimitiveType() == PrimitiveType.HLL) {
            checkHllCompatibility();
            return this;
        }
        Expr newExpr = castTo(targetType);
        newExpr.checkValueValid();
        return newExpr;
    }

    private void checkHllCompatibility() throws AnalysisException {
        final String hllMismatchLog = "Column's type is HLL,"
                + " SelectList must contains HLL or hll_hash or hll_empty function's result";
        if (this instanceof SlotRef) {
            final SlotRef slot = (SlotRef) this;
            if (!slot.getType().equals(Type.HLL)) {
                throw new AnalysisException(hllMismatchLog);
            }
        } else if (this instanceof FunctionCallExpr) {
            final FunctionCallExpr functionExpr = (FunctionCallExpr) this;
            if (!functionExpr.getFnName().getFunction().equalsIgnoreCase("hll_hash") &&
                    !functionExpr.getFnName().getFunction().equalsIgnoreCase("hll_empty")) {
                throw new AnalysisException(hllMismatchLog);
            }
        } else {
            throw new AnalysisException(hllMismatchLog);
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
        // If the targetType is NULL_TYPE then ignore the cast because NULL_TYPE
        // is compatible with all types and no cast is necessary.
        if (targetType.isNull()) {
            return this;
        }

        if ((targetType.isStringType() || targetType.isHllType())
               && (this.type.isStringType() || this.type.isHllType())) {
            return this;
        }
        // Preconditions.checkState(PrimitiveType.isImplicitCast(type, targetType), "cast %s to %s", this.type, targetType);
        // TODO(zc): use implicit cast
        if (!Type.canCastTo(this.type, targetType)) {
            throw new AnalysisException("type not match, originType=" + this.type
                    + ", targeType=" + targetType);

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
        Expr newChild = child.uncheckedCastTo(targetType);
        setChild(childIndex, newChild);
    }

    /**
     * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
     */
    public Expr ignoreImplicitCast() {
        return this;
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
        if (t1.getPrimitiveType() != compatibleType.getPrimitiveType()) {
            castChild(compatibleType, 0);
        }
        if (t2.getPrimitiveType() != compatibleType.getPrimitiveType()) {
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

    public boolean comeFrom(Expr srcExpr) {
        SlotRef unwrapSloRef = this.unwrapSlotRef();
        if (unwrapSloRef == null) {
            return false;
        }
        SlotRef unwrapSrcSlotRef = srcExpr.unwrapSlotRef();
        if (unwrapSrcSlotRef == null) {
            return false;
        }
        if (unwrapSloRef.columnEqual(unwrapSrcSlotRef)) {
            return true;
        }
        // check source expr
        SlotDescriptor slotDescriptor = unwrapSloRef.getDesc();
        if (slotDescriptor == null || slotDescriptor.getSourceExprs() == null
                || slotDescriptor.getSourceExprs().size() != 1) {
            return false;
        }
        return slotDescriptor.getSourceExprs().get(0).comeFrom(unwrapSrcSlotRef);
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
        if (unwrappedExpr instanceof SlotRef) return (SlotRef) unwrappedExpr;
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

    public static double getConstFromExpr(Expr e) throws AnalysisException{
        Preconditions.checkState(e.isConstant());
        double value = 0;
        if( e instanceof LiteralExpr){
            LiteralExpr lit = (LiteralExpr)e;
            value = lit.getDoubleValue();
        } else {
            throw new AnalysisException("To const value not a LiteralExpr " );
        }
        return value;
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

    public boolean contains(List<Expr> exprs) {
        if (exprs.isEmpty()) {
            return false;
        }
        for (Expr expr : exprs) {
            if (contains(expr)) {
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
    protected Function getBuiltinFunction(
            Analyzer analyzer, String name, Type[] argTypes, Function.CompareMode mode)
            throws AnalysisException {
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, Arrays.asList(argTypes), Type.INVALID, false,
                VectorizedUtil.isVectorized());
        Function f = Catalog.getCurrentCatalog().getFunction(searchDesc, mode);
        if (f != null && fnName.getFunction().equalsIgnoreCase("rand")) {
            if (this.children.size() == 1
                    && !(this.children.get(0) instanceof LiteralExpr)) {
                throw new AnalysisException("The param of rand function must be literal");
            }
        }
        return f;
    }

    protected Function getTableFunction(String name, Type[] argTypes,
                                        Function.CompareMode mode) {
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, Arrays.asList(argTypes), Type.INVALID, false,
                VectorizedUtil.isVectorized());
        Function f = Catalog.getCurrentCatalog().getTableFunction(searchDesc, mode);
        return f;
    }

    /**
     * Pushes negation to the individual operands of a predicate
     * tree rooted at 'root'.
     */
    public static Expr pushNegationToOperands(Expr root) {
        Preconditions.checkNotNull(root);
        if (Expr.IS_NOT_PREDICATE.apply(root)) {
            try {
                // Make sure we call function 'negate' only on classes that support it,
                // otherwise we may recurse infinitely.
                Method m = root.getChild(0).getClass().getDeclaredMethod(NEGATE_FN);
                return pushNegationToOperands(root.getChild(0).negate());
            } catch (NoSuchMethodException e) {
                // The 'negate' function is not implemented. Break the recursion.
                return root;
            }
        }

        if (root instanceof CompoundPredicate) {
            Expr left = pushNegationToOperands(root.getChild(0));
            Expr right = pushNegationToOperands(root.getChild(1));
            CompoundPredicate compoundPredicate =
                    new CompoundPredicate(((CompoundPredicate)root).getOp(), left, right);
            compoundPredicate.setPrintSqlInParens(root.getPrintSqlInParens());
            return compoundPredicate;
        }

        return root;
    }

    /**
     * Negates a boolean Expr.
     */
    public Expr negate() {
        Preconditions.checkState(type.equals(Type.BOOLEAN));
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, this, null);
    }

    /**
     * Returns the subquery of an expr. Returns null if this expr does not contain
     * a subquery.
     *
     * TODO: Support predicates with more that one subqueries when we implement
     * the independent subquery evaluation.
     */
    public Subquery getSubquery() {
        if (!contains(Subquery.class)) {
            return null;
        }
        List<Subquery> subqueries = Lists.newArrayList();
        collect(Subquery.class, subqueries);
        Preconditions.checkState(subqueries.size() == 1,
                "only support one subquery in " + this.toSql());
        return subqueries.get(0);
    }

    public boolean isCorrelatedPredicate(List<TupleId> tupleIdList) {
        if (this instanceof SlotRef && !this.isBoundByTupleIds(tupleIdList)) {
            return true;
        }
        for (Expr child : this.getChildren()) {
            if (child.isCorrelatedPredicate(tupleIdList)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("Not implemented serializable ");
    }

    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not implemented serializable ");
    }

    enum ExprSerCode {
        SLOT_REF(1),
        NULL_LITERAL(2),
        BOOL_LITERAL(3),
        INT_LITERAL(4),
        LARGE_INT_LITERAL(5),
        FLOAT_LITERAL(6),
        DECIMAL_LITERAL(7),
        STRING_LITERAL(8),
        DATE_LITERAL(9),
        MAX_LITERAL(10),
        BINARY_PREDICATE(11),
        FUNCTION_CALL(12),
        ARRAY_LITERAL(13),
        CAST_EXPR(14);

        private static Map<Integer, ExprSerCode> codeMap = Maps.newHashMap();

        static {
            for (ExprSerCode item : ExprSerCode.values()) {
                codeMap.put(item.code, item);
            }
        }

        private int code;

        ExprSerCode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static ExprSerCode fromCode(int code) {
            return codeMap.get(code);
        }
    }

    public static void writeTo(Expr expr, DataOutput output) throws IOException {
        if (expr instanceof SlotRef) {
            output.writeInt(ExprSerCode.SLOT_REF.getCode());
        } else if (expr instanceof NullLiteral) {
            output.writeInt(ExprSerCode.NULL_LITERAL.getCode());
        } else if (expr instanceof BoolLiteral) {
            output.writeInt(ExprSerCode.BOOL_LITERAL.getCode());
        } else if (expr instanceof IntLiteral) {
            output.writeInt(ExprSerCode.INT_LITERAL.getCode());
        } else if (expr instanceof LargeIntLiteral) {
            output.writeInt(ExprSerCode.LARGE_INT_LITERAL.getCode());
        } else if (expr instanceof FloatLiteral) {
            output.writeInt(ExprSerCode.FLOAT_LITERAL.getCode());
        } else if (expr instanceof DecimalLiteral) {
            output.writeInt(ExprSerCode.DECIMAL_LITERAL.getCode());
        } else if (expr instanceof StringLiteral) {
            output.writeInt(ExprSerCode.STRING_LITERAL.getCode());
        } else if (expr instanceof MaxLiteral) {
            output.writeInt(ExprSerCode.MAX_LITERAL.getCode());
        } else if (expr instanceof BinaryPredicate) {
            output.writeInt(ExprSerCode.BINARY_PREDICATE.getCode());
        } else if (expr instanceof FunctionCallExpr) {
            output.writeInt(ExprSerCode.FUNCTION_CALL.getCode());
        } else if (expr instanceof ArrayLiteral) {
            output.writeInt(ExprSerCode.ARRAY_LITERAL.getCode());
        } else if (expr instanceof CastExpr){
            output.writeInt(ExprSerCode.CAST_EXPR.getCode());
        }else {
            throw new IOException("Unknown class " + expr.getClass().getName());
        }
        expr.write(output);
    }

    /**
     * The expr result may be null
     * @param in
     * @return
     * @throws IOException
     */
    public static Expr readIn(DataInput in) throws IOException {
        int code = in.readInt();
        ExprSerCode exprSerCode = ExprSerCode.fromCode(code);
        if (exprSerCode == null) {
            throw new IOException("Unknown code: " + code);
        }
        switch (exprSerCode) {
            case SLOT_REF:
                return SlotRef.read(in);
            case NULL_LITERAL:
                return NullLiteral.read(in);
            case BOOL_LITERAL:
                return BoolLiteral.read(in);
            case INT_LITERAL:
                return IntLiteral.read(in);
            case LARGE_INT_LITERAL:
                return LargeIntLiteral.read(in);
            case FLOAT_LITERAL:
                return FloatLiteral.read(in);
            case DECIMAL_LITERAL:
                return DecimalLiteral.read(in);
            case STRING_LITERAL:
                return StringLiteral.read(in);
            case MAX_LITERAL:
                return MaxLiteral.read(in);
            case BINARY_PREDICATE:
                return BinaryPredicate.read(in);
            case FUNCTION_CALL:
                return FunctionCallExpr.read(in);
            case ARRAY_LITERAL:
                return ArrayLiteral.read(in);
            case CAST_EXPR:
                return CastExpr.read(in);
            default:
                throw new IOException("Unknown code: " + code);
        }
    }

    // If this expr can serialize and deserialize,
    // Expr will be serialized when this in load statement.
    // If one expr implement write/readFields, must override this function
    public boolean supportSerializable() {
        return false;
    }


    protected void recursiveResetChildrenResult() throws AnalysisException {
        for (int i = 0; i < children.size(); i++) {
            final Expr child = children.get(i);
            final Expr newChild = child.getResultValue();
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
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        final Expr newExpr = ExpressionFunctions.INSTANCE.evalExpr(this);
        return newExpr != null ? newExpr : this;
    }

    public String getStringValue() {
        if (this instanceof LiteralExpr) {
            return ((LiteralExpr) this).getStringValue();
        }
        return "";
    }

    public static Expr getFirstBoundChild(Expr expr, List<TupleId> tids) {
        for (Expr child: expr.getChildren()) {
            if (child.isBoundByTupleIds(tids)) return child;
        }
        return null;
    }

    /**
     * Returns true if expr contains specify function, otherwise false.
     */
    public boolean isContainsFunction(String functionName) {
        if (fn == null) return false;
        if (fn.functionName().equalsIgnoreCase(functionName))  return true;
        for (Expr child: children) {
            if (child.isContainsFunction(functionName)) return true;
        }
        return false;
    }

    /**
     * Returns true if expr contains specify className, otherwise false.
     */
    public boolean isContainsClass(String className) {
        if (this.getClass().getName().equalsIgnoreCase(className)) return true;
        for (Expr child: children) {
            if (child.isContainsClass(className)) return true;
        }
        return false;
    }

    protected boolean hasNullableChild() {
        for (Expr expr : children) {
            if (expr.isNullable()) return true;
        }
        return false;
    }

    /**
     * For excute expr the result is nullable
     * TODO: Now only SlotRef and LiteralExpr overwrite the method, each child of Expr should
     * overwrite this method to plan correct
     */
    public boolean isNullable() {
        if (fn == null) {
            return true;
        }
        switch (fn.getNullableMode()) {
            case DEPEND_ON_ARGUMENT:
                return hasNullableChild();
            case ALWAYS_NOT_NULLABLE:
                return false;
            case CUSTOM:
                return customNullableAlgorithm();
            case ALWAYS_NULLABLE:
            default:
                return true;
        }
    }

    private boolean customNullableAlgorithm() {
        Preconditions.checkState(fn.getNullableMode() == Function.NullableMode.CUSTOM);
        if (fn.functionName().equalsIgnoreCase("if")) {
            Preconditions.checkState(children.size() == 3);
            for (int i = 1; i < children.size(); i++) {
                if (children.get(i).isNullable()) {
                    return true;
                }
            }
            return false;
        }
        if (fn.functionName().equalsIgnoreCase("ifnull")) {
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
        return true;
    }
}
