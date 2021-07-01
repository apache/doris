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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This rule extracts common predicate from multiple disjunctions when it is applied
 * recursively bottom-up to a tree of CompoundPredicates.
 * There are two common predicate that will be extracted as following:
 * 1. Common Factors: (a and b) or (a and c) -> a and (b or c)
 * 2. Wide common factors: (1<k1<3 and k2 in ('Marry')) or (2<k1<4 and k2 in ('Tom'))
 *        -> (1<k1<4) and k2 in('Marry','Tom') and (1<k1<3 and k2 in ('Marry')) or (2<k1<4 and k2 in ('Tom'))
 *
 * The second rewriting can be controlled by session variable 'extract_wide_range_expr'
 */
public class ExtractCommonFactorsRule implements ExprRewriteRule {
    private final static Logger LOG = LogManager.getLogger(ExtractCommonFactorsRule.class);
    public static ExtractCommonFactorsRule INSTANCE = new ExtractCommonFactorsRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (expr == null) {
            return null;
        } else if (expr instanceof CompoundPredicate
                && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.OR) {
            Expr rewrittenExpr = extractCommonFactors(exprFormatting((CompoundPredicate) expr), analyzer);
            if (rewrittenExpr != null) {
                return rewrittenExpr;
            }
        } else {
            for (int i = 0; i < expr.getChildren().size(); i++) {
                Expr rewrittenExpr = apply(expr.getChild(i), analyzer);
                if (rewrittenExpr != null) {
                    expr.setChild(i, rewrittenExpr);
                }
            }
        }
        return expr;
    }

    /**
     * The input is a list of list, the inner list is and connected exprs, the outer list is or connected.
     * For example: Origin expr: (a and b and b) or (a and e and f)
     *
     * @param exprs: [[a, b, b], [a, e, f]]
     *               1. First step is deduplicate:
     * @code clearExprs: [[a, b, b], [a, e, f]] => [[a, b], [a, e, f]]
     * 2. Extract common factors:
     * @code commonFactorList: [a]
     * @code clearExprs: [[b], [e, f]]
     * 3. Extract wide common factors:
     * @code wideCommonExpr: b'
     * @code commonFactorList: [a, b']
     * 4. Construct new expr:
     * @return: a and b' and (b or (e and f))
     */
    private Expr extractCommonFactors(List<List<Expr>> exprs, Analyzer analyzer) {
        if (exprs.size() < 2) {
            return null;
        }
        // 1. remove duplicated elements [[a,a], [a, b], [a,b]] => [[a], [a,b]]
        Set<Set<Expr>> set = new LinkedHashSet<>();
        for (List<Expr> ex : exprs) {
            Set<Expr> es = new LinkedHashSet<>();
            es.addAll(ex);
            set.add(es);
        }
        List<List<Expr>> clearExprs = new ArrayList<>();
        for (Set<Expr> es : set) {
            List<Expr> el = new ArrayList<>();
            el.addAll(es);
            clearExprs.add(el);
        }
        if (clearExprs.size() == 1) {
            return makeCompound(clearExprs.get(0), CompoundPredicate.Operator.AND);
        }

        // 2. find duplicate cross the clause
        List<Expr> commonFactorList = new ArrayList<>(clearExprs.get(0));
        for (int i = 1; i < clearExprs.size(); ++i) {
            commonFactorList.retainAll(clearExprs.get(i));
        }
        boolean isReturnCommonFactorExpr = false;
        for (List<Expr> exprList : clearExprs) {
            exprList.removeAll(commonFactorList);
            if (exprList.size() == 0) {
                // For example, the sql is "where (a = 1) or (a = 1 and B = 2)"
                // if "(a = 1)" is extracted as a common factor expression, then the first expression "(a = 1)" has no expression
                // other than a common factor expression, and the second expression "(a = 1 and B = 2)" has an expression of "(B = 2)"
                //
                // In this case, the common factor expression ("a = 1") can be directly used to replace the whole CompoundOrPredicate.
                // In Fact, the common factor expression is actually the parent set of expression "(a = 1)" and expression "(a = 1 and B = 2)"
                //
                // exprList.size() == 0 means one child of CompoundOrPredicate has no expression other than a common factor expression.
                isReturnCommonFactorExpr = true;
                break;
            }
        }
        if (isReturnCommonFactorExpr) {
            Preconditions.checkState(!commonFactorList.isEmpty());
            Expr result = makeCompound(commonFactorList, CompoundPredicate.Operator.AND);
            if (LOG.isDebugEnabled()) {
                LOG.debug("equal ors: " + result.toSql());
            }
            return result;
        }

        // 3. find merge cross the clause
        if (analyzer.getContext().getSessionVariable().isExtractWideRangeExpr()) {
            Expr wideCommonExpr = findWideRangeExpr(clearExprs);
            if (wideCommonExpr != null) {
                commonFactorList.add(wideCommonExpr);
            }
        }

        // 4. construct new expr
        // rebuild CompoundPredicate if found duplicate predicate will build （predicate) and (.. or ..)  predicate in
        // step 1: will build (.. or ..)
        List<Expr> remainingOrClause = Lists.newArrayList();
        for (List<Expr> clearExpr : clearExprs) {
            Preconditions.checkState(!clearExpr.isEmpty());
            remainingOrClause.add(makeCompound(clearExpr, CompoundPredicate.Operator.AND));
        }
        Expr result = null;
        if (CollectionUtils.isNotEmpty(commonFactorList)) {
            result = new CompoundPredicate(CompoundPredicate.Operator.AND,
                    makeCompound(commonFactorList, CompoundPredicate.Operator.AND),
                    makeCompound(remainingOrClause, CompoundPredicate.Operator.OR));
            result.setPrintSqlInParens(true);
        } else {
            result = makeCompound(remainingOrClause, CompoundPredicate.Operator.OR);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("equal ors: " + result.toSql());
        }
        return result;
    }

    /**
     * The wide range of expr must satisfy two conditions as following:
     * 1. the expr is a constant filter for single column in single table.
     * 2. the single column of expr must appear in all clauses.
     * The expr extracted here is a wider range of expressions, similar to a pre-filtering.
     * But pre-filtering does not necessarily mean that it must have a positive impact on performance.
     */
    private Expr findWideRangeExpr(List<List<Expr>> exprList) {
        // 1. construct map <column, range/list>
        List<Map<SlotRef, Range<LiteralExpr>>> columnNameToRangeList = Lists.newArrayList();
        List<Map<SlotRef, InPredicate>> columnNameToInList = Lists.newArrayList();
        OUT_CONJUNCTS:
        for (List<Expr> conjuncts : exprList) {
            Map<SlotRef, Range<LiteralExpr>> columnNameToRange = Maps.newHashMap();
            Map<SlotRef, InPredicate> columnNameToInPredicate = Maps.newHashMap();
            for (Expr predicate : conjuncts) {
                if (!singleColumnPredicate(predicate)) {
                    continue;
                }
                SlotRef columnName = (SlotRef) predicate.getChild(0);
                if (predicate instanceof BinaryPredicate) {
                    Range<LiteralExpr> predicateRange = ((BinaryPredicate)predicate).convertToRange();
                    if (predicateRange == null){
                        continue;
                    }
                    Range<LiteralExpr> range = columnNameToRange.get(columnName);
                    if (range == null) {
                        range = predicateRange;
                    } else {
                        try {
                            range = range.intersection(predicateRange);
                        } catch (IllegalArgumentException | ClassCastException e) {
                            // (a >1 and a < 0) ignore this OR clause
                            LOG.debug("The range without intersection", e);
                            continue OUT_CONJUNCTS;
                        }
                    }
                    columnNameToRange.put(columnName, range);
                } else if (predicate instanceof InPredicate) {
                    InPredicate inPredicate = (InPredicate) predicate;
                    InPredicate intersectInPredicate = columnNameToInPredicate.get(columnName);
                    if (intersectInPredicate == null) {
                        intersectInPredicate = new InPredicate(inPredicate.getChild(0), inPredicate.getListChildren(),
                                inPredicate.isNotIn());
                    } else {
                        intersectInPredicate = intersectInPredicate.intersection((InPredicate) predicate);
                    }
                    columnNameToInPredicate.put(columnName, intersectInPredicate);
                }
            }
            columnNameToRangeList.add(columnNameToRange);
            columnNameToInList.add(columnNameToInPredicate);
        }

        // 2. merge clause
        Map<SlotRef, RangeSet<LiteralExpr>> resultRangeMap = Maps.newHashMap();
        for (Map.Entry<SlotRef, Range<LiteralExpr>> entry: columnNameToRangeList.get(0).entrySet()) {
            RangeSet<LiteralExpr> rangeSet = TreeRangeSet.create();
            rangeSet.add(entry.getValue());
            resultRangeMap.put(entry.getKey(), rangeSet);
        }
        for (int i = 1; i < columnNameToRangeList.size(); i++) {
            Map<SlotRef, Range<LiteralExpr>> columnNameToRange = columnNameToRangeList.get(i);
            resultRangeMap = mergeTwoClauseRange(resultRangeMap, columnNameToRange);
            if (resultRangeMap.isEmpty()) {
                break;
            }
        }
        Map<SlotRef, InPredicate> resultInMap = columnNameToInList.get(0);
        for (int i = 1; i < columnNameToRangeList.size(); i++) {
            Map<SlotRef, InPredicate> columnNameToIn = columnNameToInList.get(i);
            resultInMap = mergeTwoClauseIn(resultInMap, columnNameToIn);
            if (resultInMap.isEmpty()) {
                break;
            }
        }

        // 3. construct wide range expr
        List<Expr> wideRangeExprList = Lists.newArrayList();
        for (Map.Entry<SlotRef, RangeSet<LiteralExpr>> entry : resultRangeMap.entrySet()) {
            Expr wideRangeExpr = rangeSetToCompoundPredicate(entry.getKey(), entry.getValue());
            if (wideRangeExpr != null) {
                wideRangeExprList.add(wideRangeExpr);
            }
        }
        wideRangeExprList.addAll(resultInMap.values());
        return makeCompound(wideRangeExprList, CompoundPredicate.Operator.AND);
    }

    /**
     * An expression that meets the following three conditions will return true:
     * 1. single column from single table
     * 2. in or binary predicate
     * 3. one child of predicate is constant
     */
    private boolean singleColumnPredicate(Expr expr) {
        List<SlotRef> slotRefs = Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        if (slotRefs.size() != 1) {
            return false;
        }
        if (expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            if (!inPredicate.isLiteralChildren()) {
                return false;
            }
            if (inPredicate.isNotIn()) {
                return false;
            }
            if (inPredicate.getChild(0) instanceof SlotRef) {
                return true;
            }
            return false;
        } else if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            if (binaryPredicate.getChild(0) instanceof SlotRef
                    && binaryPredicate.getChild(1) instanceof LiteralExpr) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    /**
     * RangeSet1: 1<k1<3, k2<4
     * RangeSet2: 2<k1<4
     * Result: 1<k1<4
     */
    private Map<SlotRef, RangeSet<LiteralExpr>> mergeTwoClauseRange(Map<SlotRef, RangeSet<LiteralExpr>> clause1,
                                                                    Map<SlotRef, Range<LiteralExpr>> clause2) {
        Map<SlotRef, RangeSet<LiteralExpr>> result = Maps.newHashMap();
        for (Map.Entry<SlotRef, RangeSet<LiteralExpr>> clause1Entry: clause1.entrySet()) {
            SlotRef columnName = clause1Entry.getKey();
            Range<LiteralExpr> clause2Value = clause2.get(columnName);
            if (clause2Value == null) {
                continue;
            }
            try {
                clause1Entry.getValue().add(clause2Value);
            } catch (ClassCastException e) {
                // ignore a >1.0 or a <false
                LOG.debug("Abort this range of column" + columnName.toSqlImpl());
                continue;
            }
            result.put(columnName, clause1Entry.getValue());
        }
        return result;
    }

    /**
     * InPredicate1: k1 in (a), k2 in (b)
     * InPredicate2: k1 in (b)
     * Result: k1 in (a, b)
     */
    private Map<SlotRef, InPredicate> mergeTwoClauseIn(Map<SlotRef, InPredicate> clause1,
                                                       Map<SlotRef, InPredicate> clause2) {
        Map<SlotRef, InPredicate> result = Maps.newHashMap();
        for (Map.Entry<SlotRef, InPredicate> clause1Entry: clause1.entrySet()) {
            SlotRef columnName = clause1Entry.getKey();
            InPredicate clause2Value = clause2.get(columnName);
            if (clause2Value == null) {
                continue;
            }
            InPredicate union = clause1Entry.getValue().union(clause2Value);
            result.put(columnName, union);
        }
        return result;
    }

    /**
     * this function only process (a and b and c) or (d and e and f) like clause,
     * this function will format this to [[a, b, c], [d, e, f]]
     */
    private List<List<Expr>> exprFormatting(CompoundPredicate expr) {
        List<List<Expr>> orExprs = new ArrayList<>();
        for (Expr child : expr.getChildren()) {
            if (child instanceof CompoundPredicate) {
                CompoundPredicate childCp = (CompoundPredicate) child;
                if (childCp.getOp() == CompoundPredicate.Operator.OR) {
                    orExprs.addAll(exprFormatting(childCp));
                    continue;
                } else if (childCp.getOp() == CompoundPredicate.Operator.AND) {
                    orExprs.add(flatAndExpr(child));
                    continue;
                }
            }
            orExprs.add(Arrays.asList(child));
        }
        return orExprs;
    }

    /**
     * try to flat and , a and b and c => [a, b, c]
     */
    private List<Expr> flatAndExpr(Expr expr) {
        List<Expr> andExprs = new ArrayList<>();
        if (expr instanceof CompoundPredicate && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.AND) {
            andExprs.addAll(flatAndExpr(expr.getChild(0)));
            andExprs.addAll(flatAndExpr(expr.getChild(1)));
        } else {
            andExprs.add(expr);
        }
        return andExprs;
    }

    /**
     * Rebuild CompoundPredicate, [a, e, f] AND => a and e and f
     */
    private Expr makeCompound(List<Expr> exprs, CompoundPredicate.Operator op) {
        if (CollectionUtils.isEmpty(exprs)) {
            return null;
        }
        if (exprs.size() == 1) {
            return exprs.get(0);
        }
        CompoundPredicate result = new CompoundPredicate(op, exprs.get(0), exprs.get(1));
        for (int i = 2; i < exprs.size(); ++i) {
            result = new CompoundPredicate(op, result.clone(), exprs.get(i));
        }
        result.setPrintSqlInParens(true);
        return result;
    }

    /**
     * Convert RangeSet to Compound Predicate
     * @param slotRef: <k1>
     * @param rangeSet: {(1,3), (6,7)}
     * @return: (k1>1 and k1<3) or (k1>6 and k1<7)
     */
    public Expr rangeSetToCompoundPredicate(SlotRef slotRef, RangeSet<LiteralExpr> rangeSet) {
        List<Expr> compoundList = Lists.newArrayList();
        for (Range<LiteralExpr> range : rangeSet.asRanges()) {
            LiteralExpr lowerBound = null;
            LiteralExpr upperBound = null;
            if (range.hasLowerBound()) {
                lowerBound = range.lowerEndpoint();
            }
            if (range.hasUpperBound()) {
                upperBound = range.upperEndpoint();
            }
            if (lowerBound == null && upperBound == null) {
                continue;
            }
            if (lowerBound != null && upperBound != null && lowerBound.equals(upperBound)) {
                compoundList.add(new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, lowerBound));
                continue;
            }
            List<Expr> binaryPredicateList = Lists.newArrayList();
            if (lowerBound != null) {
                if (range.lowerBoundType() == BoundType.OPEN) {
                    binaryPredicateList.add(new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, lowerBound));
                } else {
                    binaryPredicateList.add(new BinaryPredicate(BinaryPredicate.Operator.GE, slotRef, lowerBound));
                }
            }
            if (upperBound !=null) {
                if (range.upperBoundType() == BoundType.OPEN) {
                    binaryPredicateList.add(new BinaryPredicate(BinaryPredicate.Operator.LT, slotRef, upperBound));
                } else {
                    binaryPredicateList.add(new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, upperBound));
                }
            }
            compoundList.add(makeCompound(binaryPredicateList, CompoundPredicate.Operator.AND));
        }
        return makeCompound(compoundList, CompoundPredicate.Operator.OR);
    }
}
