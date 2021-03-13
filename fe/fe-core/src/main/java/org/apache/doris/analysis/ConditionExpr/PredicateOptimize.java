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
package org.apache.doris.analysis.ConditionExpr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.ExistsPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public class PredicateOptimize {
    public Expr optimize(Expr whereClause) {
        List<Expr> predicates = extractPredicates(whereClause);
        // if where clause is null or contains one predicate, no need to optimize
        if (predicates == null || predicates.size() <= 1) {
            return whereClause;
        }
        boolean isSupport = checkIfSupportPredicate(predicates);
        if (!isSupport) {
            return whereClause;
        }
        try {
            ExtractionResult result = fromPredicate(whereClause);
            Expr resultingPredicate = createResultingPredicate(
                    toPredicate(result.tupleDomain),
                    result.getRemainingExpression());
            return resultingPredicate;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return whereClause;
    }

    public ExtractionResult fromPredicate(Expr expr) {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) expr;
            ExtractionResult left = fromPredicate(cp.getChild(0));
            TupleDomain<SlotRef> leftTupleDomain = left.tupleDomain;
            ExtractionResult right = fromPredicate(cp.getChild(1));
            TupleDomain<SlotRef> rightTupleDomain = right.tupleDomain;
            if (cp.getOp() == CompoundPredicate.Operator.AND) {
                return new ExtractionResult(leftTupleDomain.intersect(rightTupleDomain), combineConjuncts(left.remainingExpression, right.remainingExpression));
            } else {
                Expr remainingExpression = expr;
                TupleDomain<SlotRef> tupleDomain = TupleDomain.columnWiseUnion(leftTupleDomain, rightTupleDomain);
                if (left.getRemainingExpression().equals(right.getRemainingExpression())){
                    boolean matchingSingleSymbolDomains = !leftTupleDomain.isNone()
                            && !rightTupleDomain.isNone()
                            && leftTupleDomain.getDomains().get().size() == 1
                            && rightTupleDomain.getDomains().get().size() == 1
                            && leftTupleDomain.getDomains().get().keySet().equals(rightTupleDomain.getDomains().get().keySet());
                    boolean oneSideIsSuperSet = leftTupleDomain.contains(rightTupleDomain) || rightTupleDomain.contains(leftTupleDomain);

                    if (oneSideIsSuperSet) {
                        remainingExpression = left.getRemainingExpression();
                    } else if (matchingSingleSymbolDomains) {
                        Domain leftDomain = getOnlyElement(leftTupleDomain.getDomains().get().values());
                        Domain rightDomain = getOnlyElement(rightTupleDomain.getDomains().get().values());
                        Type type = leftDomain.getType();
                        boolean unionedDomainContainsNaN = tupleDomain.isAll() ||
                                (tupleDomain.getDomains().isPresent() &&
                                        getOnlyElement(tupleDomain.getDomains().get().values()).getValues().isAll());
                        boolean implicitlyAddedNaN = (type == Type.DOUBLE) &&
                                !leftDomain.getValues().isAll() &&
                                !rightDomain.getValues().isAll() &&
                                unionedDomainContainsNaN;
                        if (!implicitlyAddedNaN) {
                            remainingExpression = left.getRemainingExpression();
                        }
                    }
                }
                return new ExtractionResult(tupleDomain, remainingExpression);
            }
        } else {
            TupleDomain tupleDomain = createTupleDomain(expr);
            return new ExtractionResult(tupleDomain, new BoolLiteral(true));
        }
    }

    public TupleDomain createTupleDomain(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate cp = (BinaryPredicate) expr;
            BinaryPredicate.Operator operator = cp.getOp();
            Expr left = cp.getChild(0);
            SlotRef slot = (SlotRef) left;
            Expr right = cp.getChild(1);
            Domain domain = null;
            if (right instanceof LiteralExpr) {
                LiteralExpr le = (LiteralExpr) right;
                Object value;
                if (le instanceof StringLiteral || le instanceof DateLiteral) {
                    value = le.getStringValue();
                } else if (le instanceof BoolLiteral) {
                    value = ((BoolLiteral) le).getValue();
                } else {
                    value = le.getRealValue();
                }

                Type type = le.getType();

                if (operator == BinaryPredicate.Operator.EQ) {
                    domain = Domain.create(ValueSet.ofRanges(Range.equal(type, value)), false);
                } else if (operator == BinaryPredicate.Operator.GE) {
                    domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false);
                } else if (operator == BinaryPredicate.Operator.GT) {
                    domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false);
                } else if (operator == BinaryPredicate.Operator.LE) {
                    domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false);
                } else if (operator == BinaryPredicate.Operator.LT) {
                    domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false);
                } else if (operator == BinaryPredicate.Operator.NE) {
                    domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), false);
                }
                Map<SlotRef, Domain> domains = new HashMap<>();
                domains.put(slot, domain);
                TupleDomain tupleDomain = TupleDomain.withColumnDomains(domains);
                return tupleDomain;
            } else { //如果是列或者带有函数的情况，则直接返回错误
                throw new RuntimeException("do not support this expr:"+right.toSql());
            }
        } else if (expr instanceof IsNullPredicate) {
            IsNullPredicate ip = (IsNullPredicate) expr;
            Expr left = ip.getChild(0);
            SlotRef slot = (SlotRef) left;
            Type type = slot.getType();
            Domain domain = null;
            if (ip.isNotNull()) {
                domain = Domain.notNull(type);
            } else {
                domain = Domain.onlyNull(type);
            }
            Map<SlotRef, Domain> domains = new HashMap<>();
            domains.put(slot, domain);
            TupleDomain tupleDomain = TupleDomain.withColumnDomains(domains);
            return tupleDomain;
        } else if (expr instanceof InPredicate) { //TODO:转换成多个or表达式
            TupleDomain tupleDomain = TupleDomain.all();
            return tupleDomain;
        } else {
            TupleDomain tupleDomain = TupleDomain.all();
            return tupleDomain;
        }
    }

    public boolean checkIfSupportPredicate(List<Expr> predicates) {
        for (Expr expr:predicates) {
            // do not support in expression, `exist` expression, 'like' expression
            if (expr instanceof InPredicate) {
                return false;
            } else if (expr instanceof ExistsPredicate) {
                return false;
            } else if (expr instanceof LikePredicate) {
                return false;
            } else if (expr instanceof IsNullPredicate) {
                return false;
            }
            Expr left = expr.getChild(0);
            // if left is not column, like column contains function
            if (!(left instanceof SlotRef)) {
                return false;
            }
        }
        return true;
    }

    public List<Expr> extractPredicates(Expr expr) {
        if (expr == null) {
            return null;
        }
        List<Expr> res = new ArrayList<>();
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate bp = (CompoundPredicate) expr;
            Expr left = bp.getChild(0);
            List<Expr> lpredicates = extractPredicates(left);
            res.addAll(lpredicates);
            Expr right = bp.getChild(1);
            List<Expr> rpredicates = extractPredicates(right);
            res.addAll(rpredicates);
        } else {
            res.add(expr);
        }
        return res;
    }

    public Expr toPredicate(TupleDomain<SlotRef> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return new BoolLiteral(false);
        }

        Map<SlotRef, Domain> domains = tupleDomain.getDomains().get();
        return domains.entrySet().stream()
                .map(entry -> toPredicate(entry.getValue(), entry.getKey()))
                .collect(collectingAndThen(toImmutableList(), expressions -> combineConjuncts(expressions)));
    }

    private Expr toPredicate(Domain domain, SlotRef column)
    {
        if (domain.getValues().isNone()) {
            return new BoolLiteral(false);
        }

        if (domain.getValues().isAll()) {
            return new BoolLiteral(true);
        }

        List<Expr> disjuncts = new ArrayList<>();

        disjuncts.addAll(domain.getValues().getValuesProcessor().transform(
                ranges -> extractDisjuncts(domain.getType(), ranges, column),
                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                }));

        return combineConjuncts(disjuncts);
    }

    private List<Expr> extractDisjuncts(Type type, Ranges ranges, SlotRef column)
    {
        List<Expr> disjuncts = new ArrayList<>();
        List<Expr> singleValues = new ArrayList<>();
        List<Range> orderedRanges = ranges.getOrderedRanges();

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(type, orderedRanges);
        SortedRangeSet complement = sortedRangeSet.complement();

        List<Range> singleValueExclusionsList = complement.getOrderedRanges().stream().filter(Range::isSingleValue).collect(toList());
        List<Range> originalUnionSingleValues = SortedRangeSet.copyOf(type, singleValueExclusionsList).union(sortedRangeSet).getOrderedRanges();
        PeekingIterator<Range> singleValueExclusions = peekingIterator(singleValueExclusionsList.iterator());

        /*
        For types including NaN, it is incorrect to introduce range "all" while processing a set of ranges,
        even if the component ranges cover the entire value set.
        This is because partial ranges don't include NaN, while range "all" does.
        Example: ranges (unbounded , 1.0) and (1.0, unbounded) should not be coalesced to (unbounded, unbounded) with excluded point 1.0.
        That result would be further translated to expression "xxx <> 1.0", which is satisfied by NaN.
        To avoid error, in such case the ranges are not optimised.
         */
        if (type == Type.DOUBLE) {
            boolean originalRangeIsAll = orderedRanges.stream().anyMatch(Range::isAll);
            boolean coalescedRangeIsAll = originalUnionSingleValues.stream().anyMatch(Range::isAll);
            if (!originalRangeIsAll && coalescedRangeIsAll) {
                for (Range range : orderedRanges) {
                    disjuncts.add(processRange(type, range, column));
                }
                return disjuncts;
            }
        }

        for (Range range : originalUnionSingleValues) {
            if (range.isSingleValue()) {
                singleValues.add(toExpression(range.getSingleValue(), type));
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<Expr> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                singleValuesInRange.add(toExpression(singleValueExclusions.next().getSingleValue(), type));
            }

            disjuncts.add(processRange(type, range, column));
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(new BinaryPredicate(BinaryPredicate.Operator.EQ, column, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new InPredicate(column, singleValues, false));
        }
        return disjuncts;
    }

    private Expr processRange(Type type, Range range, SlotRef column)
    {
        if (range.isAll()) {
            return new BoolLiteral(true);
        }

//        if (isBetween(range)) {
//            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
//            return new BetweenPredicate(column, toExpression(range.getLow().getValue(), type), toExpression(range.getHigh().getValue(), type), false);
//        }

        List<Expr> rangeConjuncts = new ArrayList<>();
        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case ABOVE:
                    rangeConjuncts.add(new BinaryPredicate(BinaryPredicate.Operator.GT, column, toExpression(range.getLow().getValue(), type)));
                    break;
                case EXACTLY:
                    rangeConjuncts.add(new BinaryPredicate(BinaryPredicate.Operator.GE, column, toExpression(range.getLow().getValue(),
                            type)));
                    break;
                case BELOW:
                    throw new IllegalStateException("Low Marker should never use BELOW bound: " + range);
                default:
                    throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
            }
        }
        if (!range.getHigh().isUpperUnbounded()) {
            switch (range.getHigh().getBound()) {
                case ABOVE:
                    throw new IllegalStateException("High Marker should never use ABOVE bound: " + range);
                case EXACTLY:
                    rangeConjuncts.add(new BinaryPredicate(BinaryPredicate.Operator.LE, column, toExpression(range.getHigh().getValue(), type)));
                    break;
                case BELOW:
                    rangeConjuncts.add(new BinaryPredicate(BinaryPredicate.Operator.LT, column, toExpression(range.getHigh().getValue(), type)));
                    break;
                default:
                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return combineConjuncts(rangeConjuncts);
    }

    private static boolean isBetween(Range range)
    {
        return !range.getLow().isLowerUnbounded() && range.getLow().getBound() == Marker.Bound.EXACTLY
                && !range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == Marker.Bound.EXACTLY;
    }

    public static class ExtractionResult
    {
        private final TupleDomain<SlotRef> tupleDomain;
        private final Expr remainingExpression;

        public ExtractionResult(TupleDomain<SlotRef> tupleDomain, Expr remainingExpression)
        {
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public TupleDomain<SlotRef> getTupleDomain()
        {
            return tupleDomain;
        }

        public Expr getRemainingExpression()
        {
            return remainingExpression;
        }
    }

    public static Expr combineConjuncts(Expr... expressions)
    {
        return combineConjuncts(Arrays.asList(expressions));
    }

    public static Expr combineConjuncts(Collection<Expr> expressions)
    {
        requireNonNull(expressions, "expressions is null");
        List<Expr> conjuncts = new ArrayList<>();
        for (Expr e:expressions) {
            if (e instanceof BoolLiteral ) {
                BoolLiteral bl = (BoolLiteral) e;
                if (bl.getValue()) {
                    continue;
                }
            }
            conjuncts.add(e);
        }
        if (conjuncts.size() <= 0) {
            return new BoolLiteral(true);
        }
        // remove duplicate expr
        conjuncts = removeDuplicates(conjuncts);
        // when contains a FALSE expr
        for (Expr e:conjuncts) {
            if (e instanceof BoolLiteral ) {
                BoolLiteral bl = (BoolLiteral) e;
                if (!bl.getValue()) {
                    return bl;
                }
            }
        }

        if (conjuncts.size() == 1) {
            return conjuncts.get(0);
        }
        CompoundPredicate cp = new CompoundPredicate(CompoundPredicate.Operator.AND, conjuncts.get(0), conjuncts.get(1));
        for (int i = 2; i < conjuncts.size(); i++) {
            cp = new CompoundPredicate(CompoundPredicate.Operator.AND, cp, conjuncts.get(i));
        }
        return cp;
    }

    public Expr toExpression(Object value, Type type) {
        if (type.getPrimitiveType() == PrimitiveType.TINYINT) {
            return new IntLiteral((long)value);
        } else if (type.getPrimitiveType() == PrimitiveType.SMALLINT) {
            return new IntLiteral((long)value);
        } else if (type.getPrimitiveType() == PrimitiveType.INT) {
            return new IntLiteral((long)value);
        } else if (type.getPrimitiveType() == PrimitiveType.BIGINT) {
            return new IntLiteral((long)value);
        } else if (type.getPrimitiveType() == PrimitiveType.LARGEINT) {
            try {
                if (value instanceof BigInteger) {
                    return new LargeIntLiteral(((BigInteger)value).toString());
                } else {
                    return new LargeIntLiteral(String.valueOf(value));
                }
            } catch (Exception e) {
                throw new RuntimeException("can not parse bigint:"+e.getMessage());
            }
        } else if (type.getPrimitiveType() == PrimitiveType.CHAR) {
            return new StringLiteral((String)value);
        } else if (type.getPrimitiveType() == PrimitiveType.VARCHAR) {
            return new StringLiteral((String)value);
        } else if (type.getPrimitiveType() == PrimitiveType.FLOAT) {
            return new FloatLiteral((Double)value);
        } else if (type.getPrimitiveType() == PrimitiveType.DOUBLE) {
            return new FloatLiteral((Double)value);
        } else if (type.getPrimitiveType() == PrimitiveType.BOOLEAN) {
            return new BoolLiteral((boolean)value);
        } else if (type.getPrimitiveType() == PrimitiveType.DATE) {
            try {
                return new DateLiteral((String) value, Type.DATE);
            } catch (Exception e) {
                throw new RuntimeException("parse date type value fail.");
            }
        } else if (type.getPrimitiveType() == PrimitiveType.DATETIME) {
            try {
                return new DateLiteral((String) value, Type.DATETIME);
            } catch (Exception e) {
                throw new RuntimeException("parse datetime type value fail.");
            }
        } else if (type.getPrimitiveType() == PrimitiveType.DECIMAL) {
            return new DecimalLiteral((BigDecimal)value);
        } else if (type.getPrimitiveType() == PrimitiveType.DECIMALV2) {
            return new DecimalLiteral((BigDecimal)value);
        } else if (type.getPrimitiveType() == PrimitiveType.INVALID_TYPE) {
            return new NullLiteral();
        }
        throw new RuntimeException("parse transform object to expression fail.");
    }

    private static List<Expr> removeDuplicates(List<Expr> expressions)
    {
        Set<Expr> seen = new HashSet<>();

        ImmutableList.Builder<Expr> result = ImmutableList.builder();
        for (Expr expression : expressions) {
            if (!seen.contains(expression)) {
                result.add(expression);
                seen.add(expression);
            }
        }

        return result.build();
    }

    Expr createResultingPredicate(
            Expr unenforcedConstraints,
            Expr remainingDecomposedPredicate)
    {
        return combineConjuncts(unenforcedConstraints, remainingDecomposedPredicate);
    }
}
