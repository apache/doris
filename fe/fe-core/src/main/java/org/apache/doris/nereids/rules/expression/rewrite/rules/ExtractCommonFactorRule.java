package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class ExtractCommonFactorRule extends AbstractExpressionRewriteRule {

    public static final ExtractCommonFactorRule INSTANCE = new ExtractCommonFactorRule();

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate expr, ExpressionRewriteContext context) {


        Expression rewrittenChildren = ExpressionUtils.combine(expr.getType(), ExpressionUtils.extract(expr).stream()
                .map(predicate -> rewrite(predicate, context)).collect(Collectors.toList()));

        if (!(rewrittenChildren instanceof CompoundPredicate)) {
            return rewrittenChildren;
        }

        CompoundPredicate compoundPredicate = (CompoundPredicate) rewrittenChildren;

        List<List<Expression>> partitions = ExpressionUtils.extract(compoundPredicate).stream()
                .map(predicate -> predicate instanceof CompoundPredicate ? ExpressionUtils.extract(
                        (CompoundPredicate) predicate) : Lists.newArrayList(predicate)).collect(Collectors.toList());

        Set<Expression> commons = partitions.stream().map(predicates -> predicates.stream().collect(Collectors.toSet()))
                .reduce(Sets::intersection).orElse(Collections.emptySet());

        List<List<Expression>> uncorrelated = partitions.stream()
                .map(predicates -> predicates.stream().filter(p -> !commons.contains(p)).collect(Collectors.toList()))
                .collect(Collectors.toList());

        Expression combineUncorrelated = ExpressionUtils.combine(compoundPredicate.getType(),
                uncorrelated.stream().map(predicates -> ExpressionUtils.combine(compoundPredicate.flip(), predicates))
                        .collect(Collectors.toList()));

        List<Expression> finalCompound = Lists.newArrayList(commons);
        finalCompound.add(combineUncorrelated);

        return ExpressionUtils.combine(compoundPredicate.flip(), finalCompound);
    }
}