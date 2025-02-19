package org.apache.doris.nereids.processor.pre;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.PlanCachePhase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.PlaceholderLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.PlanCacheKey;

import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NormalizeForPlanCache extends PlanPreprocessor {
    private AtomicInteger literalIdGenerator = new AtomicInteger(0);

    @Override
    public Plan rewriteRoot(Plan root, StatementContext context) {
        if (!context.getConnectContext().getSessionVariable().enablePlanCache) {
            return root;
        }

        LogicalPlan normalizedPlan = (LogicalPlan) super.rewriteRoot(root, context);

        context.planCachePhase = PlanCachePhase.ONE;
        context.initPlaceholderPlan = normalizedPlan;
        Optional<LogicalPlan> cachedPlan = getCachedPlan(normalizedPlan);
        if (cachedPlan.isPresent()) {
            context.planCachePhase = PlanCachePhase.TWO;
            return cachedPlan.get();
        }
        return normalizedPlan;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, StatementContext context) {
        Plan child = filter.child().accept(this, context);
        AtomicBoolean hasLiteral = new AtomicBoolean();
        Expression newFilter = filter.getPredicate().rewriteUp(e -> {
            if (e instanceof Literal) {
                hasLiteral.set(true);
                PlaceholderLiteral placeholderLiteral = new PlaceholderLiteral(literalIdGenerator.getAndIncrement());
                context.placeholderLiteralToLiteral.put(placeholderLiteral, (Literal) e);
                return placeholderLiteral;
            } else {
                return e;
            }
        });

        if (hasLiteral.get()) {
            return new LogicalFilter<>(ImmutableSet.of(newFilter), child);
        }

        if (child != filter.child()) {
            return filter.withChildren(child);
        }

        return filter;
    }

    private Optional<LogicalPlan> getCachedPlan(LogicalPlan normalizedPlan) {
        NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();
        LogicalPlan cachedPlan = sqlCacheManager.planCache.get(new PlanCacheKey(normalizedPlan));
        return Optional.ofNullable(cachedPlan);
    }
}
