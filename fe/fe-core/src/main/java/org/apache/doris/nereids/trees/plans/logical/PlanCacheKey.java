package org.apache.doris.nereids.trees.plans.logical;

public class PlanCacheKey {
    public final LogicalPlan cachedPlan;

    public PlanCacheKey(LogicalPlan cachedPlan) {
        this.cachedPlan = cachedPlan;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PlanCacheKey && cachedPlan.deepEquals(((PlanCacheKey) o).cachedPlan);
    }

    @Override
    public int hashCode() {
        return cachedPlan.hashCode();
    }
}
