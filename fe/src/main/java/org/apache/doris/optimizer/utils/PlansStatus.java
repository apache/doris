package org.apache.doris.optimizer.utils;

import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;

public class PlansStatus {
    // This will take long time for large dag.
    public static long getPlanCount(OptGroup group) {
        long planCountTotal = 0;
        for (MultiExpression mExpr : group.getMultiExpressions()) {
            long planCount = 1;
            for (OptGroup child : mExpr.getInputs()) {
                planCount *= getPlanCount(child);
            }
            planCountTotal += planCount;
        }
        return planCountTotal;
    }
}
