package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.List;

public class PhysicalAggregation extends PhysicalUnaryOperator<PhysicalAggregation, PhysicalPlan>{

    private List<Expression> groupByExprList;

    private List<Expression> aggExprList;

    private AggregateInfo.AggPhase aggPhase;

    private boolean needFinalize;

    private boolean usingStream;

    public PhysicalAggregation() {
        super(OperatorType.PHYSICAL_AGGREGATION);
    }

}
