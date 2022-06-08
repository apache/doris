package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.List;

public class PhysicalAggregation extends PhysicalUnaryOperator<PhysicalAggregation, PhysicalPlan> {

    private List<Expression> groupByExprList;

    private List<Expression> aggExprList;

    private List<Expression> partitionExprList;

    private AggregateInfo.AggPhase aggPhase;

    private boolean needFinalize;

    private boolean usingStream;

    public PhysicalAggregation() {
        super(OperatorType.PHYSICAL_AGGREGATION);
    }

    public PhysicalAggregation(OperatorType type, List<Expression> groupByExprList, List<Expression> aggExprList,
            AggregateInfo.AggPhase aggPhase) {
        super(type);
        this.groupByExprList = groupByExprList;
        this.aggExprList = aggExprList;
        this.aggPhase = aggPhase;
    }

    public List<Expression> getGroupByExprList() {
        return groupByExprList;
    }

    public List<Expression> getAggExprList() {
        return aggExprList;
    }

    public AggregateInfo.AggPhase getAggPhase() {
        return aggPhase;
    }

    public boolean isNeedFinalize() {
        return needFinalize;
    }

    public boolean isUsingStream() {
        return usingStream;
    }

    public void setGroupByExprList(List<Expression> groupByExprList) {
        this.groupByExprList = groupByExprList;
    }

    public void setAggExprList(List<Expression> aggExprList) {
        this.aggExprList = aggExprList;
    }

    public List<Expression> getPartitionExprList() {
        return partitionExprList;
    }

    public void setPartitionExprList(List<Expression> partitionExprList) {
        this.partitionExprList = partitionExprList;
    }

    public void setAggPhase(AggregateInfo.AggPhase aggPhase) {
        this.aggPhase = aggPhase;
    }

    public void setNeedFinalize(boolean needFinalize) {
        this.needFinalize = needFinalize;
    }

    public void setUsingStream(boolean usingStream) {
        this.usingStream = usingStream;
    }
}
