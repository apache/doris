package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.properties.Order;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.List;

public class PhysicalSort extends PhysicalUnaryOperator<PhysicalSort, PhysicalPlan> {

    private int offset;

    private int limit;

    private List<Order> orderList;

    // if true, the output of this node feeds an AnalyticNode
    private boolean isAnalyticSort;

    public PhysicalSort() {
        super(OperatorType.PHYSICAL_SORT);
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public List<Order> getOrderList() {
        return orderList;
    }

    public boolean isAnalyticSort() {
        return isAnalyticSort;
    }
}
