package org.apache.doris.optimizer.operator;

import org.apache.doris.optimizer.base.DistributionEnforcerProperty;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OrderEnforcerProperty;
import org.apache.doris.optimizer.base.RequiredPhysicalProperty;

public class OptPhysicalUnionAll extends OptPhysical {

    public OptPhysicalUnionAll() {
        super(OptOperatorType.OP_PHYSICAL_UNION);
    }

    @Override
    public OrderEnforcerProperty getChildReqdOrder(
            OptExpressionHandle handle, OrderEnforcerProperty reqdOrder, int childIndex) {
        return OrderEnforcerProperty.EMPTY;
    }

    @Override
    public DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle handle, DistributionEnforcerProperty reqdDistribution, int childIndex) {
        return reqdDistribution;
    }

    @Override
    protected OptColumnRefSet deriveChildReqdColumns(
            OptExpressionHandle exprHandle, RequiredPhysicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        columns.intersects(exprHandle.getChildLogicalProperty(childIndex).getOutputColumns());
        return columns;
    }
}
