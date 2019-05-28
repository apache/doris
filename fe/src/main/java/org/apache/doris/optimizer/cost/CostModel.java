package org.apache.doris.optimizer.cost;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.operator.OptExpressionHandle;

public abstract class CostModel {
    private int hostNum;

    protected CostModel(int hostNum) {
        this.hostNum = hostNum;
    }

    public void setHostNum(int hostNum) {
        this.hostNum = hostNum;
    }

    public int getHostNum() {
        return this.hostNum;
    }

    protected abstract OptCost costHashAgg(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costHashJoin(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costSort(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costScan(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costDistribute(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costUnion(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costLimit(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costProject(OptExpressionHandle exprHandle, CostingInfo info);
    protected abstract OptCost costFilter(OptExpressionHandle exprHandle, CostingInfo info);

    protected OptCost costChildren(OptExpressionHandle exprhandle, CostingInfo info) {
        OptCost childCost = new OptCost();
        for (int i = 0; i < exprhandle.arity(); i++) {
            childCost.add(info.getChildrenCost(i));
        }
        return childCost;
    }

    public OptCost cost(OptExpressionHandle exprHandle, CostingInfo info) {
        OptCost cost = null;
        switch (exprHandle.getOp().getType()) {
            case OP_PHYSICAL_HASH_AGG:
                cost = costHashAgg(exprHandle, info);
                break;
            case OP_PHYSICAL_HASH_JOIN:
                cost = costHashJoin(exprHandle, info);
                break;
            case OP_PHYSICAL_SORT:
                cost = costSort(exprHandle, info);
                break;
            case OP_PHYSICAL_OLAP_SCAN:
                cost = costScan(exprHandle, info);
                break;
            case OP_PHYSICAL_DISTRIBUTION:
                cost = costDistribute(exprHandle, info);
                break;
            case OP_PHYSICAL_UNION:
                cost = costUnion(exprHandle, info);
                break;
            case OP_PHYSICAL_LIMIT:
                cost = costLimit(exprHandle, info);
                break;
            case OP_PHYSICAL_PROJECT:
                cost = costProject(exprHandle, info);
                break;
            case OP_PHYSICAL_FILTER:
                cost = costFilter(exprHandle, info);
                break;
            default:
                Preconditions.checkArgument(false,
                        "Operator does't to be supported for CostModel.");
        }
        final OptCost childrenCost = costChildren(exprHandle, info);
        cost.add(childrenCost);
        return cost;
    }
}
