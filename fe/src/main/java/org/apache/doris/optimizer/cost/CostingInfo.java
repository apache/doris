package org.apache.doris.optimizer.cost;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.List;

public final class CostingInfo {

    private Statistics statistics;
    private List<Statistics> childrenStatistics;

    private long rowCount;
    private List<Long> childrenRowCount;
    private List<OptCost> childrenCosts;

    public CostingInfo(){
        this.childrenStatistics = Lists.newArrayList();
        this.childrenRowCount = Lists.newArrayList();
        this.childrenCosts = Lists.newArrayList();
    }

    public void setStat(Statistics statistics) {
        this.statistics = statistics;
    }

    public void addChildrenStat(Statistics statistics) {
        this.childrenStatistics.add(statistics);
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getRowCount() {
        return this.rowCount;
    }

    public void addChildCount(long rowCount) {
        this.childrenRowCount.add(rowCount);
    }

    public long getChildRowCount(int index) {
        return childrenStatistics.get(index).getRowCount();
    }

    public void addChildCost(OptCost cost) {
        this.childrenCosts.add(cost);
    }

    public OptCost getChildrenCost(int index) {
        return childrenCosts.get(index);
    }

    public int arity() {
        Preconditions.checkArgument(
                childrenStatistics.size() == childrenRowCount.size()
                        && childrenRowCount.size() == childrenCosts.size());
        return childrenCosts.size();
    }
}
