package org.apache.doris.nereids.commonCTE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class TableSignature {
    private final boolean isSPJG;
    private final boolean containsAggregation;
    private final Set<Long> tableIds;

    public static TableSignature EMPTY = new TableSignature(false, false, ImmutableSet.of());

    public TableSignature(boolean isSPJG, boolean containsAggregation , Set<Long> tableIds) {
        this.isSPJG = isSPJG;
        this.tableIds = ImmutableSet.copyOf(tableIds);
        this.containsAggregation = containsAggregation;
    }

    public boolean isSPJG() {
        return isSPJG;
    }

    public boolean isContainsAggregation() {
        return containsAggregation;
    }

    public Set<Long> getTableIds() {
        return tableIds;
    }

    public TableSignature withContainsAggregation(boolean containsAggregation) {
        return new TableSignature(isSPJG, containsAggregation, tableIds);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isSPJG) {
            sb.append("SPJG ");
        }
        if (containsAggregation) {
            sb.append("AGG ");
        }
        if (tableIds != null && !tableIds.isEmpty()) {
            sb.append(tableIds);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSignature that = (TableSignature) o;
        return isSPJG == that.isSPJG && containsAggregation == that.containsAggregation
                && tableIds.equals(that.tableIds);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode() + tableIds.hashCode();
    }
}
