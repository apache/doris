package org.apache.doris.statistics;

import org.apache.doris.nereids.trees.expressions.Literal;

public class SlotStatsDeriveResult {

    // number of distinct value
    private long ndv;
    private Literal max;
    private Literal min;

    public long getNdv() {
        return ndv;
    }

    public void setNdv(long ndv) {
        this.ndv = ndv;
    }

    public Literal getMax() {
        return max;
    }

    public void setMax(Literal max) {
        this.max = max;
    }

    public Literal getMin() {
        return min;
    }

    public void setMin(Literal min) {
        this.min = min;
    }
}
