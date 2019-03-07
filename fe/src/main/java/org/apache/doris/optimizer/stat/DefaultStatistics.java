package org.apache.doris.optimizer.stat;

public class DefaultStatistics implements Statistics {

    private final long cardinality;
    private final long rowNumbers;

    public DefaultStatistics(long cardinality, long rowNumbers) {
        this.cardinality = cardinality;
        this.rowNumbers = rowNumbers;
    }

    @Override
    public long getCardinality() {
        return cardinality;
    }

    @Override
    public long getRowNumbers() {
        return rowNumbers;
    }

    @Override
    public long getScanBytes() {
        return 0;
    }
}
