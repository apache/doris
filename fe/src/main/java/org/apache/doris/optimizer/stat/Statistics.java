package org.apache.doris.optimizer.stat;

public interface Statistics {

    long getCardinality();

    long getRowNumbers();

    long getScanBytes();
}
