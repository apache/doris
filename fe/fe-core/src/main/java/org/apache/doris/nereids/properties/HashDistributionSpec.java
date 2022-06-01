package org.apache.doris.nereids.properties;

import org.apache.doris.analysis.HashDistributionDesc;

public class HashDistributionSpec extends DistributionSpec {

    public enum ShuffleType {
        COLOCATE,
        BUCKET,
        AGG,
        NORMAL
    }

    private ShuffleType shuffleType;

    private HashDistributionDesc hashDistributionDesc;

}
