package org.apache.doris.optimizer.operator;

public class OptPhysicalHashJoin extends OptPhysical {

    public OptPhysicalHashJoin() {
        super(OptOperatorType.OP_PHYSICAL_HASH_JOIN);
    }
}
