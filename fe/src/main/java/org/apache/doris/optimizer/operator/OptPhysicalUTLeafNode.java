package org.apache.doris.optimizer.operator;

import org.apache.doris.optimizer.OptUtils;

public class OptPhysicalUTLeafNode extends OptPhysical {
    private int value;

    public OptPhysicalUTLeafNode() {
        super(OptOperatorType.OP_PHYSICAL_UNIT_TEST_LEAF);
        this.value = OptUtils.getUTOperatorId();
    }

    public int getValue() { return value; }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptPhysicalUTLeafNode rhs = (OptPhysicalUTLeafNode) object;
        return value == rhs.value;
    }

    @Override
    public String getExplainString(String prefix) { return type.getName() + " (value=" + value + ")";
    }
}
