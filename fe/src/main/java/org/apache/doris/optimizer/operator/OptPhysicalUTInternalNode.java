package org.apache.doris.optimizer.operator;

import org.apache.doris.optimizer.OptUtils;

public class OptPhysicalUTInternalNode extends OptPhysical {

    private int value;

    public OptPhysicalUTInternalNode() {
        super(OptOperatorType.OP_PHYSICAL_UNIT_TEST_INTERNAL);
        this.value = OptUtils.getUTOperatorId();;
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
        final OptPhysicalUTInternalNode rhs = (OptPhysicalUTInternalNode) object;
        return value == rhs.value;
    }

    @Override
    public String getExplainString(String prefix) { return type.getName() + " (value=" + value + ")"; }
}
