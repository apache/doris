package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.physical.PhysicalUnaryOperator;

public class PhysicalExchange extends PhysicalUnaryOperator {



    public PhysicalExchange() {
        super(OperatorType.PHYSICAL_EXCHANGE);
    }
}
