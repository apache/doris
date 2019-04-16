package org.apache.doris.optimizer.base;

import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;

public abstract class OptReqProperty {

    public abstract void compute(MultiExpression root, OptGroup child, OptProperty property);

}
