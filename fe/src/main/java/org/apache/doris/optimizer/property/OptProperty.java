package org.apache.doris.optimizer.property;

import org.apache.doris.optimizer.OptExpressionWapper;

import java.util.List;

public abstract class OptProperty {

    public abstract void derive(OptExpressionWapper wapper, List<OptProperty> childrenProperty);

}
