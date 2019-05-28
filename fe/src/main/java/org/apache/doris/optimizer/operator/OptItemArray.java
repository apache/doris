package org.apache.doris.optimizer.operator;

import org.apache.doris.catalog.Type;

public class OptItemArray extends OptItem {

    protected OptItemArray() {
        super(OptOperatorType.OP_ITEM_ARRAY);
    }

    @Override
    public Type getReturnType() {
        return null;
    }
}
