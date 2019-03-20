package org.apache.doris.optimizer.property;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpressionWapper;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.operator.OptLogical;

import java.util.List;

public class OptLogicalProperty extends OptProperty {

    private List<OptColumnRef> outputs;

    public OptLogicalProperty() {
        this.outputs = Lists.newArrayList();
    }

    public List<OptColumnRef> getOutputs() { return outputs; }
    public void setOutputs(List<OptColumnRef> columns) { outputs.addAll(columns); }

    @Override
    public void derive(OptExpressionWapper wapper, List<OptProperty> childrenProperty) {
        // Derive columns
        final OptLogical logical = (OptLogical) wapper.getExpression().getOp();
        outputs.addAll(logical.deriveOuput(wapper));
    }
}
