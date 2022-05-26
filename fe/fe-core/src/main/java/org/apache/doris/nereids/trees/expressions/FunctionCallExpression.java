package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.Function;
import org.apache.doris.nereids.trees.NodeType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// Temp define
public class FunctionCallExpression extends Expression<FunctionCallExpression> {

    private FunctionName functionName;

    private List<Expression> params;

    private Function fn;

    public FunctionCallExpression(FunctionName functionName,
                                  Function fn, Expression... children) {
        super(NodeType.EXPRESSION, children);
        this.functionName = functionName;
        this.params = Arrays.stream(children).collect(Collectors.toList());
        this.fn = fn;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public List<Expression> getParams() {
        return params;
    }

    public Function getFn() {
        return fn;
    }

}
