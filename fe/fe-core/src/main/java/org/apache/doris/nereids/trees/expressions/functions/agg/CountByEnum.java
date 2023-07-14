package org.apache.doris.nereids.trees.expressions.functions.agg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.StringType;

import java.util.List;
public class CountByEnum extends AggregateFunction implements CustomSignature, AlwaysNotNullable {

    public CountByEnum() {
        super("count_by_enum");
    }

    public CountByEnum(AggregateParam aggregateParam) {
        super("count_by_enum", aggregateParam);
    }

    public CountByEnum(Expression child) {
        super("count_by_enum", child);
    }

    public CountByEnum(AggregateParam aggregateParam, Expression... varArgs) {
        super("count_by_enum", aggregateParam, varArgs);
    }

    @Override
    public AggregateFunction withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 0 || children.size() == 1);
        if (children.size() == 0) {
            return this;
        }
        return new CountByEnum(getAggregateParam(), children.get(0));
    }

    @Override
    public AggregateFunction withAggregateParam(AggregateParam aggregateParam) {
        if (arity() == 0) {
            return new CountByEnum(aggregateParam);
        } else {
            return new CountByEnum(aggregateParam, child(0));
        }
    }

    @Override
    protected List<DataType> intermediateTypes(List<DataType> argumentTypes, List<Expression> arguments) {
        return ImmutableList.of(StringType.INSTANCE);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCountByEnum(this, context);
    }

    @Override
    public FunctionSignature customSignature(List<DataType> argumentTypes, List<Expression> arguments) {
        return FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE);
    }
}
