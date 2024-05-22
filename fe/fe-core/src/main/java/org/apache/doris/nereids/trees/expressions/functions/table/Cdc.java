package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.tablefunction.NumbersTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import java.util.Map;

public class Cdc extends TableValuedFunction{
    public Cdc(Properties properties) {
        super("cdc", properties);
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return new NumbersTableValuedFunction(arguments);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build NumbersTableValuedFunction by "
                + this + ": " + t.getMessage(), t);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCdc(this, context);
    }
}
