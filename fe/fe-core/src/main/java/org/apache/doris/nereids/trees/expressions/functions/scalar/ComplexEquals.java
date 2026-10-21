// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'complex_equals'. Compares two values of the same complex type recursively.
 *
 * <p>The comparison is NULL safe: two NULL values, including a NULL nested field, are equal, while
 * a NULL and a non-NULL value are not. This is the equality the load mapping needs for a nested
 * value; the caller keeps the SQL three-valued behavior of a top level NULL search itself.
 */
public class ComplexEquals extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(new AnyDataType(0), new AnyDataType(0)));

    /**
     * constructor with 2 arguments.
     */
    public ComplexEquals(Expression arg0, Expression arg1) {
        super("complex_equals", arg0, arg1);
    }

    private ComplexEquals(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public ComplexEquals withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ComplexEquals(getFunctionParams(children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkComparableArguments();
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkComparableArguments();
    }

    private void checkComparableArguments() {
        for (Expression argument : getArguments()) {
            DataType type = argument.getDataType();
            if (!supportsColumnComparison(type)) {
                throw new AnalysisException(getName() + " does not support argument type " + type.toSql());
            }
        }
    }

    /** The BE implementation calls Column::compare_at on every non-NULL leaf. */
    private static boolean supportsColumnComparison(DataType type) {
        if (type.isArrayType()) {
            return supportsColumnComparison(((ArrayType) type).getItemType());
        }
        if (type.isMapType()) {
            MapType map = (MapType) type;
            return supportsColumnComparison(map.getKeyType())
                    && supportsColumnComparison(map.getValueType());
        }
        if (type.isStructType()) {
            for (StructField field : ((StructType) type).getFields()) {
                if (!supportsColumnComparison(field.getDataType())) {
                    return false;
                }
            }
            return true;
        }
        return type.isNullType() || (type.isPrimitive() && !type.isJsonType()
                && !type.isObjectOrVariantType() && !type.isAggStateType());
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
