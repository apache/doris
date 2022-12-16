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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.ResponsibilityChain;

import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** BoundFunction. */
public abstract class BoundFunction extends Expression implements FunctionTrait, ComputeSignature {
    private final String name;

    private final Supplier<FunctionSignature> signatureCache = Suppliers.memoize(() -> {
        // first step: find the candidate signature in the signature list
        List<Expression> originArguments = getOriginArguments();
        FunctionSignature matchedSignature = searchSignature(
                getOriginArgumentTypes(), originArguments, getSignatures());
        // second step: change the signature, e.g. fill precision for decimal v2
        return computeSignature(matchedSignature, originArguments);
    });

    public BoundFunction(String name, Expression... arguments) {
        super(arguments);
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    public BoundFunction(String name, List<Expression> children) {
        super(children);
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    protected FunctionSignature computeSignature(FunctionSignature signature, List<Expression> arguments) {
        // NOTE:
        // this computed chain only process the common cases.
        // If you want to add some common cases to here, please separate the process code
        // to the other methods and add to this chain.
        // If you want to add some special cases, please override this method in the special
        // function class, like 'If' function and 'Substring' function.
        return ComputeSignatureChain.from(signature, arguments)
                .then(this::computePrecisionForDatetimeV2)
                .then(this::upgradeDateOrDateTimeToV2)
                .then(this::upgradeDecimalV2ToV3)
                .then(this::computePrecisionForDecimal)
                .then(this::dynamicComputePropertiesOfArray)
                .get();
    }

    public String getName() {
        return name;
    }

    public FunctionSignature getSignature() {
        return signatureCache.get();
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return ComputeSignature.super.expectedInputTypes();
    }

    @Override
    public DataType getDataType() {
        return ComputeSignature.super.getDataType();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBoundFunction(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoundFunction that = (BoundFunction) o;
        return Objects.equals(name, that.name) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, children);
    }

    @Override
    public String toSql() throws UnboundException {
        String args = children()
                .stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", "));
        return name + "(" + args + ")";
    }

    @Override
    public String toString() {
        String args = children()
                .stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", "));
        return name + "(" + args + ")";
    }

    private FunctionSignature computePrecisionForDatetimeV2(
            FunctionSignature signature, List<Expression> arguments) {

        // fill for arguments type
        signature = signature.withArgumentTypes(arguments, (sigArgType, realArgType) -> {
            if (sigArgType instanceof DateTimeV2Type && realArgType.getDataType() instanceof DateTimeV2Type) {
                return realArgType.getDataType();
            }
            return sigArgType;
        });

        // fill for return type
        if (signature.returnType instanceof DateTimeV2Type) {
            Integer maxScale = signature.argumentsTypes.stream()
                    .filter(DateTimeV2Type.class::isInstance)
                    .map(t -> ((DateTimeV2Type) t).getScale())
                    .reduce(Math::max)
                    .orElse(((DateTimeV2Type) signature.returnType).getScale());
            signature = signature.withReturnType(DateTimeV2Type.of(maxScale));
        }

        return signature;
    }

    private FunctionSignature upgradeDateOrDateTimeToV2(
            FunctionSignature signature, List<Expression> arguments) {
        DataType returnType = signature.returnType;
        Type type = returnType.toCatalogDataType();
        if ((type.isDate() || type.isDatetime()) && Config.enable_date_conversion) {
            Type legacyReturnType = ScalarType.getDefaultDateType(returnType.toCatalogDataType());
            signature = signature.withReturnType(DataType.fromCatalogType(legacyReturnType));
        }
        return signature;
    }

    @Developing
    private FunctionSignature computePrecisionForDecimal(
            FunctionSignature signature, List<Expression> arguments) {
        if (signature.returnType instanceof DecimalV3Type || signature.returnType instanceof DecimalV2Type) {
            if (this instanceof DecimalSamePrecision) {
                signature = signature.withReturnType(arguments.get(0).getDataType());
            } else if (this instanceof DecimalWiderPrecision) {
                ScalarType widerType = ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                        ((ScalarType) arguments.get(0).getDataType().toCatalogDataType()).getScalarScale());
                signature = signature.withReturnType(DataType.fromCatalogType(widerType));
            } else if (this instanceof DecimalStddevPrecision) {
                // for all stddev function, use decimal(38,9) as computing result
                ScalarType stddevDecimalType = ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                        DecimalStddevPrecision.STDDEV_DECIMAL_SCALE);
                signature = signature.withReturnType(DataType.fromCatalogType(stddevDecimalType));
            }
        }

        return signature;
    }

    private FunctionSignature upgradeDecimalV2ToV3(
            FunctionSignature signature, List<Expression> arguments) {
        DataType returnType = signature.returnType;
        Type type = returnType.toCatalogDataType();
        if (type.isDecimalV2() && Config.enable_decimal_conversion) {
            Type v3Type = ScalarType.createDecimalV3Type(type.getPrecision(), ((ScalarType) type).getScalarScale());
            signature = signature.withReturnType(DataType.fromCatalogType(v3Type));
        }
        return signature;
    }

    private FunctionSignature dynamicComputePropertiesOfArray(
            FunctionSignature signature, List<Expression> arguments) {
        if (!(signature.returnType instanceof ArrayType)) {
            return signature;
        }

        // TODO: compute array(...) function's itemType

        // fill item type by the type of first item
        ArrayType arrayType = (ArrayType) signature.returnType;

        // fill containsNull if any array argument contains null
        boolean containsNull = signature.argumentsTypes
                .stream()
                .filter(argType -> argType instanceof ArrayType)
                .map(ArrayType.class::cast)
                .anyMatch(ArrayType::containsNull);
        return signature.withReturnType(
                ArrayType.of(arrayType.getItemType(), arrayType.containsNull() || containsNull));
    }

    static class ComputeSignatureChain {
        private ResponsibilityChain<Pair<FunctionSignature, List<Expression>>> computeChain;

        public ComputeSignatureChain(ResponsibilityChain<Pair<FunctionSignature, List<Expression>>> computeChain) {
            this.computeChain = computeChain;
        }

        public static ComputeSignatureChain from(FunctionSignature signature, List<Expression> arguments) {
            return new ComputeSignatureChain(ResponsibilityChain.from(Pair.of(signature, arguments)));
        }

        public ComputeSignatureChain then(
                BiFunction<FunctionSignature, List<Expression>, FunctionSignature> computeFunction) {
            computeChain.then(pair -> Pair.of(computeFunction.apply(pair.first, pair.second), pair.second));
            return this;
        }

        public FunctionSignature get() {
            return computeChain.get().first;
        }
    }
}
