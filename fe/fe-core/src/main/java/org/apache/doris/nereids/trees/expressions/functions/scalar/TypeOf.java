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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** ScalarFunction 'typeof', returning the static type of its argument. */
public class TypeOf extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    public TypeOf(Expression arg) {
        super("typeof", arg);
    }

    private TypeOf(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public TypeOf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new TypeOf(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTypeOf(this, context);
    }

    /** Return the name used by Presto's typeof function for a Nereids type. */
    public static String typeName(DataType type) {
        if (type instanceof NullType) {
            return "unknown";
        } else if (type instanceof BooleanType) {
            return "boolean";
        } else if (type instanceof TinyIntType) {
            return "tinyint";
        } else if (type instanceof SmallIntType) {
            return "smallint";
        } else if (type instanceof IntegerType) {
            return "integer";
        } else if (type instanceof BigIntType) {
            return "bigint";
        } else if (type instanceof LargeIntType) {
            return "decimal(38,0)";
        } else if (type instanceof FloatType) {
            return "real";
        } else if (type instanceof DoubleType) {
            return "double";
        } else if (type instanceof StringType) {
            return "varchar";
        } else if (type instanceof VarcharType) {
            return ((VarcharType) type).getLen() >= 0
                    ? "varchar(" + ((VarcharType) type).getLen() + ")" : "varchar";
        } else if (type instanceof CharType) {
            return ((CharType) type).getLen() >= 0
                    ? "char(" + ((CharType) type).getLen() + ")" : "char";
        } else if (type instanceof DecimalV2Type) {
            DecimalV2Type decimal = (DecimalV2Type) type;
            return "decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
        } else if (type instanceof DecimalV3Type) {
            DecimalV3Type decimal = (DecimalV3Type) type;
            return "decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
        } else if (type instanceof DateType || type instanceof DateV2Type) {
            return "date";
        } else if (type instanceof DateTimeType || type instanceof DateTimeV2Type
                || type instanceof TimeStampNsType) {
            return "timestamp";
        } else if (type instanceof TimeStampTzType) {
            return "timestamp with time zone";
        } else if (type instanceof TimeV2Type) {
            return "time";
        } else if (type instanceof IPv4Type) {
            return "ipv4";
        } else if (type instanceof IPv6Type) {
            return "ipv6";
        } else if (type instanceof VarBinaryType) {
            return "varbinary";
        } else if (type instanceof JsonType) {
            return "json";
        } else if (type instanceof VariantType) {
            return "variant";
        } else if (type instanceof BitmapType) {
            return "bitmap";
        } else if (type instanceof HllType) {
            return "hll";
        } else if (type instanceof QuantileStateType) {
            return "quantile_state";
        } else if (type instanceof AggStateType) {
            return "agg_state";
        } else if (type instanceof ArrayType) {
            return "array(" + typeName(((ArrayType) type).getItemType()) + ")";
        } else if (type instanceof MapType) {
            MapType map = (MapType) type;
            return "map(" + typeName(map.getKeyType()) + ", " + typeName(map.getValueType()) + ")";
        } else if (type instanceof StructType) {
            return "row(" + ((StructType) type).getFields().stream()
                    .map(field -> "\"" + field.getName() + "\" " + typeName(field.getDataType()))
                    .collect(Collectors.joining(", ")) + ")";
        }
        return type.simpleString().toLowerCase(Locale.ROOT);
    }
}
