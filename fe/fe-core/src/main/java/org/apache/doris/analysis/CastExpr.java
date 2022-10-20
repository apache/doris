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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/CastExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class CastExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(CastExpr.class);

    // Only set for explicit casts. Null for implicit casts.
    private TypeDef targetTypeDef;

    // True if this is a "pre-analyzed" implicit cast.
    private boolean isImplicit;

    // True if this cast does not change the type.
    private boolean noOp = false;

    private static final Map<Pair<Type, Type>, Function.NullableMode> TYPE_NULLABLE_MODE;

    static {
        TYPE_NULLABLE_MODE = Maps.newHashMap();
        for (ScalarType fromType : Type.getSupportedTypes()) {
            if (fromType.isNull()) {
                continue;
            }
            for (ScalarType toType : Type.getSupportedTypes()) {
                if (fromType.isNull()) {
                    continue;
                }
                if (fromType.isStringType() && !toType.isStringType()) {
                    TYPE_NULLABLE_MODE.put(Pair.of(fromType, toType), Function.NullableMode.ALWAYS_NULLABLE);
                } else if (!fromType.isDateType() && toType.isDateType()) {
                    TYPE_NULLABLE_MODE.put(Pair.of(fromType, toType), Function.NullableMode.ALWAYS_NULLABLE);
                } else {
                    TYPE_NULLABLE_MODE.put(Pair.of(fromType, toType), Function.NullableMode.DEPEND_ON_ARGUMENT);
                }
            }
        }
    }

    // only used restore from readFields.
    public CastExpr() {

    }

    public CastExpr(Type targetType, Expr e) {
        super();
        Preconditions.checkArgument(targetType.isValid());
        Preconditions.checkNotNull(e);
        type = targetType;
        targetTypeDef = null;
        isImplicit = true;

        children.add(e);

        try {
            analyze();
        } catch (AnalysisException ex) {
            LOG.warn("Implicit casts fail", ex);
            Preconditions.checkState(false,
                    "Implicit casts should never throw analysis exception.");
        }
        analysisDone();
    }

    /**
     * Just use for nereids, put analyze() in finalizeImplForNereids
     */
    public CastExpr(Type targetType, Expr e, Void v) {
        super();
        Preconditions.checkArgument(targetType.isValid());
        Preconditions.checkNotNull(e);
        type = targetType;
        targetTypeDef = null;
        isImplicit = true;
        children.add(e);
    }

    /**
     * Copy c'tor used in clone().
     */
    public CastExpr(TypeDef targetTypeDef, Expr e) {
        Preconditions.checkNotNull(targetTypeDef);
        Preconditions.checkNotNull(e);
        this.targetTypeDef = targetTypeDef;
        isImplicit = false;
        children.add(e);
    }

    protected CastExpr(CastExpr other) {
        super(other);
        targetTypeDef = other.targetTypeDef;
        isImplicit = other.isImplicit;
        noOp = other.noOp;
    }

    private static String getFnName(Type targetType) {
        return "castTo" + targetType.getPrimitiveType().toString();
    }

    public TypeDef getTargetTypeDef() {
        return targetTypeDef;
    }

    private static boolean disableRegisterCastingFunction(Type fromType, Type toType) {
        // Disable casting from boolean to decimal or datetime or date
        if (fromType.isBoolean() && (toType.equals(Type.DECIMALV2) || toType.isDecimalV3()
                || toType.isDateType())) {
            return true;
        }

        // Disable casting operation of hll/bitmap/quantile_state
        if (fromType.isObjectStored() || toType.isObjectStored()) {
            return true;
        }
        // Disable no-op casting
        return fromType.equals(toType);
    }

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type fromType : Type.getSupportedTypes()) {
            if (fromType.isNull()) {
                continue;
            }
            for (Type toType : Type.getSupportedTypes()) {
                if (toType.isNull() || disableRegisterCastingFunction(fromType, toType)) {
                    continue;
                }
                String beClass = toType.isDecimalV2() || fromType.isDecimalV2()
                        ? "DecimalV2Operators" : "CastFunctions";
                if (fromType.isTime()) {
                    beClass = "TimeOperators";
                }
                String typeName = Function.getUdfTypeName(toType.getPrimitiveType());
                // only refactor date/datetime for vectorized engine.
                if (toType.getPrimitiveType() == PrimitiveType.DATE) {
                    typeName = "date_val";
                }
                if (toType.getPrimitiveType() == PrimitiveType.DATEV2) {
                    typeName = "datev2_val";
                }
                if (toType.getPrimitiveType() == PrimitiveType.DATETIMEV2) {
                    typeName = "datetimev2_val";
                }
                String beSymbol = "doris::" + beClass + "::cast_to_"
                        + typeName;
                functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(getFnName(toType),
                        toType, TYPE_NULLABLE_MODE.get(Pair.of(fromType, toType)),
                        Lists.newArrayList(fromType), false,
                        beSymbol, null, null, true));
            }
        }
    }

    @Override
    public Expr clone() {
        return new CastExpr(this);
    }

    @Override
    public String toSqlImpl() {
        boolean isVerbose = ConnectContext.get() != null
                && ConnectContext.get().getExecutor() != null
                && ConnectContext.get().getExecutor().getParsedStmt() != null
                && ConnectContext.get().getExecutor().getParsedStmt().getExplainOptions() != null
                && ConnectContext.get().getExecutor().getParsedStmt().getExplainOptions().isVerbose();
        if (isImplicit && !isVerbose) {
            return getChild(0).toSql();
        }
        if (isAnalyzed) {
            if (type.isStringType()) {
                return "CAST(" + getChild(0).toSql() + " AS " + "CHARACTER" + ")";
            } else {
                return "CAST(" + getChild(0).toSql() + " AS " + type.toString() + ")";
            }
        } else {
            return "CAST(" + getChild(0).toSql() + " AS " + targetTypeDef.toSql() + ")";
        }
    }

    @Override
    public String toDigestImpl() {
        boolean isVerbose = ConnectContext.get() != null
                && ConnectContext.get().getExecutor() != null
                && ConnectContext.get().getExecutor().getParsedStmt() != null
                && ConnectContext.get().getExecutor().getParsedStmt().getExplainOptions() != null
                && ConnectContext.get().getExecutor().getParsedStmt().getExplainOptions().isVerbose();
        if (isImplicit && !isVerbose) {
            return getChild(0).toDigest();
        }
        if (isAnalyzed) {
            if (type.isStringType()) {
                return "CAST(" + getChild(0).toDigest() + " AS " + "CHARACTER" + ")";
            } else {
                return "CAST(" + getChild(0).toDigest() + " AS " + type.toString() + ")";
            }
        } else {
            return "CAST(" + getChild(0).toDigest() + " AS " + targetTypeDef.toString() + ")";
        }
    }

    @Override
    protected void treeToThriftHelper(TExpr container) {
        if (noOp) {
            getChild(0).treeToThriftHelper(container);
            return;
        }
        super.treeToThriftHelper(container);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.CAST_EXPR;
        msg.setOpcode(opcode);
        msg.setOutputColumn(outputColumn);
        if (type.isNativeType() && getChild(0).getType().isNativeType()) {
            msg.setChildType(getChild(0).getType().getPrimitiveType().toThrift());
        }
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    public void setImplicit(boolean implicit) {
        isImplicit = implicit;
    }

    public void analyze() throws AnalysisException {
        // do not analyze ALL cast
        if (type == Type.ALL) {
            return;
        }
        // cast was asked for in the query, check for validity of cast
        Type childType = getChild(0).getType();

        // this cast may result in loss of precision, but the user requested it
        noOp = Type.matchExactType(childType, type);

        if (noOp) {
            return;
        }

        // select stmt will make BE coredump when its castExpr is like cast(int as array<>),
        // it is necessary to check if it is castable before creating fn.
        // char type will fail in canCastTo, so for compatibility, only the cast of array type is checked here.
        if (type.isArrayType() || childType.isArrayType()) {
            if (childType.isNull() || !Type.canCastTo(childType, type)) {
                throw new AnalysisException("Invalid type cast of " + getChild(0).toSql()
                        + " from " + childType + " to " + type);
            }
        }

        this.opcode = TExprOpcode.CAST;
        FunctionName fnName = new FunctionName(getFnName(type));
        Function searchDesc = new Function(fnName, Arrays.asList(collectChildReturnTypes()), Type.INVALID, false);
        if (type.isScalarType()) {
            if (isImplicit) {
                fn = Env.getCurrentEnv().getFunction(
                        searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            } else {
                fn = Env.getCurrentEnv().getFunction(
                        searchDesc, Function.CompareMode.IS_IDENTICAL);
            }
        } else if (type.isArrayType()) {
            fn = ScalarFunction.createBuiltin(getFnName(Type.ARRAY),
                    type, Function.NullableMode.ALWAYS_NULLABLE,
                    Lists.newArrayList(Type.VARCHAR), false,
                    "doris::CastFunctions::cast_to_array_val", null, null, true);
        }

        if (fn == null) {
            throw new AnalysisException("Invalid type cast of " + getChild(0).toSql()
                    + " from " + childType + " to " + type);
        }

        if (PrimitiveType.typeWithPrecision.contains(type.getPrimitiveType())) {
            Preconditions.checkState(type.getPrimitiveType() == fn.getReturnType().getPrimitiveType(),
                    type + " != " + fn.getReturnType());
        } else {
            Preconditions.checkState(type.matchesType(fn.getReturnType()), type + " != " + fn.getReturnType());
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (isImplicit) {
            return;
        }
        // When cast target type is string and it's length is default -1, the result length
        // of cast is decided by child.
        if (targetTypeDef.getType().isScalarType()) {
            final ScalarType targetType = (ScalarType) targetTypeDef.getType();
            if (!(targetType.getPrimitiveType().isStringType() && !targetType.isLengthSet())) {
                targetTypeDef.analyze(analyzer);
            }
        } else {
            targetTypeDef.analyze(analyzer);
        }
        type = targetTypeDef.getType();
        analyze();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        CastExpr expr = (CastExpr) obj;
        return this.opcode == expr.opcode;
    }

    /**
     * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
     */
    @Override
    public Expr ignoreImplicitCast() {
        if (isImplicit) {
            // we don't expect to see to consecutive implicit casts
            Preconditions.checkState(!(getChild(0) instanceof CastExpr)
                    || !((CastExpr) getChild(0)).isImplicit());
            return getChild(0);
        } else {
            return this;
        }
    }

    public boolean canHashPartition() {
        if (type.isFixedPointType() && getChild(0).getType().isFixedPointType()) {
            return true;
        }
        if (type.isDateType() && getChild(0).getType().isDateType()) {
            return true;
        }
        return false;
    }

    @Override
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        final Expr value = children.get(0);
        if (!(value instanceof LiteralExpr)) {
            return this;
        }
        Expr targetExpr;
        try {
            targetExpr = castTo((LiteralExpr) value);
        } catch (AnalysisException ae) {
            targetExpr = this;
        } catch (NumberFormatException nfe) {
            targetExpr = new NullLiteral();
        }
        return targetExpr;
    }

    private Expr castTo(LiteralExpr value) throws AnalysisException {
        if (value instanceof NullLiteral) {
            return value;
        } else if (type.isIntegerType()) {
            return new IntLiteral(value.getLongValue(), type);
        } else if (type.isLargeIntType()) {
            return new LargeIntLiteral(value.getStringValue());
        } else if (type.isDecimalV2() || type.isDecimalV3()) {
            return new DecimalLiteral(value.getStringValue());
        } else if (type.isFloatingPointType()) {
            return new FloatLiteral(value.getDoubleValue(), type);
        } else if (type.isStringType()) {
            return new StringLiteral(value.getStringValue());
        } else if (type.isDateType()) {
            return new StringLiteral(value.getStringValue()).convertToDate(type);
        } else if (type.isBoolean()) {
            return new BoolLiteral(value.getStringValue());
        }
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isImplicit);
        if (targetTypeDef.getType() instanceof ScalarType) {
            ScalarType scalarType = (ScalarType) targetTypeDef.getType();
            scalarType.write(out);
        } else {
            throw new IOException("Can not write type " + targetTypeDef.getType());
        }
        out.writeInt(children.size());
        for (Expr expr : children) {
            Expr.writeTo(expr, out);
        }
    }

    public static CastExpr read(DataInput input) throws IOException {
        CastExpr castExpr = new CastExpr();
        castExpr.readFields(input);
        return castExpr;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isImplicit = in.readBoolean();
        ScalarType scalarType = ScalarType.read(in);
        targetTypeDef = new TypeDef(scalarType);
        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            children.add(Expr.readIn(in));
        }
    }

    public CastExpr rewriteExpr(List<String> parameters, List<Expr> inputParamsExprs) throws AnalysisException {
        // child
        Expr child = this.getChild(0);
        Expr newChild = null;
        if (child instanceof SlotRef) {
            String columnName = ((SlotRef) child).getColumnName();
            int index = parameters.indexOf(columnName);
            if (index != -1) {
                newChild = inputParamsExprs.get(index);
            }
        }
        // rewrite cast expr in children
        if (child instanceof CastExpr) {
            newChild = ((CastExpr) child).rewriteExpr(parameters, inputParamsExprs);
        }

        // type def
        ScalarType targetType = (ScalarType) targetTypeDef.getType();
        PrimitiveType primitiveType = targetType.getPrimitiveType();
        ScalarType newTargetType = null;
        switch (primitiveType) {
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                // normal decimal
                if (targetType.getPrecision() != 0) {
                    newTargetType = targetType;
                    break;
                }
                int precision = getDigital(targetType.getScalarPrecisionStr(), parameters, inputParamsExprs);
                int scale = getDigital(targetType.getScalarScaleStr(), parameters, inputParamsExprs);
                if (precision != -1 && scale != -1) {
                    newTargetType = ScalarType.createType(primitiveType, 0, precision, scale);
                } else if (precision != -1) {
                    newTargetType = ScalarType.createType(primitiveType, 0, precision, ScalarType.DEFAULT_SCALE);
                }
                break;
            case CHAR:
            case VARCHAR:
                // normal char/varchar
                if (targetType.getLength() != -1) {
                    newTargetType = targetType;
                    break;
                }
                int len = getDigital(targetType.getLenStr(), parameters, inputParamsExprs);
                if (len != -1) {
                    newTargetType = ScalarType.createType(primitiveType, len, 0, 0);
                }
                // default char/varchar, which len is -1
                if (len == -1 && targetType.getLength() == -1) {
                    newTargetType = targetType;
                }
                break;
            default:
                newTargetType = targetType;
                break;
        }

        if (newTargetType != null && newChild != null) {
            TypeDef typeDef = new TypeDef(newTargetType);
            return new CastExpr(typeDef, newChild);
        }

        return this;
    }

    private int getDigital(String desc, List<String> parameters, List<Expr> inputParamsExprs) {
        int index = parameters.indexOf(desc);
        if (index != -1) {
            Expr expr = inputParamsExprs.get(index);
            if (expr.getType().isIntegerType()) {
                return ((Long) ((IntLiteral) expr).getRealValue()).intValue();
            }
        }
        return -1;
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable()
                || (children.get(0).getType().isStringType() && !getType().isStringType())
                || (!children.get(0).getType().isDateType() && getType().isDateType());
    }

    @Override
    public void finalizeImplForNereids() throws AnalysisException {
        try {
            analyze();
        } catch (AnalysisException ex) {
            LOG.warn("Implicit casts fail", ex);
            Preconditions.checkState(false,
                    "Implicit casts should never throw analysis exception.");
        }
        FunctionName fnName = new FunctionName(getFnName(type));
        Function searchDesc = new Function(fnName, Arrays.asList(collectChildReturnTypes()), Type.INVALID, false);
        if (type.isScalarType()) {
            if (isImplicit) {
                fn = Env.getCurrentEnv().getFunction(
                        searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            } else {
                fn = Env.getCurrentEnv().getFunction(
                        searchDesc, Function.CompareMode.IS_IDENTICAL);
            }
        } else if (type.isArrayType()) {
            fn = ScalarFunction.createBuiltin(getFnName(Type.ARRAY),
                    type, Function.NullableMode.ALWAYS_NULLABLE,
                    Lists.newArrayList(Type.VARCHAR), false,
                    "doris::CastFunctions::cast_to_array_val", null, null, true);
        }
    }
}
