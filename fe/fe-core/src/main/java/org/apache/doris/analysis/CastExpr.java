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
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Map;


public class CastExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(CastExpr.class);

    // Only set for explicit casts. Null for implicit casts.
    @SerializedName("ttd")
    protected TypeDef targetTypeDef;

    // True if this is a "pre-analyzed" implicit cast.
    @SerializedName("ii")
    protected boolean isImplicit;

    // True if this cast does not change the type.
    protected boolean noOp = false;

    protected boolean notFold = false;

    protected static final Map<Pair<Type, Type>, Function.NullableMode> TYPE_NULLABLE_MODE;

    static {
        TYPE_NULLABLE_MODE = Maps.newHashMap();
        for (ScalarType fromType : Type.getSupportedTypes()) {
            if (fromType.isNull()) {
                continue;
            }
            for (ScalarType toType : Type.getSupportedTypes()) {
                if (toType.isNull()) {
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
    private CastExpr() {

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
        Preconditions.checkArgument(targetType.isValid());
        Preconditions.checkNotNull(e, "cast child is null");
        opcode = TExprOpcode.CAST;
        type = targetType;
        targetTypeDef = null;
        isImplicit = true;
        children.add(e);

        noOp = Type.matchExactType(e.type, type, true);
        if (noOp) {
            // For decimalv2, we do not perform an actual cast between different precision/scale. Instead, we just
            // set the target type as the child's type.
            if (type.isDecimalV2() && e.type.isDecimalV2()) {
                getChild(0).setType(type);
            }
            // as the targetType have struct field name, if use the default name will be
            // like col1,col2, col3... in struct, and the filed name is import in BE.
            if (type.isStructType() && e.type.isStructType()) {
                getChild(0).setType(type);
            }
            if (type.isScalarType()) {
                targetTypeDef = new TypeDef(type);
            }
            analysisDone();
            return;
        }

        if (e.type.isNull()) {
            analysisDone();
            return;
        }

        // new function
        if (type.isScalarType()) {
            Type from = getActualArgTypes(collectChildReturnTypes())[0];
            Type to = getActualType(type);
            NullableMode nullableMode = TYPE_NULLABLE_MODE.get(Pair.of(from, to));
            // for complex type cast to jsonb we make ret is always nullable
            if (from.isComplexType() && type.isJsonbType()) {
                nullableMode = Function.NullableMode.ALWAYS_NULLABLE;
            }
            fn = new Function(new FunctionName(getFnName(type)), Lists.newArrayList(e.type), type,
                    false, true, nullableMode);
        } else {
            createComplexTypeCastFunction();
        }

        analysisDone();
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

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type fromType : Type.getTrivialTypes()) {
            if (fromType.isNull()) {
                continue;
            }
            for (Type toType : Type.getTrivialTypes()) {
                functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(getFnName(toType),
                        toType, TYPE_NULLABLE_MODE.get(Pair.of(fromType, toType)),
                        Lists.newArrayList(fromType), false,
                        null, null, null, true));
            }
        }
    }

    @Override
    public Expr clone() {
        return new CastExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (isAnalyzed) {
            return "CAST(" + getChild(0).toSql() + " AS " + type.toSql() + ")";
        } else {
            return "CAST(" + getChild(0).toSql() + " AS "
                    + (isImplicit ? type.toString() : targetTypeDef.toSql()) + ")";
        }
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        if (needExternalSql) {
            return getChild(0).toSql(disableTableName, needExternalSql, tableType, table);
        }
        if (isAnalyzed) {
            return "CAST(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " AS "
                    + type.toSql() + ")";
        } else {
            return "CAST(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " AS "
                    + (isImplicit ? type.toString() : targetTypeDef.toSql()) + ")";
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
            return "CAST(" + getChild(0).toDigest() + " AS " + type.toString() + ")";
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

    private void createComplexTypeCastFunction() {
        if (type.isArrayType()) {
            fn = ScalarFunction.createBuiltin(getFnName(Type.ARRAY),
                    type, Function.NullableMode.ALWAYS_NULLABLE,
                    Lists.newArrayList(getActualArgTypes(collectChildReturnTypes())[0]), false,
                    "doris::CastFunctions::cast_to_array_val", null, null, true);
        } else if (type.isMapType()) {
            fn = ScalarFunction.createBuiltin(getFnName(Type.MAP),
                    type, Function.NullableMode.ALWAYS_NULLABLE,
                    Lists.newArrayList(getActualArgTypes(collectChildReturnTypes())[0]), false,
                    "doris::CastFunctions::cast_to_map_val", null, null, true);
        } else if (type.isStructType()) {
            fn = ScalarFunction.createBuiltin(getFnName(Type.STRUCT),
                    type, Function.NullableMode.ALWAYS_NULLABLE,
                    Lists.newArrayList(Type.VARCHAR), false,
                    "doris::CastFunctions::cast_to_struct_val", null, null, true);
        }
    }

    public void analyze() throws AnalysisException {
        // do not analyze ALL cast
        if (type == Type.ALL) {
            return;
        }
        // cast was asked for in the query, check for validity of cast
        Type childType = getChild(0).getType();

        // this cast may result in loss of precision, but the user requested it
        noOp = Type.matchExactType(childType, type, true);

        if (noOp) {
            // For decimalv2, we do not perform an actual cast between different precision/scale. Instead, we just
            // set the target type as the child's type.
            if (type.isDecimalV2() && childType.isDecimalV2()) {
                getChild(0).setType(type);
            }
            return;
        }

        // select stmt will make BE coredump when its castExpr is like cast(int as array<>),
        // it is necessary to check if it is castable before creating fn.
        // char type will fail in canCastTo, so for compatibility, only the cast of array type is checked here.
        if (type.isArrayType() || childType.isArrayType()) {
            if (!Type.canCastTo(childType, type)) {
                throw new AnalysisException("Invalid type cast of " + getChild(0).toSql()
                        + " from " + childType + " to " + type);
            }
        }

        this.opcode = TExprOpcode.CAST;
        FunctionName fnName = new FunctionName(getFnName(type));
        Function searchDesc = new Function(fnName, Arrays.asList(getActualArgTypes(collectChildReturnTypes())),
                Type.INVALID, false);
        if (type.isScalarType()) {
            fn = Env.getCurrentEnv().getFunction(searchDesc, Function.CompareMode.IS_IDENTICAL);
        } else {
            createComplexTypeCastFunction();
        }

        if (fn == null) {
            //TODO(xy): check map type
            if ((type.isMapType() || type.isStructType()) && childType.isStringType()) {
                return;
            }
            // same with Type.canCastTo() can be cast to jsonb
            if (childType.isComplexType() && type.isJsonbType()) {
                return;
            }
            if (childType.isNull() && Type.canCastTo(childType, type)) {
                return;
            } else {
                throw new AnalysisException("Invalid type cast of " + getChild(0).toSql()
                    + " from " + childType + " to " + type);
            }
        }

        if (PrimitiveType.typeWithPrecision.contains(type.getPrimitiveType())) {
            Preconditions.checkState(type.isDecimalV3() == fn.getReturnType().isDecimalV3()
                            || type.isDatetimeV2() == fn.getReturnType().isDatetimeV2(),
                    type + " != " + fn.getReturnType());
        } else {
            Preconditions.checkState(type.matchesType(fn.getReturnType()), type + " != " + fn.getReturnType());
        }
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
    public boolean isNullable() {
        return children.get(0).isNullable()
                || (children.get(0).getType().isStringType() && !getType().isStringType())
                || (!children.get(0).getType().isDateType() && getType().isDateType());
    }

    @Override
    public String getStringValueForStreamLoad(FormatOptions options) {
        return children.get(0).getStringValueForStreamLoad(options);
    }

    @Override
    protected String getStringValueInComplexTypeForQuery(FormatOptions options) {
        return children.get(0).getStringValueInComplexTypeForQuery(options);
    }

    public void setNotFold(boolean notFold) {
        this.notFold = notFold;
    }
}
