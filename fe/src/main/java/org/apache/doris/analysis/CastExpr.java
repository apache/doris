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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CastExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(CastExpr.class);

    // Only set for explicit casts. Null for implicit casts.
    private final TypeDef targetTypeDef;

    // True if this is a "pre-analyzed" implicit cast.
    private final boolean isImplicit;

    // True if this cast does not change the type.
    private boolean noOp = false;

    public CastExpr(Type targetType, Expr e) {
        super();
        Preconditions.checkArgument(targetType.isValid());
        Preconditions.checkNotNull(e);
        type = targetType;
        targetTypeDef = null;
        isImplicit = true;

        children.add(e);
        if (isImplicit) {
            try {
                analyze();
            } catch (AnalysisException ex) {
                Preconditions.checkState(false,
                        "Implicit casts should never throw analysis exception.");
            }
            analysisDone();
        }
    }

    /**
     * Copy c'tor used in clone().
     */
    protected CastExpr(TypeDef targetTypeDef, Expr e) {
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

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type fromType : Type.getSupportedTypes()) {
            if (fromType.isNull()) {
                continue;
            }
            for (Type toType : Type.getSupportedTypes()) {
                if (toType.isNull()) {
                    continue;
                }
                // Disable casting from string to boolean
                if (fromType.isStringType() && toType.isBoolean()) {
                    continue;
                }
                // Disable casting from boolean to decimal or datetime or date
                if (fromType.isBoolean() &&
                        (toType == Type.DECIMAL || toType == Type.DECIMALV2 ||
                                toType == Type.DATETIME || toType == Type.DATE)) {
                    continue;
                }
                // Disable no-op casts
                if (fromType.equals(toType)) {
                    continue;
                }
                String beClass = toType.isDecimalV2() || fromType.isDecimalV2() ? "DecimalV2Operators" : "CastFunctions";
                if (toType.isDecimal() || fromType.isDecimal()) {
                    beClass = "DecimalOperators";
                } else if (fromType.isTime()) {
                    beClass = "TimeOperators";
                }
                String typeName = Function.getUdfTypeName(toType.getPrimitiveType());
                if (toType.getPrimitiveType() == PrimitiveType.DATE) {
                    typeName = "date_val";
                }
                String beSymbol = "doris::" + beClass + "::cast_to_"
                        + typeName;
                functionSet.addBuiltin(ScalarFunction.createBuiltin(getFnName(toType),
                        Lists.newArrayList(fromType), false, toType, beSymbol, null, null, true));
            }
        }
    }

    @Override
    public Expr clone() {
        return new CastExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (isImplicit) {
            return getChild(0).toSql();
        }
        if (type.isStringType()) {
            return "CAST(" + getChild(0).toSql() + " AS " + "CHARACTER" + ")";
        } else {
            return "CAST(" + getChild(0).toSql() + " AS " + type.toString() + ")";
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
        msg.setOutput_column(outputColumn);
        if (type.isNativeType() && getChild(0).getType().isNativeType()) {
            msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
        }
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    public void analyze() throws AnalysisException {
        // cast was asked for in the query, check for validity of cast
        Type childType = getChild(0).getType();

        // this cast may result in loss of precision, but the user requested it
        if (childType.matchesType(type)) {
            noOp = true;
            return;
        }

        this.opcode = TExprOpcode.CAST;
        FunctionName fnName = new FunctionName(getFnName(type));
        Function searchDesc = new Function(fnName, collectChildReturnTypes(), Type.INVALID, false);
        if (isImplicit) {
            fn = Catalog.getInstance().getFunction(
                    searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else {
            fn = Catalog.getInstance().getFunction(
                    searchDesc, Function.CompareMode.IS_IDENTICAL);
        }

        if (fn == null) {
            throw new AnalysisException("Invalid type cast of " + getChild(0).toSql()
                    + " from " + childType + " to " + type);
        }

        Preconditions.checkState(type.matchesType(fn.getReturnType()), type + " != " + fn.getReturnType());
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(!isImplicit);
        // When cast target type is string and it's length is default -1, the result length
        // of cast is decided by child.
        if (targetTypeDef.getType().isScalarType()) {
            final ScalarType targetType = (ScalarType) targetTypeDef.getType();
            if (!(targetType.getPrimitiveType().isStringType() 
                    && !targetType.isAssignedStrLenInColDefinition())) {
                targetTypeDef.analyze(analyzer);
            }
        } else {
            targetTypeDef.analyze(analyzer);
        }
        type = targetTypeDef.getType();
        analyze();
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
            Preconditions.checkState(
              !(getChild(0) instanceof CastExpr) || !((CastExpr) getChild(0)).isImplicit());
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
            targetExpr = castTo((LiteralExpr)value);
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
        } else if (type.isDecimal()) {
            return new DecimalLiteral(value.getStringValue());
        } else if (type.isFloatingPointType()) {
            return new FloatLiteral(value.getDoubleValue(), type);
        } else if (type.isStringType()) {
            return new StringLiteral(value.getStringValue());
        } else if (type.isDateType()) {
            return new DateLiteral(value.getStringValue(), type);
        } else if (type.isBoolean()) {
            return new BoolLiteral(value.getStringValue());
        }
        return this;
    }
}
