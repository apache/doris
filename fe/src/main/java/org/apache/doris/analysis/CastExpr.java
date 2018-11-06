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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class CastExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(CastExpr.class);

    private final Type targetType;
    /**
     * true if this is a "pre-analyzed" implicit cast
     */
    private final boolean isImplicit;

    // True if this cast does not change the type.
    private boolean noOp = false;

    public CastExpr(Type targetType, Expr e, boolean isImplicit) {
        super();
        Preconditions.checkArgument(targetType != Type.INVALID);
        this.targetType = targetType;
        this.isImplicit = isImplicit;
        Preconditions.checkNotNull(e);
        children.add(e);
        if (isImplicit) {
            type = targetType;
            try {
                analyze();
            } catch (AnalysisException ex) {
                Preconditions.checkState(false,
                        "Implicit casts should never throw analysis exception.");
            }
            analysisDone();
        }
    }

    protected CastExpr(CastExpr other) {
        super(other);
        targetType = other.targetType;
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
                // Disable casting from boolean/timestamp to decimal
                if ((fromType.isBoolean() || fromType.isDateType()) && toType == Type.DECIMAL) {
                    continue;
                }
//                if (fromType.getPrimitiveType() == PrimitiveType.CHAR
//                        && toType.getPrimitiveType() == PrimitiveType.CHAR) {
//                    // Allow casting from CHAR(N) to Char(N)
//                    String beSymbol = "impala::CastFunctions::CastToChar";
//                    functionSet.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
//                            Lists.newArrayList((Type) ScalarType.createCharType(-1)), false,
//                            ScalarType.CHAR, beSymbol, null, null, true));
//                    continue;
//                }
//                if (fromType.getPrimitiveType() == PrimitiveType.VARCHAR
//                        && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
//                    // Allow casting from VARCHAR(N) to VARCHAR(M)
//                    String beSymbol = "impala::CastFunctions::CastToStringVal";
//                    functionSet.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
//                            Lists.newArrayList((Type) ScalarType.VARCHAR), false, ScalarType.VARCHAR,
//                            beSymbol, null, null, true));
//                    continue;
//                }
//                if (fromType.getPrimitiveType() == PrimitiveType.VARCHAR
//                        && toType.getPrimitiveType() == PrimitiveType.CHAR) {
//                    // Allow casting from VARCHAR(N) to CHAR(M)
//                    String beSymbol = "impala::CastFunctions::CastToChar";
//                    functionSet.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
//                            Lists.newArrayList((Type) ScalarType.VARCHAR), false, ScalarType.CHAR,
//                            beSymbol, null, null, true));
//                    continue;
//                }
//                if (fromType.getPrimitiveType() == PrimitiveType.CHAR
//                        && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
//                    // Allow casting from CHAR(N) to VARCHAR(M)
//                    String beSymbol = "impala::CastFunctions::CastToStringVal";
//                    functionSet.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
//                            Lists.newArrayList((Type) ScalarType.CHAR), false, ScalarType.VARCHAR,
//                            beSymbol, null, null, true));
//                    continue;
//                }

                // Disable no-op casts
                if (fromType.equals(toType)) {
                    continue;
                }
                String beClass = toType.isDecimal() || fromType.isDecimal() ? "DecimalOperators" : "CastFunctions";
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
        if (targetType.isStringType()) {
            return "CAST(" + getChild(0).toSql() + " AS " + "CHARACTER" + ")";
        } else {
            return "CAST(" + getChild(0).toSql() + " AS " + targetType.toString() + ")";
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

    private void analyze() throws AnalysisException {
        // cast was asked for in the query, check for validity of cast
        Type childType = getChild(0).getType();

        // this cast may result in loss of precision, but the user requested it
        if (childType.equals(targetType)) {
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
        type = targetType;
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

}
