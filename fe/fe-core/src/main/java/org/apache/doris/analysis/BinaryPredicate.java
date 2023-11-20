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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/BinaryPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.TypeUtils;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Most predicates with two operands..
 */
public class BinaryPredicate extends Predicate implements Writable {
    private static final Logger LOG = LogManager.getLogger(BinaryPredicate.class);

    // true if this BinaryPredicate is inferred from slot equivalences, false otherwise.
    private boolean isInferred = false;

    public enum Operator {
        EQ("=", "eq", TExprOpcode.EQ),
        NE("!=", "ne", TExprOpcode.NE),
        LE("<=", "le", TExprOpcode.LE),
        GE(">=", "ge", TExprOpcode.GE),
        LT("<", "lt", TExprOpcode.LT),
        GT(">", "gt", TExprOpcode.GT),
        EQ_FOR_NULL("<=>", "eq_for_null", TExprOpcode.EQ_FOR_NULL);

        private final String description;
        private final String name;
        private final TExprOpcode opcode;

        Operator(String description,
                 String name,
                 TExprOpcode opcode) {
            this.description = description;
            this.name = name;
            this.opcode = opcode;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public TExprOpcode getOpcode() {
            return opcode;
        }

        public Operator commutative() {
            switch (this) {
                case EQ:
                    return this;
                case NE:
                    return this;
                case LE:
                    return GE;
                case GE:
                    return LE;
                case LT:
                    return GT;
                case GT:
                    return LE;
                case EQ_FOR_NULL:
                    return this;
                default:
                    return null;
            }
        }

        public Operator converse() {
            switch (this) {
                case EQ:
                    return EQ;
                case NE:
                    return NE;
                case LE:
                    return GE;
                case GE:
                    return LE;
                case LT:
                    return GT;
                case GT:
                    return LT;
                case EQ_FOR_NULL:
                    return EQ_FOR_NULL;
                // case DISTINCT_FROM: return DISTINCT_FROM;
                // case NOT_DISTINCT: return NOT_DISTINCT;
                // case NULL_MATCHING_EQ:
                // throw new IllegalStateException("Not implemented");
                default:
                    throw new IllegalStateException("Invalid operator");
            }
        }

        public boolean isEquivalence() {
            return this == EQ || this == EQ_FOR_NULL;
        }

        public boolean isUnNullSafeEquivalence() {
            return this == EQ;
        }

        public boolean isUnequivalence() {
            return this == NE;
        }
    }

    private Operator op;
    // check if left is slot and right isnot slot.
    private Boolean slotIsleft = null;

    // for restoring
    public BinaryPredicate() {
        super();
    }

    public BinaryPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        this.opcode = op.opcode;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
    }

    public BinaryPredicate(Operator op, Expr e1, Expr e2, Type retType, NullableMode nullableMode) {
        super();
        this.op = op;
        this.opcode = op.opcode;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        fn = new Function(new FunctionName(op.name), Lists.newArrayList(e1.getType(), e2.getType()), retType,
                false, true, nullableMode);
    }

    protected BinaryPredicate(BinaryPredicate other) {
        super(other);
        op = other.op;
        slotIsleft = other.slotIsleft;
        isInferred = other.isInferred;
    }

    public boolean isInferred() {
        return isInferred;
    }

    public void setIsInferred() {
        isInferred = true;
    }

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.EQ.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.NE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.LE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.GE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.LT.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.GT.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.EQ_FOR_NULL.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
        }
    }

    @Override
    public Expr clone() {
        return new BinaryPredicate(this);
    }

    public Operator getOp() {
        return op;
    }

    public void setOp(Operator op) {
        this.op = op;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(children.get(0)).append(" ").append(op).append(" ").append(children.get(1));
        return builder.toString();
    }

    @Override
    public Expr negate() {
        Operator newOp = null;
        switch (op) {
            case EQ:
                newOp = Operator.NE;
                break;
            case NE:
                newOp = Operator.EQ;
                break;
            case LT:
                newOp = Operator.GE;
                break;
            case LE:
                newOp = Operator.GT;
                break;
            case GE:
                newOp = Operator.LT;
                break;
            case GT:
                newOp = Operator.LE;
                break;
            default:
                throw new IllegalStateException("Not implemented");
        }
        return new BinaryPredicate(newOp, getChild(0), getChild(1));
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((BinaryPredicate) obj).opcode == this.opcode;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    public String toDigestImpl() {
        return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(opcode);
        msg.setVectorOpcode(vectorOpcode);
        msg.setChildType(getChild(0).getType().getPrimitiveType().toThrift());
    }

    @Override
    public void vectorizedAnalyze(Analyzer analyzer) {
        super.vectorizedAnalyze(analyzer);
        Function match = null;

        //OpcodeRegistry.BuiltinFunction match = OpcodeRegistry.instance().getFunctionInfo(
        //        op.toFilterFunctionOp(), true, true, cmpType, cmpType);
        try {
            match = getBuiltinFunction(op.name, collectChildReturnTypes(),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } catch (AnalysisException e) {
            Preconditions.checkState(false);
        }
        Preconditions.checkState(match != null);
        Preconditions.checkState(match.getReturnType().getPrimitiveType() == PrimitiveType.BOOLEAN);
        //todo(dhc): should add oppCode
        //this.vectorOpcode = match.opcode;
        LOG.debug(debugString() + " opcode: " + vectorOpcode);
    }

    private boolean canCompareDate(PrimitiveType t1, PrimitiveType t2) {
        if (t1.isDateType()) {
            if (t2.isDateType() || t2.isStringType() || t2.isIntegerType()) {
                return true;
            }
            return false;
        } else if (t2.isDateType()) {
            if (t1.isStringType() || t1.isIntegerType()) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    private boolean canCompareIP(PrimitiveType t1, PrimitiveType t2) {
        if (t1.isIPv4Type()) {
            return t2.isIPv4Type() || t2.isStringType();
        } else if (t2.isIPv4Type()) {
            return t1.isStringType();
        } else if (t1.isIPv6Type()) {
            return t2.isIPv6Type() || t2.isStringType();
        } else if (t2.isIPv6Type()) {
            return t1.isStringType();
        }
        return false;
    }

    private Type dateV2ComparisonResultType(ScalarType t1, ScalarType t2) {
        if (!t1.isDatetimeV2() && !t2.isDatetimeV2()) {
            return Type.DATEV2;
        } else if (t1.isDatetimeV2() && t2.isDatetimeV2()) {
            return ScalarType.createDatetimeV2Type(Math.max(t1.getScalarScale(), t2.getScalarScale()));
        } else if (t1.isDatetimeV2()) {
            return t1;
        } else {
            return t2;
        }
    }

    private Type getCmpType() throws AnalysisException {
        if (!getChild(0).isConstantImpl() && getChild(1).isConstantImpl()) {
            getChild(1).compactForLiteral(getChild(0).getType());
        } else if (!getChild(1).isConstantImpl() && getChild(0).isConstantImpl()) {
            getChild(0).compactForLiteral(getChild(1).getType());
        }
        PrimitiveType t1 = getChild(0).getType().getResultType().getPrimitiveType();
        PrimitiveType t2 = getChild(1).getType().getResultType().getPrimitiveType();

        for (Expr e : getChildren()) {
            if (e.getType().getPrimitiveType() == PrimitiveType.HLL) {
                throw new AnalysisException("Hll type dose not support operand: " + toSql());
            }
            if (e.getType().getPrimitiveType() == PrimitiveType.BITMAP) {
                throw new AnalysisException("Bitmap type dose not support operand: " + toSql());
            }
            if (e.getType().isArrayType()) {
                throw new AnalysisException("Array type dose not support operand: " + toSql());
            }
        }

        if (canCompareDate(getChild(0).getType().getPrimitiveType(), getChild(1).getType().getPrimitiveType())) {
            if (getChild(0).getType().isDatetimeV2() && getChild(1).getType().isDatetimeV2()) {
                Preconditions.checkArgument(getChild(0).getType() instanceof ScalarType
                        && getChild(1).getType() instanceof ScalarType);
                return dateV2ComparisonResultType((ScalarType) getChild(0).getType(),
                        (ScalarType) getChild(1).getType());
            } else if (getChild(0).getType().isDatetimeV2()) {
                return getChild(0).getType();
            } else if (getChild(1).getType().isDatetimeV2()) {
                return getChild(1).getType();
            } else if (getChild(0).getType().isDateV2()
                    && (getChild(1).getType().isDate() || getChild(1).getType().isDateV2())) {
                return getChild(0).getType();
            } else if (getChild(1).getType().isDateV2()
                    && (getChild(0).getType().isDate() || getChild(0).getType().isDateV2())) {
                return getChild(1).getType();
            } else if (getChild(0).getType().isDateV2()
                    && (getChild(1).getType().isStringType() && getChild(1) instanceof StringLiteral)) {
                if (((StringLiteral) getChild(1)).canConvertToDateType(Type.DATEV2)) {
                    return Type.DATEV2;
                } else {
                    return Type.DATETIMEV2;
                }
            } else if (getChild(1).getType().isDateV2()
                    && (getChild(0).getType().isStringType() && getChild(0) instanceof StringLiteral)) {
                if (((StringLiteral) getChild(0)).canConvertToDateType(Type.DATEV2)) {
                    return Type.DATEV2;
                } else {
                    return Type.DATETIMEV2;
                }
            } else if (getChild(0).getType().isDatetimeV2()
                    && (getChild(1).getType().isStringType() && getChild(1) instanceof StringLiteral)) {
                return getChild(0).getType();
            } else if (getChild(1).getType().isDatetimeV2()
                    && (getChild(0).getType().isStringType() && getChild(0) instanceof StringLiteral)) {
                return getChild(1).getType();
            } else if (getChild(0).getType().isDate()
                    && (getChild(1).getType().isStringType() && getChild(1) instanceof StringLiteral)) {
                return ((StringLiteral) getChild(1)).canConvertToDateType(Type.DATE) ? Type.DATE : Type.DATETIME;
            } else if (getChild(1).getType().isDate()
                    && (getChild(0).getType().isStringType() && getChild(0) instanceof StringLiteral)) {
                return ((StringLiteral) getChild(0)).canConvertToDateType(Type.DATE) ? Type.DATE : Type.DATETIME;
            } else if (getChild(1).getType().isDate() && getChild(0).getType().isDate()) {
                return Type.DATE;
            } else {
                return Type.DATETIME;
            }
        }

        if (canCompareIP(getChild(0).getType().getPrimitiveType(), getChild(1).getType().getPrimitiveType())) {
            if ((getChild(0).getType().isIP() && getChild(1) instanceof StringLiteral)
                    || (getChild(1).getType().isIP() && getChild(0) instanceof StringLiteral)
                    || (getChild(0).getType().isIP() && getChild(1).getType().isIP())) {
                if (getChild(0).getType().isIPv4() || getChild(1).getType().isIPv4()) {
                    return Type.IPV4;
                }
                if (getChild(0).getType().isIPv6() || getChild(1).getType().isIPv6()) {
                    return Type.IPV6;
                }
            }
        }

        // Following logical is compatible with MySQL:
        //    Cast to DOUBLE by default, because DOUBLE has the largest range of values.
        if (t1 == PrimitiveType.VARCHAR && t2 == PrimitiveType.VARCHAR) {
            return Type.VARCHAR;
        }
        if ((t1 == PrimitiveType.STRING && (t2 == PrimitiveType.VARCHAR || t2 == PrimitiveType.STRING)) || (
                t2 == PrimitiveType.STRING && (t1 == PrimitiveType.VARCHAR || t1 == PrimitiveType.STRING))) {
            return Type.STRING;
        }
        if (t1 == PrimitiveType.BIGINT && t2 == PrimitiveType.BIGINT) {
            return Type.getAssignmentCompatibleType(getChild(0).getType(), getChild(1).getType(), false);
        }
        if ((t1 == PrimitiveType.BIGINT && t2 == PrimitiveType.DECIMALV2)
                || (t2 == PrimitiveType.BIGINT && t1 == PrimitiveType.DECIMALV2)
                || (t1 == PrimitiveType.LARGEINT && t2 == PrimitiveType.DECIMALV2)
                || (t2 == PrimitiveType.LARGEINT && t1 == PrimitiveType.DECIMALV2)) {
            // only decimalv3 can hold big and large int
            return ScalarType.createDecimalType(PrimitiveType.DECIMAL128, ScalarType.MAX_DECIMAL128_PRECISION,
                    ScalarType.MAX_DECIMALV2_SCALE);
        }
        if ((t1 == PrimitiveType.BIGINT || t1 == PrimitiveType.LARGEINT)
                && (t2 == PrimitiveType.BIGINT || t2 == PrimitiveType.LARGEINT)) {
            return Type.LARGEINT;
        }

        // Implicit conversion affects query performance.
        // For a common example datekey='20200825' which datekey is int type.
        // If we up conversion to double type directly.
        // PartitionPruner will not take effect. Then it will scan all partitions.
        // When int column compares with string, Mysql will convert string to int.
        // So it is also compatible with Mysql.

        if (t1.isStringType() || t2.isStringType()) {
            if ((t1 == PrimitiveType.BIGINT || t1 == PrimitiveType.LARGEINT) && TypeUtils.canParseTo(getChild(1), t1)) {
                return Type.fromPrimitiveType(t1);
            }
            if ((t2 == PrimitiveType.BIGINT || t2 == PrimitiveType.LARGEINT) && TypeUtils.canParseTo(getChild(0), t2)) {
                return Type.fromPrimitiveType(t2);
            }
        }

        if ((t1.isDecimalV3Type() && !t2.isStringType() && !t2.isFloatingPointType())
                || (t2.isDecimalV3Type() && !t1.isStringType() && !t1.isFloatingPointType())) {
            return Type.getAssignmentCompatibleType(getChild(0).getType(), getChild(1).getType(), false);
        }

        return Type.DOUBLE;
    }

    // Expr only support Literal
    public Pair<SlotRef, Expr> extract() {
        Expr lexpr = getChild(0);
        Expr rexpr = getChild(1);
        if (lexpr instanceof SlotRef && (rexpr instanceof LiteralExpr)) {
            SlotRef slot = (SlotRef) lexpr;
            return Pair.of(slot, rexpr);
        } else if (rexpr instanceof SlotRef && (lexpr instanceof LiteralExpr)) {
            SlotRef slot = (SlotRef) rexpr;
            return Pair.of(slot, lexpr);
        }
        return null;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        this.checkIncludeBitmap();
        // Ignore placeholder
        if (getChild(0) instanceof PlaceHolderExpr || getChild(1) instanceof PlaceHolderExpr) {
            return;
        }

        for (Expr expr : children) {
            if (expr instanceof Subquery) {
                Subquery subquery = (Subquery) expr;
                if (!subquery.returnsScalarColumn()) {
                    String msg = "Subquery of binary predicate must return a single column: " + expr.toSql();
                    throw new AnalysisException(msg);
                }
                /**
                 * Situation: The expr is a binary predicate and the type of subquery is not scalar type.
                 * Add assert: The stmt of subquery is added an assert condition (return error if row count > 1).
                 * Input params:
                 *     expr: k1=(select k1 from t2)
                 *     subquery stmt: select k1 from t2
                 * Output params:
                 *     new expr: k1 = (select k1 from t2 (assert row count: return error if row count > 1 ))
                 *     subquery stmt: select k1 from t2 (assert row count: return error if row count > 1 )
                 */
                if (!subquery.getType().isScalarType()) {
                    subquery.getStatement().setAssertNumRowsElement(1, AssertNumRowsElement.Assertion.LE);
                }
            }
        }

        // if children has subquery, it will be rewritten and reanalyzed in the future.
        if (contains(Subquery.class)) {
            return;
        }

        Type cmpType = getCmpType();
        // Ignore return value because type is always bool for predicates.
        castBinaryOp(cmpType);

        this.opcode = op.getOpcode();
        String opName = op.getName();
        fn = getBuiltinFunction(opName, collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
        if (fn == null) {
            Preconditions.checkState(false, String.format(
                    "No match for '%s' with operand types %s and %s", toSql()));
        }

        // determine selectivity
        Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
        if (op == Operator.EQ && isSingleColumnPredicate(slotRefRef, null)
                && slotRefRef.getRef().getNumDistinctValues() > 0) {
            Preconditions.checkState(slotRefRef.getRef() != null);
            selectivity = 1.0 / slotRefRef.getRef().getNumDistinctValues();
            selectivity = Math.max(0, Math.min(1, selectivity));
        } else {
            // TODO: improve using histograms, once they show up
            selectivity = Expr.DEFAULT_SELECTIVITY;
        }

        // vectorizedAnalyze(analyzer);
    }

    public Expr invokeFunctionExpr(ArrayList<Expr> partitionExprs, Expr paramExpr) {
        for (Expr partExpr : partitionExprs) {
            if (partExpr instanceof FunctionCallExpr) {
                FunctionCallExpr function = (FunctionCallExpr) partExpr.clone();
                ArrayList<Expr> children = function.getChildren();
                for (int i = 0; i < children.size(); ++i) {
                    if (children.get(i) instanceof SlotRef) {
                        // when create partition have check only support one slotRef
                        function.setChild(i, paramExpr);
                        return ExpressionFunctions.INSTANCE.evalExpr(function);
                    }
                }
            }
        }
        return null;
    }

    /**
     * If predicate is of the form "<slotref> <op> <expr>", returns expr,
     * otherwise returns null. Slotref may be wrapped in a CastExpr.
     * now, when support auto create partition by function(column), so need check the "<function(column)> <op> <expr>"
     * because import data use function result to create partition,
     * so when have a SQL of query, prune partition also need use this function
     */
    public Expr getSlotBinding(SlotId id, ArrayList<Expr> partitionExprs) {
        SlotRef slotRef = null;
        boolean isFunctionCallExpr = false;
        // check left operand
        if (getChild(0) instanceof SlotRef) {
            slotRef = (SlotRef) getChild(0);
        } else if (getChild(0) instanceof CastExpr && getChild(0).getChild(0) instanceof SlotRef) {
            if (((CastExpr) getChild(0)).canHashPartition()) {
                slotRef = (SlotRef) getChild(0).getChild(0);
            }
        } else if (getChild(0) instanceof FunctionCallExpr) {
            FunctionCallExpr left = (FunctionCallExpr) getChild(0);
            if (partitionExprs != null && left.findEqual(partitionExprs) != null) {
                ArrayList<Expr> children = left.getChildren();
                for (int i = 0; i < children.size(); ++i) {
                    if (children.get(i) instanceof SlotRef) {
                        slotRef = (SlotRef) children.get(i);
                        isFunctionCallExpr = true;
                        break;
                    }
                }
            }
        }

        if (slotRef != null && slotRef.getSlotId() == id) {
            slotIsleft = true;
            if (isFunctionCallExpr) {
                return invokeFunctionExpr(partitionExprs, getChild(1));
            }
            return getChild(1);
        }

        // check right operand
        if (getChild(1) instanceof SlotRef) {
            slotRef = (SlotRef) getChild(1);
        } else if (getChild(1) instanceof CastExpr && getChild(1).getChild(0) instanceof SlotRef) {
            if (((CastExpr) getChild(1)).canHashPartition()) {
                slotRef = (SlotRef) getChild(1).getChild(0);
            }
        } else if (getChild(1) instanceof FunctionCallExpr) {
            FunctionCallExpr left = (FunctionCallExpr) getChild(1);
            if (partitionExprs != null && left.findEqual(partitionExprs) != null) {
                ArrayList<Expr> children = left.getChildren();
                for (int i = 0; i < children.size(); ++i) {
                    if (children.get(i) instanceof SlotRef) {
                        slotRef = (SlotRef) children.get(i);
                        isFunctionCallExpr = true;
                        break;
                    }
                }
            }
        }

        if (slotRef != null && slotRef.getSlotId() == id) {
            slotIsleft = false;
            if (isFunctionCallExpr) {
                return invokeFunctionExpr(partitionExprs, getChild(0));
            }
            return getChild(0);
        }

        return null;
    }

    /**
     * If e is an equality predicate between two slots that only require implicit
     * casts, returns those two slots; otherwise returns null.
     */
    public static Pair<SlotId, SlotId> getEqSlots(Expr e) {
        if (!(e instanceof BinaryPredicate)) {
            return null;
        }
        return ((BinaryPredicate) e).getEqSlots();
    }

    /**
     * If this is an equality predicate between two slots that only require implicit
     * casts, returns those two slots; otherwise returns null.
     */
    @Override
    public Pair<SlotId, SlotId> getEqSlots() {
        if (op != Operator.EQ) {
            return null;
        }
        SlotRef lhs = getChild(0).unwrapSlotRef(true);
        if (lhs == null) {
            return null;
        }
        SlotRef rhs = getChild(1).unwrapSlotRef(true);
        if (rhs == null) {
            return null;
        }
        return Pair.of(lhs.getSlotId(), rhs.getSlotId());
    }


    public boolean slotIsLeft() {
        Preconditions.checkState(slotIsleft != null);
        return slotIsleft;
    }

    public Range<LiteralExpr> convertToRange() {
        Preconditions.checkState(getChildWithoutCast(0) instanceof SlotRef);
        Preconditions.checkState(getChildWithoutCast(1) instanceof LiteralExpr);
        LiteralExpr literalExpr = (LiteralExpr) getChildWithoutCast(1);
        switch (op) {
            case EQ:
                return Range.singleton(literalExpr);
            case GE:
                return Range.atLeast(literalExpr);
            case GT:
                return Range.greaterThan(literalExpr);
            case LE:
                return Range.atMost(literalExpr);
            case LT:
                return Range.lessThan(literalExpr);
            case NE:
            default:
                return null;
        }
    }

    //    public static enum Operator2 {
    //        EQ("=", FunctionOperator.EQ, FunctionOperator.FILTER_EQ),
    //        NE("!=", FunctionOperator.NE, FunctionOperator.FILTER_NE),
    //        LE("<=", FunctionOperator.LE, FunctionOperator.FILTER_LE),
    //        GE(">=", FunctionOperator.GE, FunctionOperator.FILTER_GE),
    //        LT("<", FunctionOperator.LT, FunctionOperator.FILTER_LT),
    //        GT(">", FunctionOperator.GT, FunctionOperator.FILTER_LE);
    //
    //        private final String           description;
    //        private final FunctionOperator functionOp;
    //        private final FunctionOperator filterFunctionOp;
    //
    //        private Operator(String description,
    //                         FunctionOperator functionOp,
    //                         FunctionOperator filterFunctionOp) {
    //            this.description = description;
    //            this.functionOp = functionOp;
    //            this.filterFunctionOp = filterFunctionOp;
    //        }
    //
    //        @Override
    //        public String toString() {
    //            return description;
    //        }
    //
    //        public FunctionOperator toFunctionOp() {
    //            return functionOp;
    //        }
    //
    //        public FunctionOperator toFilterFunctionOp() {
    //            return filterFunctionOp;
    //        }
    //    }

    /*
     * the follow persistence code is only for TableFamilyDeleteInfo.
     * Maybe useless
     */
    @Override
    public void write(DataOutput out) throws IOException {
        boolean isWritable = true;
        Expr left = this.getChild(0);
        if (!(left instanceof SlotRef)) {
            isWritable = false;
        }

        Expr right = this.getChild(1);
        if (!(right instanceof StringLiteral)) {
            isWritable = false;
        }

        if (isWritable) {
            out.writeInt(1);
            // write op
            Text.writeString(out, op.name());
            // write left
            Text.writeString(out, ((SlotRef) left).getColumnName());
            // write right
            Text.writeString(out, ((StringLiteral) right).getStringValue());
        } else {
            out.writeInt(0);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int isWritable = in.readInt();
        if (isWritable == 0) {
            return;
        }

        // read op
        Operator op = Operator.valueOf(Text.readString(in));
        // read left
        SlotRef left = new SlotRef(null, Text.readString(in));
        // read right
        StringLiteral right = new StringLiteral(Text.readString(in));

        this.op = op;
        this.addChild(left);
        this.addChild(right);
    }

    public static BinaryPredicate read(DataInput in) throws IOException {
        BinaryPredicate binaryPredicate = new BinaryPredicate();
        binaryPredicate.readFields(in);
        return binaryPredicate;
    }

    @Override
    public Expr getResultValue(boolean forPushDownPredicatesToView) throws AnalysisException {
        recursiveResetChildrenResult(forPushDownPredicatesToView);
        final Expr leftChildValue = getChild(0);
        final Expr rightChildValue = getChild(1);
        if (!(leftChildValue instanceof LiteralExpr)
                || !(rightChildValue instanceof LiteralExpr)) {
            return this;
        }
        return compareLiteral((LiteralExpr) leftChildValue, (LiteralExpr) rightChildValue);
    }

    private Expr compareLiteral(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        final boolean isFirstNull = (first instanceof NullLiteral);
        final boolean isSecondNull = (second instanceof NullLiteral);
        if (op == Operator.EQ_FOR_NULL) {
            if (isFirstNull && isSecondNull) {
                return new BoolLiteral(true);
            } else if (isFirstNull || isSecondNull) {
                return new BoolLiteral(false);
            }
        } else  {
            if (isFirstNull || isSecondNull) {
                return new NullLiteral();
            }
        }

        final int compareResult = first.compareLiteral(second);
        switch (op) {
            case EQ:
            case EQ_FOR_NULL:
                return new BoolLiteral(compareResult == 0);
            case GE:
                return new BoolLiteral(compareResult == 1 || compareResult == 0);
            case GT:
                return new BoolLiteral(compareResult == 1);
            case LE:
                return new BoolLiteral(compareResult == -1 || compareResult == 0);
            case LT:
                return new BoolLiteral(compareResult == -1);
            case NE:
                return new BoolLiteral(compareResult != 0);
            default:
                Preconditions.checkState(false, "No defined binary operator.");
        }
        return this;
    }

    @Override
    public void setSelectivity() {
        switch (op) {
            case EQ:
            case EQ_FOR_NULL: {
                Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
                boolean singlePredicate = isSingleColumnPredicate(slotRefRef, null);
                if (singlePredicate) {
                    long distinctValues = slotRefRef.getRef().getNumDistinctValues();
                    if (distinctValues != -1) {
                        selectivity = 1.0 / distinctValues;
                    }
                }
                break;
            }
            default: {
                // Reference hive
                selectivity = 1.0 / 3.0;
                break;
            }
        }

        return;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public boolean isNullable() {
        if (op == Operator.EQ_FOR_NULL) {
            return false;
        }
        return hasNullableChild();
    }
}
