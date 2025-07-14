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
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Most predicates with two operands..
 */
public class BinaryPredicate extends Predicate {

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

        @SerializedName("desc")
        private final String description;
        @SerializedName("name")
        private final String name;
        @SerializedName("opcode")
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

    @SerializedName("op")
    private Operator op;
    // check if left is slot and right isnot slot.
    private Boolean slotIsleft = null;

    // for restoring
    public BinaryPredicate() {
        super();
        printSqlInParens = true;
    }

    public BinaryPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        this.opcode = op.opcode;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        printSqlInParens = true;
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
        printSqlInParens = true;
    }

    protected BinaryPredicate(BinaryPredicate other) {
        super(other);
        op = other.op;
        slotIsleft = other.slotIsleft;
        isInferred = other.isInferred;
        printSqlInParens = true;
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
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " " + op.toString() + " "
                + getChild(1).toSql(disableTableName, needExternalSql, tableType, table);
    }

    @Override
    public String toDigestImpl() {
        return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(opcode);
        msg.setChildType(getChild(0).getType().getPrimitiveType().toThrift());
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

    private Expr compareLiteral(LiteralExpr first, LiteralExpr second) {
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
                return new BoolLiteral(compareResult >= 0);
            case GT:
                return new BoolLiteral(compareResult > 0);
            case LE:
                return new BoolLiteral(compareResult <= 0);
            case LT:
                return new BoolLiteral(compareResult < 0);
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
