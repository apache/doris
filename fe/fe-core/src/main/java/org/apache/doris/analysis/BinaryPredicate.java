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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

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
    }

    @SerializedName("op")
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
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((BinaryPredicate) obj).opcode == this.opcode;
    }

    @Override
    public String toSqlImpl() {
        return "(" + getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql() + ")";
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return "(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table)
                + " " + op.toString() + " "
                + getChild(1).toSql(disableTableName, needExternalSql, tableType, table) + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(opcode);
        msg.setChildType(getChild(0).getType().getPrimitiveType().toThrift());
    }

    /**
     * If predicate is of the form "<slotref> <op> <expr>", returns expr,
     * otherwise returns null. Slotref may be wrapped in a CastExpr.
     */
    public Expr getSlotBinding(SlotId id) {
        SlotRef slotRef = null;
        // check left operand
        if (getChild(0) instanceof SlotRef) {
            slotRef = (SlotRef) getChild(0);
        } else if (getChild(0) instanceof CastExpr && getChild(0).getChild(0) instanceof SlotRef) {
            if (((CastExpr) getChild(0)).canHashPartition()) {
                slotRef = (SlotRef) getChild(0).getChild(0);
            }
        }

        if (slotRef != null && slotRef.getSlotId() == id) {
            slotIsleft = true;
            return getChild(1);
        }

        // check right operand
        if (getChild(1) instanceof SlotRef) {
            slotRef = (SlotRef) getChild(1);
        } else if (getChild(1) instanceof CastExpr && getChild(1).getChild(0) instanceof SlotRef) {
            if (((CastExpr) getChild(1)).canHashPartition()) {
                slotRef = (SlotRef) getChild(1).getChild(0);
            }
        }

        if (slotRef != null && slotRef.getSlotId() == id) {
            slotIsleft = false;
            return getChild(0);
        }

        return null;
    }

    public boolean slotIsLeft() {
        Preconditions.checkState(slotIsleft != null);
        return slotIsleft;
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
