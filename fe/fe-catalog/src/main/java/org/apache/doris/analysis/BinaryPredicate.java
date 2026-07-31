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
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Most predicates with two operands..
 */
public class BinaryPredicate extends Predicate {

    public enum Operator {
        EQ("=", "eq"),
        NE("!=", "ne"),
        LE("<=", "le"),
        GE(">=", "ge"),
        LT("<", "lt"),
        GT(">", "gt"),
        EQ_FOR_NULL("<=>", "eq_for_null");

        @SerializedName("desc")
        private final String description;
        @SerializedName("name")
        private final String name;

        Operator(String description,
                 String name) {
            this.description = description;
            this.name = name;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public Operator commutative() {
            switch (this) {
                case EQ:
                case EQ_FOR_NULL:
                case NE:
                    return this;
                case LE:
                    return GE;
                case GE:
                    return LE;
                case LT:
                    return GT;
                case GT:
                    return LT;
                default:
                    return null;
            }
        }
    }

    @SerializedName("op")
    private Operator op;
    // check if left is slot and right isnot slot.
    private Boolean slotIsLeft = null;

    // for restoring
    public BinaryPredicate() {
        super();
    }

    /**
     * NOTICE: only used for ddl and test.
     */
    public BinaryPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
    }

    public BinaryPredicate(Operator op, Expr e1, Expr e2, Type retType, boolean nullable) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        fn = new Function(new FunctionName(op.name), Lists.newArrayList(e1.getType(), e2.getType()), retType,
                false, true,
                op == Operator.EQ_FOR_NULL ? NullableMode.ALWAYS_NOT_NULLABLE : NullableMode.DEPEND_ON_ARGUMENT);
        this.nullable = nullable;
    }

    protected BinaryPredicate(BinaryPredicate other) {
        super(other);
        op = other.op;
        slotIsLeft = other.slotIsLeft;
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
        return ((BinaryPredicate) obj).op == this.op;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
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
            slotIsLeft = true;
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
            slotIsLeft = false;
            return getChild(0);
        }

        return null;
    }

    public boolean slotIsLeft() {
        Preconditions.checkState(slotIsLeft != null);
        return slotIsLeft;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }
}
