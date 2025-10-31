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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ArithmeticExpr.java
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

import java.util.List;
import java.util.Objects;

public class ArithmeticExpr extends Expr {

    enum OperatorPosition {
        BINARY_INFIX,
        UNARY_PREFIX,
        UNARY_POSTFIX,
    }

    public enum Operator {
        MULTIPLY("*", "multiply", OperatorPosition.BINARY_INFIX, TExprOpcode.MULTIPLY),
        DIVIDE("/", "divide", OperatorPosition.BINARY_INFIX, TExprOpcode.DIVIDE),
        MOD("%", "mod", OperatorPosition.BINARY_INFIX, TExprOpcode.MOD),
        INT_DIVIDE("DIV", "int_divide", OperatorPosition.BINARY_INFIX, TExprOpcode.INT_DIVIDE),
        ADD("+", "add", OperatorPosition.BINARY_INFIX, TExprOpcode.ADD),
        SUBTRACT("-", "subtract", OperatorPosition.BINARY_INFIX, TExprOpcode.SUBTRACT),
        BITAND("&", "bitand", OperatorPosition.BINARY_INFIX, TExprOpcode.BITAND),
        BITOR("|", "bitor", OperatorPosition.BINARY_INFIX, TExprOpcode.BITOR),
        BITXOR("^", "bitxor", OperatorPosition.BINARY_INFIX, TExprOpcode.BITXOR),
        BITNOT("~", "bitnot", OperatorPosition.UNARY_PREFIX, TExprOpcode.BITNOT),
        FACTORIAL("!", "factorial", OperatorPosition.UNARY_POSTFIX, TExprOpcode.FACTORIAL);

        private final String description;
        private final String name;
        private final OperatorPosition pos;
        private final TExprOpcode opcode;

        Operator(String description, String name, OperatorPosition pos, TExprOpcode opcode) {
            this.description = description;
            this.name = name;
            this.pos = pos;
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

        public boolean isUnary() {
            return pos == OperatorPosition.UNARY_PREFIX
                    || pos == OperatorPosition.UNARY_POSTFIX;
        }

        public boolean isBinary() {
            return pos == OperatorPosition.BINARY_INFIX;
        }
    }

    @SerializedName("op")
    private final Operator op;

    public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(
                op == Operator.BITNOT && e2 == null || op != Operator.BITNOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
    }

    /**
     * constructor only used for Nereids.
     */
    public ArithmeticExpr(Operator op, Expr e1, Expr e2, Type returnType, NullableMode nullableMode) {
        this(op, e1, e2);
        List<Type> argTypes;
        if (e2 == null) {
            argTypes = Lists.newArrayList(e1.getType());
        } else {
            argTypes = Lists.newArrayList(e1.getType(), e2.getType());
        }
        fn = new Function(new FunctionName(op.getName()), argTypes, returnType, false, true, nullableMode);
        type = returnType;
    }

    /**
     * Copy c'tor used in clone().
     */
    protected ArithmeticExpr(ArithmeticExpr other) {
        super(other);
        this.op = other.op;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public Expr clone() {
        return new ArithmeticExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toSql();
        } else {
            return "(" + getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql() + ")";
        }
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toSql(disableTableName, needExternalSql, tableType, table);
        } else {
            return "(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " " + op.toString()
                    + " " + getChild(1).toSql(disableTableName, needExternalSql, tableType, table) + ")";
        }
    }

    @Override
    public String toDigestImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toDigest();
        } else {
            return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
        if (!(type.isDecimalV2() || type.isDecimalV3())) {
            msg.setOpcode(op.getOpcode());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((ArithmeticExpr) obj).opcode == opcode;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }
}
