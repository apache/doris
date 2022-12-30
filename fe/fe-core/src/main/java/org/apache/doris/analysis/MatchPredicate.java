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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * filed MATCH query_str
 */
public class MatchPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(MatchPredicate.class);

    public enum Operator {
        MATCH_ANY("MATCH_ANY", "match_any", TExprOpcode.MATCH_ANY),
        MATCH_ALL("MATCH_ALL", "match_all", TExprOpcode.MATCH_ALL),
        MATCH_PHRASE("MATCH_PHRASE", "match_phrase", TExprOpcode.MATCH_PHRASE),
        MATCH_ELEMENT_EQ("MATCH_ELEMENT_EQ", "match_element_eq", TExprOpcode.MATCH_ELEMENT_EQ),
        MATCH_ELEMENT_LT("MATCH_ELEMENT_LT", "match_element_lt", TExprOpcode.MATCH_ELEMENT_LT),
        MATCH_ELEMENT_GT("MATCH_ELEMENT_GT", "match_element_gt", TExprOpcode.MATCH_ELEMENT_GT),
        MATCH_ELEMENT_LE("MATCH_ELEMENT_LE", "match_element_le", TExprOpcode.MATCH_ELEMENT_LE),
        MATCH_ELEMENT_GE("MATCH_ELEMENT_GE", "match_element_ge", TExprOpcode.MATCH_ELEMENT_GE);


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
    }

    public static void initBuiltins(FunctionSet functionSet) {
        String symbolNotUsed = "symbol_not_used";

        for (Type t : Type.getNumericDateTimeTypes()) {
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ELEMENT_EQ.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ELEMENT_LT.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ELEMENT_GT.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ELEMENT_LE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ELEMENT_GE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
        }
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_ANY.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_ANY.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(new ArrayType(Type.VARCHAR), Type.VARCHAR),
                Type.BOOLEAN));

        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_ALL.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_ALL.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(new ArrayType(Type.VARCHAR), Type.VARCHAR),
                Type.BOOLEAN));

        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_PHRASE.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_PHRASE.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(new ArrayType(Type.VARCHAR), Type.VARCHAR),
                Type.BOOLEAN));
    }

    private final Operator op;

    public MatchPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        // TODO: Calculate selectivity
        selectivity = Expr.DEFAULT_SELECTIVITY;
    }

    public Boolean isMatchElement(Operator op) {
        return Objects.equals(op.getName(), Operator.MATCH_ELEMENT_EQ.getName())
                || Objects.equals(op.getName(), Operator.MATCH_ELEMENT_LT.getName())
                || Objects.equals(op.getName(), Operator.MATCH_ELEMENT_GT.getName())
                || Objects.equals(op.getName(), Operator.MATCH_ELEMENT_LE.getName())
                || Objects.equals(op.getName(), Operator.MATCH_ELEMENT_GE.getName());
    }

    protected MatchPredicate(MatchPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new MatchPredicate(this);
    }

    public Operator getOp() {
        return this.op;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((MatchPredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_PRED;
        msg.setOpcode(op.getOpcode());
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        if (isMatchElement(op) && !getChild(0).getType().isArrayType()) {
            throw new AnalysisException(
                    "left operand of " + op.toString() + " must be Array: " + toSql());
        }
        if (getChild(0).getType().isObjectStored()) {
            throw new AnalysisException(
                    "left operand of " + op.toString() + " must not be Bitmap or HLL: " + toSql());
        }
        if (!isMatchElement(op) && !getChild(1).getType().isStringType() && !getChild(1).getType().isNull()) {
            throw new AnalysisException("right operand of " + op.toString() + " must be of type STRING: " + toSql());
        }

        if (!getChild(0).getType().isStringType() && !getChild(0).getType().isArrayType()) {
            throw new AnalysisException(
                    "left operand of " + op.toString() + " must be of type STRING or ARRAY: " + toSql());
        }

        fn = getBuiltinFunction(op.toString(),
                collectChildReturnTypes(), Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            throw new AnalysisException(
                    "no function found for " + op.toString() + " " + toSql());
        }
        Expr e1 = getChild(0);
        Expr e2 = getChild(1);
        // Here we cast match_element_xxx value type from string to array item type.
        // Because be need to know the actual TExprNodeType when doing Expr Literal transform
        if (isMatchElement(op) && e1.type.isArrayType() && (e2 instanceof StringLiteral)) {
            Type itemType = ((ArrayType) e1.type).getItemType();
            try {
                setChild(1, e2.castTo(itemType));
            } catch (NumberFormatException nfe) {
                throw new AnalysisException("Invalid number format literal: " + e2.getStringValue());
            }
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

}
