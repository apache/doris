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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Abstract for all binary operator, include binary arithmetic, compound predicate, comparison predicate.
 */
public abstract class BinaryOperator extends Expression implements BinaryExpression, ExpectsInputTypes {

    protected final String symbol;

    public BinaryOperator(List<Expression> children, String symbol) {
        this(children, symbol, false);
    }

    public BinaryOperator(List<Expression> children, String symbol, boolean inferred) {
        super(children, inferred);
        this.symbol = symbol;
    }

    public abstract DataType inputType();

    @Override
    public List<DataType> expectedInputTypes() {
        return ImmutableList.of(inputType(), inputType());
    }

    @Override
    public String computeToSql() {
        return "(" + left().toSql() + " " + symbol + " " + right().toSql() + ")";
    }

    @Override
    public String toString() {
        return "(" + left().toString() + " " + symbol + " " + right().toString() + ")";
    }

    @Override
    public String getFingerprint() {
        String leftHboString = left().toString();
        String rightHboString = right().toString();
        // expression with function will not be parameterized.
        // case: cast(s_zip as varchar(20)) = "31904"
        // leftHboString: "substring(cast(s_zip#48 as VARCHAR(20)), 1, 20)"
        // the '1' and '20' will not and should not be parameterized, which is expected.
        // but the foldable expression like "substring("3190400", 1, 5)" has been folded as '31904' before cbo stage,
        // so it is safe during the plan matching and runtime stats recording.
        if (left() instanceof Literal) {
            leftHboString = ((Literal) left()).getFingerprint();
        }
        if (right() instanceof Literal) {
            rightHboString = ((Literal) right()).getFingerprint();
        }
        return "(" + leftHboString + " " + symbol + " " + rightHboString + ")";
    }

    @Override
    public String shapeInfo() {
        return "(" + left().shapeInfo() + " " + symbol + " " + right().shapeInfo() + ")";
    }
}
