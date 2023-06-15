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

import org.apache.doris.analysis.MatchPredicate.Operator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

/**
 * like expression: a MATCH 'hello'.
 */
public abstract class Match extends Expression {

    protected final String symbol;

    public Match(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;
    }

    /**
    * translate symbol to operator in MatchPredicate
    */
    public Operator op() throws AnalysisException {
        switch (symbol) {
            case "MATCH":
            case "MATCH_ANY":
                return Operator.MATCH_ANY;
            case "MATCH_ALL":
                return Operator.MATCH_ALL;
            case "MATCH_PHRASE":
                return Operator.MATCH_PHRASE;
            default:
                throw new AnalysisException("UnSupported type: " + symbol);
        }
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        Preconditions.checkArgument(children.size() == 2);
        return (children.get(0).nullable() || children.get(1).nullable());
    }

    @Override
    public String toSql() {
        Preconditions.checkArgument(children.size() == 2);
        return "(" + children.get(0).toSql() + " " + symbol + " " + children.get(1).toSql() + ")";
    }

    @Override
    public String toString() {
        Preconditions.checkArgument(children.size() == 2);
        return "(" + children.get(0).toString() + " " + symbol + " " + children.get(1).toString() + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMatch(this, context);
    }
}
