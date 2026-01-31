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

import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.analysis.MatchPredicate.Operator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * like expression: a MATCH 'hello'.
 */
public abstract class Match extends BinaryOperator implements PropagateNullable {

    private final Optional<String> analyzer;

    public Match(List<Expression> children, String symbol) {
        this(children, symbol, null);
    }

    /**
     * Constructor with analyzer parameter.
     * @param children child expressions
     * @param symbol the match operator symbol
     * @param analyzer the analyzer name (will be normalized to lowercase)
     */
    public Match(List<Expression> children, String symbol, String analyzer) {
        super(children, symbol);
        // Normalize analyzer name to lowercase for case-insensitive matching
        this.analyzer = Optional.ofNullable(analyzer)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase);
    }

    /**
     * Template method for creating a new instance with the given children and analyzer.
     * Each subclass must implement this to return a new instance of itself.
     */
    protected abstract Match createInstance(List<Expression> children, String analyzer);

    @Override
    public final Match withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return createInstance(children, analyzer.orElse(null));
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
            case "MATCH_PHRASE_PREFIX":
                return Operator.MATCH_PHRASE_PREFIX;
            case "MATCH_REGEXP":
                return Operator.MATCH_REGEXP;
            case "MATCH_PHRASE_EDGE":
                return Operator.MATCH_PHRASE_EDGE;
            default:
                throw new AnalysisException("UnSupported type for match: " + symbol);
        }
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public DataType inputType() {
        return AnyDataType.INSTANCE_WITHOUT_INDEX;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return left().nullable() || right().nullable();
    }

    @Override
    public String computeToSql() {
        return "(" + left().toSql() + " " + symbol + " " + right().toSql()
                + analyzerSqlFragment() + ")";
    }

    @Override
    public String toString() {
        return "(" + left().toString() + " " + symbol + " " + right().toString()
                + analyzerSqlFragment() + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMatch(this, context);
    }

    public Optional<String> getAnalyzer() {
        return analyzer;
    }

    protected Optional<String> analyzer() {
        return analyzer;
    }

    protected String analyzerSqlFragment() {
        return InvertedIndexUtil.buildAnalyzerSqlFragment(analyzer.orElse(null));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Match)) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        Match other = (Match) obj;
        return Objects.equals(analyzer, other.analyzer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), analyzer);
    }
}
