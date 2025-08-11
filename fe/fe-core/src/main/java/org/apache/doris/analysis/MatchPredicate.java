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
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TMatchPredicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.Objects;

/**
 * filed MATCH query_str
 */
public class MatchPredicate extends Predicate {

    public enum Operator {
        MATCH_ANY("MATCH_ANY", "match_any", TExprOpcode.MATCH_ANY),
        MATCH_ALL("MATCH_ALL", "match_all", TExprOpcode.MATCH_ALL),
        MATCH_PHRASE("MATCH_PHRASE", "match_phrase", TExprOpcode.MATCH_PHRASE),
        MATCH_PHRASE_PREFIX("MATCH_PHRASE_PREFIX", "match_phrase_prefix", TExprOpcode.MATCH_PHRASE_PREFIX),
        MATCH_REGEXP("MATCH_REGEXP", "match_regexp", TExprOpcode.MATCH_REGEXP),
        MATCH_PHRASE_EDGE("MATCH_PHRASE_EDGE", "match_phrase_edge", TExprOpcode.MATCH_PHRASE_EDGE);

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

        for (Type t : Type.getStringTypes()) {
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ANY.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ANY.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));

            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ALL.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_ALL.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));

            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_PHRASE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_PHRASE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_PHRASE_PREFIX.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_PHRASE_PREFIX.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_REGEXP.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_REGEXP.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_PHRASE_EDGE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t),
                    Type.BOOLEAN));
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_PHRASE_EDGE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(new ArrayType(t), t),
                    Type.BOOLEAN));
        }
    }

    @SerializedName("op")
    private Operator op;
    private String invertedIndexParser;
    private String invertedIndexParserMode;
    private Map<String, String> invertedIndexCharFilter;
    private boolean invertedIndexParserLowercase = true;
    private String invertedIndexParserStopwords = "";
    private String invertedIndexCustomAnalyzer = "";

    private MatchPredicate() {
        // use for serde only
        invertedIndexParser = InvertedIndexUtil.INVERTED_INDEX_PARSER_UNKNOWN;
        invertedIndexParserMode = InvertedIndexUtil.INVERTED_INDEX_PARSER_FINE_GRANULARITY;
    }

    public MatchPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        // TODO: Calculate selectivity
        selectivity = Expr.DEFAULT_SELECTIVITY;
        invertedIndexParser = InvertedIndexUtil.INVERTED_INDEX_PARSER_UNKNOWN;
        invertedIndexParserMode = InvertedIndexUtil.INVERTED_INDEX_PARSER_FINE_GRANULARITY;
    }

    protected MatchPredicate(MatchPredicate other) {
        super(other);
        op = other.op;
        invertedIndexParser = other.invertedIndexParser;
        invertedIndexParserMode = other.invertedIndexParserMode;
        invertedIndexCharFilter = other.invertedIndexCharFilter;
        invertedIndexParserLowercase = other.invertedIndexParserLowercase;
        invertedIndexParserStopwords = other.invertedIndexParserStopwords;
        invertedIndexCustomAnalyzer = other.invertedIndexCustomAnalyzer;
    }

    /**
     * use for Nereids ONLY
     */
    public MatchPredicate(Operator op, Expr e1, Expr e2, Type retType,
            NullableMode nullableMode, Index invertedIndex) {
        this(op, e1, e2);
        if (invertedIndex != null) {
            this.invertedIndexParser = invertedIndex.getInvertedIndexParser();
            this.invertedIndexParserMode = invertedIndex.getInvertedIndexParserMode();
            this.invertedIndexCharFilter = invertedIndex.getInvertedIndexCharFilter();
            this.invertedIndexParserLowercase = invertedIndex.getInvertedIndexParserLowercase();
            this.invertedIndexParserStopwords = invertedIndex.getInvertedIndexParserStopwords();
            this.invertedIndexCustomAnalyzer = invertedIndex.getInvertedIndexCustomAnalyzer();
        }
        fn = new Function(new FunctionName(op.name), Lists.newArrayList(e1.getType(), e2.getType()), retType,
                false, true, nullableMode);
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
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " " + op.toString() + " "
                + getChild(1).toSql(disableTableName, needExternalSql, tableType, table);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_PRED;
        msg.setOpcode(op.getOpcode());
        msg.match_predicate = new TMatchPredicate(invertedIndexParser, invertedIndexParserMode);
        msg.match_predicate.setCharFilterMap(invertedIndexCharFilter);
        msg.match_predicate.setParserLowercase(invertedIndexParserLowercase);
        msg.match_predicate.setParserStopwords(invertedIndexParserStopwords);
        msg.match_predicate.setCustomAnalyzer(invertedIndexCustomAnalyzer);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }
}
