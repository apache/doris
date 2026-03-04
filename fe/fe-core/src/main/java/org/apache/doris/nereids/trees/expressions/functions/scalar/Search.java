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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'search' - simplified architecture similar to MultiMatch.
 * Handles DSL parsing and generates SearchPredicate during translation.
 * <p>
 * Supports 1-2 parameters:
 * - search(dsl_string): Traditional usage with field specified in DSL
 * - search(dsl_string, options): With JSON options for configuration
 * <p>
 * Options parameter (JSON format):
 * - default_field: default field name when DSL doesn't specify field
 * - default_operator: "and" or "or" for multi-term queries (default: "and")
 * - mode: "standard" (default) or "lucene" (ES/Lucene-style boolean parsing)
 * - minimum_should_match: integer for Lucene mode (default: 0 for filter context)
 * <p>
 * Example options: '{"default_field":"title","mode":"lucene","minimum_should_match":0}'
 */
public class Search extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            // Original signature: search(dsl_string)
            FunctionSignature.ret(BooleanType.INSTANCE).args(StringType.INSTANCE),
            // With options: search(dsl_string, options)
            FunctionSignature.ret(BooleanType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE)
    );

    public Search(Expression... varArgs) {
        super("search", varArgs);
    }

    private Search(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Search withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 2,
                "search() requires 1-2 arguments");
        return new Search(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    /**
     * Get DSL string from first argument
     */
    public String getDslString() {
        Expression dslArg = child(0);
        if (dslArg instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) dslArg).getStringValue();
        }
        return dslArg.toString();
    }

    /**
     * Get options JSON string from second argument (optional).
     * Options is a JSON string containing all configuration:
     * - default_field: default field name when DSL doesn't specify field
     * - default_operator: "and" or "or" for multi-term queries
     * - mode: "standard" or "lucene"
     * - minimum_should_match: integer for Lucene mode
     * Example: '{"default_field":"title","mode":"lucene","minimum_should_match":0}'
     */
    public String getOptionsJson() {
        if (children().size() < 2) {
            return null;
        }
        Expression optionsArg = child(1);
        if (optionsArg instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) optionsArg).getStringValue();
        }
        return optionsArg.toString();
    }

    /**
     * Get parsed DSL plan - deferred to translation phase
     * This will be handled by SearchPredicate during ExpressionTranslator.visitSearch()
     */
    public SearchDslParser.QsPlan getQsPlan() {
        // Lazy evaluation will be handled in SearchPredicate
        return SearchDslParser.parseDsl(getDslString(), getOptionsJson());
    }

    @Override
    public <R, C> R accept(org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor<R, C> visitor,
            C context) {
        return visitor.visitSearch(this, context);
    }
}


