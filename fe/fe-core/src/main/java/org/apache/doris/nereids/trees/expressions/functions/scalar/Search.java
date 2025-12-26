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
 * Supports 1-3 parameters:
 * - search(dsl_string): Traditional usage
 * - search(dsl_string, default_field): Simplified syntax with default field
 * - search(dsl_string, default_field, default_operator): Full control over expansion
 */
public class Search extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            // Original signature: search(dsl_string)
            FunctionSignature.ret(BooleanType.INSTANCE).args(StringType.INSTANCE),
            // With default field: search(dsl_string, default_field)
            FunctionSignature.ret(BooleanType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE),
            // With default field and operator: search(dsl_string, default_field, default_operator)
            FunctionSignature.ret(BooleanType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE,
                    StringType.INSTANCE)
    );

    public Search(Expression... varArgs) {
        super("search", varArgs);
    }

    private Search(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Search withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 3,
                "search() requires 1-3 arguments");
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
     * Get default field from second argument (optional)
     */
    public String getDefaultField() {
        if (children().size() < 2) {
            return null;
        }
        Expression fieldArg = child(1);
        if (fieldArg instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) fieldArg).getStringValue();
        }
        return fieldArg.toString();
    }

    /**
     * Get default operator from third argument (optional)
     */
    public String getDefaultOperator() {
        if (children().size() < 3) {
            return null;
        }
        Expression operatorArg = child(2);
        if (operatorArg instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) operatorArg).getStringValue();
        }
        return operatorArg.toString();
    }

    /**
     * Get parsed DSL plan - deferred to translation phase
     * This will be handled by SearchPredicate during ExpressionTranslator.visitSearch()
     */
    public SearchDslParser.QsPlan getQsPlan() {
        // Lazy evaluation will be handled in SearchPredicate
        return SearchDslParser.parseDsl(getDslString(), getDefaultField(), getDefaultOperator());
    }

    @Override
    public <R, C> R accept(org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor<R, C> visitor,
            C context) {
        return visitor.visitSearch(this, context);
    }
}


