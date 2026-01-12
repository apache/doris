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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.AIResource;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.Resource;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'AI_AGG'.
 */
public class AIAgg extends NullableAggregateFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                .args(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 1 argument.
     */
    public AIAgg(Expression arg0, Expression arg1) {
        this(new StringLiteral(getResourceName()), arg0, arg1);
    }

    /**
     * constructor with 2 argument.
     */
    public AIAgg(Expression arg0, Expression arg1, Expression arg2) {
        super("ai_agg", false, false, arg0, arg1, arg2);
    }

    /**
     * constructor for withChildren and reuse signature
     */
    private AIAgg(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (!child(arity() - 1).isLiteral()) {
            throw new AnalysisException("AI_AGG must accept literal for the task.");
        }

        if (arity() == 3) {
            //The resource must be literal
            if (!child(0).isLiteral() || !child(2).isLiteral()) {
                throw new AnalysisException("AI_AGG must accept literal for the resource name.");
            }

            //Check if the resource is valid
            String resourceName = getArgument(0).toString().replaceAll("^['\"]|['\"]$", "");
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
            if (!(resource instanceof AIResource)) {
                throw new AnalysisException("AI resource '" + resourceName + "' does not exist");
            }
        }
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkLegalityAfterRewrite();
    }

    @Override
    public AIAgg withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3,
                "AI_AGG only accept two or three parameters");
        return new AIAgg(getFunctionParams(distinct, children));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new AIAgg(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    /**
     *  If a user doesn't specify which resource to use when calling a function
     *  the current session variable will automatically allocate a resource.
     *  <p>
     *  1. First, attempt to use the user-specified argument passed to the function.
     *  2. If not available, try using the global default AI resource.
     */
    public static String getResourceName() throws AnalysisException {
        String resourceName = ConnectContext.get().getSessionVariable().defaultAIResource;
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new AnalysisException("Please specify the AI Resource in argument "
                + "or session variable.");
        }
        return resourceName;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAIAgg(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
