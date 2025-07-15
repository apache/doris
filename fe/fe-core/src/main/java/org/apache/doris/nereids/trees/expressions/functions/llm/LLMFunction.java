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

package org.apache.doris.nereids.trees.expressions.functions.llm;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LLMResource;
import org.apache.doris.catalog.Resource;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

/**
 * Base class for LLM related functions.
 */
public abstract class LLMFunction extends ScalarFunction
        implements AlwaysNullable, ExplicitlyCastableSignature {
    /**
     * constructor with at least 1 argument.
     */
    public LLMFunction(String name, Expression... expressions) {
        super(name, expressions);
        if (children.isEmpty()) {
            throw new AnalysisException(name + " requires at least 1 argument.");
        }
    }

    public abstract int getMaxArgsNum();

    @Override
    public void checkLegalityAfterRewrite() {
        if (arity() == getMaxArgsNum()) {
            String resourceName = getArgument(0).toString().replaceAll("^['\"]|['\"]$", "");
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
            if (!(resource instanceof LLMResource)) {
                throw new AnalysisException("LLM resource '" + resourceName + "' does not exist");
            }
        }
    }

    /**
     *  If a user doesn't specify which resource to use when calling a function
     *  the current session variable will automatically allocate a resource.
     *  <p>
     *  1. First, attempt to use the user-specified argument passed to the function.
     *  2. If not available, try using a specific function-level session, e.g., default_llm_xxx_resource.
     *  3. If that also fails, fall back to the global default LLM resource.
     */
    public static String getResourceName(String functionName) throws AnalysisException {
        String resourceName = "";
        switch (functionName) {
            case "llm_classify":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMClassifyResource;
                break;
            case "llm_extract":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMExtractResource;
                break;
            case "llm_fixgrammar":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMFixGrammarResource;
                break;
            case "llm_generate":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMGenerateResource;
                break;
            case "llm_mask":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMMaskResource;
                break;
            case "llm_sentiment":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMSentimentResource;
                break;
            case "llm_summarize":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMSummarizeResource;
                break;
            case "llm_translate":
                resourceName = ConnectContext.get().getSessionVariable().defaultLLMTranslateResource;
                break;
            default:
                throw new AnalysisException("Unknown LLM_Function: " + functionName);
        }
        if (Strings.isNullOrEmpty(resourceName)) {
            resourceName = ConnectContext.get().getSessionVariable().defaultLLMResource;
            if (Strings.isNullOrEmpty(resourceName)) {
                throw new AnalysisException("Please specify the LLM Resource in argument "
                        + "or session variable.");
            }
        }

        return resourceName;
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkLegalityAfterRewrite();
    }

    @Override
    public StringType getDataType() {
        return StringType.INSTANCE;
    }

    @Override
    public abstract <R, C> R accept(ExpressionVisitor<R, C> visitor, C context);
}
