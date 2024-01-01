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

package org.apache.doris.nereids.parser.trino;

import org.apache.doris.nereids.analyzer.PlaceholderExpression;
import org.apache.doris.nereids.parser.AbstractFnCallTransformers;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;

/**
 * The builder and factory for trino function call transformers,
 * and supply transform facade ability.
 */
public class TrinoFnCallTransformers extends AbstractFnCallTransformers {

    private TrinoFnCallTransformers() {
    }

    @Override
    protected void registerTransformers() {
        registerStringFunctionTransformer();
        // TODO: add other function transformer
    }

    @Override
    protected void registerComplexTransformers() {
        DateDiffFnCallTransformer dateDiffFnCallTransformer = new DateDiffFnCallTransformer();
        doRegister(dateDiffFnCallTransformer.getSourceFnName(), dateDiffFnCallTransformer);
        // TODO: add other complex function transformer
    }

    protected void registerStringFunctionTransformer() {
        doRegister("codepoint", "ascii",
                Lists.newArrayList(PlaceholderExpression.of(Expression.class, 1)));
        // TODO: add other string function transformer
    }

    static class SingletonHolder {
        private static final TrinoFnCallTransformers INSTANCE = new TrinoFnCallTransformers();
    }

    public static TrinoFnCallTransformers getSingleton() {
        return SingletonHolder.INSTANCE;
    }
}
