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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The abstract holder for {@link AbstractFnCallTransformer},
 * and supply transform facade ability.
 */
public abstract class AbstractFnCallTransformers {

    private final ImmutableListMultimap<String, AbstractFnCallTransformer> transformerMap;
    private final ImmutableListMultimap<String, AbstractFnCallTransformer> complexTransformerMap;
    private final ImmutableListMultimap.Builder<String, AbstractFnCallTransformer> transformerBuilder =
            ImmutableListMultimap.builder();
    private final ImmutableListMultimap.Builder<String, AbstractFnCallTransformer> complexTransformerBuilder =
            ImmutableListMultimap.builder();

    protected AbstractFnCallTransformers() {
        registerTransformers();
        transformerMap = transformerBuilder.build();
        registerComplexTransformers();
        // build complex transformer map in the end
        complexTransformerMap = complexTransformerBuilder.build();
    }

    /**
     * Function transform facade
     */
    public Function transform(String sourceFnName, List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        List<AbstractFnCallTransformer> transformers = getTransformers(sourceFnName);
        return doTransform(transformers, sourceFnName, sourceFnTransformedArguments, context);
    }

    private Function doTransform(List<AbstractFnCallTransformer> transformers,
            String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        for (AbstractFnCallTransformer transformer : transformers) {
            if (transformer.check(sourceFnName, sourceFnTransformedArguments, context)) {
                Function transformedFunction =
                        transformer.transform(sourceFnName, sourceFnTransformedArguments, context);
                if (transformedFunction == null) {
                    continue;
                }
                return transformedFunction;
            }
        }
        return null;
    }

    protected void doRegister(
            String sourceFnNme,
            String targetFnName,
            List<? extends Expression> targetFnArguments) {

        List<Expression> castedTargetFnArguments = targetFnArguments
                .stream()
                .map(each -> (Expression) each)
                .collect(Collectors.toList());
        transformerBuilder.put(sourceFnNme, new CommonFnCallTransformer(new UnboundFunction(
                targetFnName, castedTargetFnArguments)));
    }

    protected void doRegister(
            String sourceFnNme,
            AbstractFnCallTransformer transformer) {
        complexTransformerBuilder.put(sourceFnNme, transformer);
    }

    private List<AbstractFnCallTransformer> getTransformers(String sourceFnName) {
        ImmutableList<AbstractFnCallTransformer> fnCallTransformers =
                transformerMap.get(sourceFnName);
        ImmutableList<AbstractFnCallTransformer> complexFnCallTransformers =
                complexTransformerMap.get(sourceFnName);
        return ImmutableList.copyOf(Iterables.concat(fnCallTransformers, complexFnCallTransformers));
    }

    protected abstract void registerTransformers();

    protected abstract void registerComplexTransformers();
}
