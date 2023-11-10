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
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The builder and factory for {@link org.apache.doris.nereids.parser.trino.TrinoFnCallTransformer},
 * and supply transform facade ability.
 */
public class TrinoFnCallTransformers {

    private static ImmutableListMultimap<String, AbstractFnCallTransformer> TRANSFORMER_MAP;
    private static ImmutableListMultimap<String, AbstractFnCallTransformer> COMPLEX_TRANSFORMER_MAP;
    private static final ImmutableListMultimap.Builder<String, AbstractFnCallTransformer> transformerBuilder =
            ImmutableListMultimap.builder();
    private static final ImmutableListMultimap.Builder<String, AbstractFnCallTransformer> complexTransformerBuilder =
            ImmutableListMultimap.builder();

    static {
        registerTransformers();
        registerComplexTransformers();
    }

    private TrinoFnCallTransformers() {
    }

    /**
     * Function transform facade
     */
    public static Function transform(String sourceFnName, List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        List<AbstractFnCallTransformer> transformers = getTransformers(sourceFnName);
        return doTransform(transformers, sourceFnName, sourceFnTransformedArguments, context);
    }

    private static Function doTransform(List<AbstractFnCallTransformer> transformers,
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

    private static List<AbstractFnCallTransformer> getTransformers(String sourceFnName) {
        ImmutableList<AbstractFnCallTransformer> fnCallTransformers =
                TRANSFORMER_MAP.get(sourceFnName);
        ImmutableList<AbstractFnCallTransformer> complexFnCallTransformers =
                COMPLEX_TRANSFORMER_MAP.get(sourceFnName);
        return ImmutableList.copyOf(Iterables.concat(fnCallTransformers, complexFnCallTransformers));
    }

    private static void registerTransformers() {
        registerStringFunctionTransformer();
        // TODO: add other function transformer
        // build transformer map in the end
        TRANSFORMER_MAP = transformerBuilder.build();
    }

    private static void registerComplexTransformers() {
        DateDiffFnCallTransformer dateDiffFnCallTransformer = new DateDiffFnCallTransformer();
        doRegister(dateDiffFnCallTransformer.getSourceFnName(), dateDiffFnCallTransformer);
        // TODO: add other complex function transformer
        // build complex transformer map in the end
        COMPLEX_TRANSFORMER_MAP = complexTransformerBuilder.build();
    }

    private static void registerStringFunctionTransformer() {
        doRegister("codepoint", 1, "ascii",
                Lists.newArrayList(PlaceholderExpression.of(Expression.class, 1)), false);
        // TODO: add other string function transformer
    }

    private static void doRegister(
            String sourceFnNme,
            int sourceFnArgumentsNum,
            String targetFnName,
            List<? extends Expression> targetFnArguments,
            boolean variableArgument) {

        List<Expression> castedTargetFnArguments = targetFnArguments
                .stream()
                .map(each -> (Expression) each)
                .collect(Collectors.toList());
        transformerBuilder.put(sourceFnNme, new TrinoFnCallTransformer(new UnboundFunction(
                targetFnName, castedTargetFnArguments), variableArgument, sourceFnArgumentsNum));
    }

    private static void doRegister(
            String sourceFnNme,
            AbstractFnCallTransformer transformer) {
        complexTransformerBuilder.put(sourceFnNme, transformer);
    }
}
