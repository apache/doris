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

import org.apache.doris.nereids.analyzer.PlaceholderExpression;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Common function transformer,
 * can transform functions which the size of function arguments is the same with the source function.
 */
public class CommonFnCallTransformer extends AbstractFnCallTransformer {
    private final UnboundFunction targetFunction;
    private final List<PlaceholderExpression> targetArguments;

    /**
     * Common function transformer, mostly this handle common function.
     */
    public CommonFnCallTransformer(UnboundFunction targetFunction) {
        this.targetFunction = targetFunction;
        PlaceholderCollector placeHolderCollector = new PlaceholderCollector();
        placeHolderCollector.visit(targetFunction, null);
        this.targetArguments = placeHolderCollector.getPlaceholderExpressions();
    }

    @Override
    protected boolean check(String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        List<Class<? extends Expression>> sourceFnTransformedArgClazz = sourceFnTransformedArguments.stream()
                .map(Expression::getClass)
                .collect(Collectors.toList());
        if (sourceFnTransformedArguments.size() != targetArguments.size()) {
            return false;
        }
        for (PlaceholderExpression targetArgument : targetArguments) {
            int position = targetArgument.getPosition();
            if (!targetArgument.getDelegateClazz().isAssignableFrom(sourceFnTransformedArgClazz.get(position - 1))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Function transform(String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        List<Expression> sourceFnTransformedArgumentsInorder = Lists.newArrayList();
        for (PlaceholderExpression placeholderExpression : targetArguments) {
            Expression expression = sourceFnTransformedArguments.get(placeholderExpression.getPosition() -1);
            sourceFnTransformedArgumentsInorder.add(expression);
        }
        return targetFunction.withChildren(sourceFnTransformedArgumentsInorder);
    }

    /**
     * This is the collector for placeholder expression, which placeholder expression
     * identify the expression that we want to use later but current now is not confirmed.
     */
    public static final class PlaceholderCollector extends DefaultExpressionVisitor<Void, Void> {

        private final List<PlaceholderExpression> placeholderExpressions = new ArrayList<>();

        public PlaceholderCollector() {}

        @Override
        public Void visitPlaceholderExpression(PlaceholderExpression placeholderExpression, Void context) {
            placeholderExpressions.add(placeholderExpression);
            return null;
        }

        public List<PlaceholderExpression> getPlaceholderExpressions() {
            return placeholderExpressions;
        }
    }
}
