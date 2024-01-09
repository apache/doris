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
 * can transform functions which the size and type of target arguments are both the same with the source function,
 * or source function is a variable-arguments function.
 */
public class CommonFnCallTransformer extends AbstractFnCallTransformer {
    private final UnboundFunction targetFunction;
    private final List<PlaceholderExpression> targetArguments;

    // true means the arguments of this function is dynamic, for example:
    // - named_struct('f1', 1, 'f2', 'a', 'f3', "abc")
    // - struct(1, 'a', 'abc');
    private final boolean variableArguments;

    /**
     * Common function transformer, mostly this handle common function.
     */
    public CommonFnCallTransformer(UnboundFunction targetFunction, boolean variableArguments) {
        this.targetFunction = targetFunction;
        PlaceholderCollector placeHolderCollector = new PlaceholderCollector();
        placeHolderCollector.visit(targetFunction, null);
        this.targetArguments = placeHolderCollector.getPlaceholderExpressions();
        this.variableArguments = variableArguments;
    }

    public CommonFnCallTransformer(UnboundFunction targetFunction) {
        this.targetFunction = targetFunction;
        PlaceholderCollector placeHolderCollector = new PlaceholderCollector();
        placeHolderCollector.visit(targetFunction, null);
        this.targetArguments = placeHolderCollector.getPlaceholderExpressions();
        this.variableArguments = false;
    }

    @Override
    protected boolean check(String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        // if variableArguments=true, we can not recognize if the type of all arguments is valid or not,
        // because:
        //     1. the argument size is not sure
        //     2. there are some functions which can accept different types of arguments,
        //        for example: struct(1, 'a', 'abc')
        // so just return true here.
        if (variableArguments) {
            return true;
        }
        List<Class<? extends Expression>> sourceFnTransformedArgClazz = sourceFnTransformedArguments.stream()
                .map(Expression::getClass)
                .collect(Collectors.toList());
        if (sourceFnTransformedArguments.size() != targetArguments.size()) {
            return false;
        }
        for (PlaceholderExpression targetArgument : targetArguments) {
            // replace the arguments of target function by the position of target argument
            int position = targetArgument.getPosition();
            Class<? extends Expression> sourceArgClazz = sourceFnTransformedArgClazz.get(position - 1);
            boolean valid = false;
            for (Class<? extends Expression> targetArgClazz : targetArgument.getDelegateClazzSet()) {
                if (targetArgClazz.isAssignableFrom(sourceArgClazz)) {
                    valid = true;
                    break;
                }
            }
            if (!valid) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Function transform(String sourceFnName,
            List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        if (variableArguments) {
            // not support adjust the order of arguments when variableArguments=true
            return targetFunction.withChildren(sourceFnTransformedArguments);
        }
        List<Expression> sourceFnTransformedArgumentsInorder = Lists.newArrayList();
        for (PlaceholderExpression placeholderExpression : targetArguments) {
            Expression expression = sourceFnTransformedArguments.get(placeholderExpression.getPosition() - 1);
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
