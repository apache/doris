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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.WindowFunctionChecker;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Check and standardize Window expression
 */
public class CheckAndStandardizeWindowFunctionAndFrame extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return RuleType.CHECK_AND_STANDARDIZE_WINDOW_FUNCTION_AND_FRAME.build(
            logicalWindow().whenNot(LogicalWindow::isChecked).then(logicalWindow ->
                checkAndStandardize(logicalWindow))
        );
    }

    private LogicalWindow checkAndStandardize(LogicalWindow<Plan> logicalWindow) {

        List<NamedExpression> newOutputExpressions = logicalWindow.getWindowExpressions().stream()
                .map(expr -> {
                    WindowExpression window = (WindowExpression) expr.child(0);
                    WindowFunctionChecker checker = new WindowFunctionChecker(window);
                    checker.checkWindowBeforeFunc();
                    checker.checkWindowFunction();
                    checker.checkWindowAfterFunc();
                    WindowExpression newWindow = checker.getWindow();
                    Preconditions.checkArgument(newWindow.getWindowFrame().isPresent(),
                            "WindowFrame shouldn't be null after checkAndStandardize");
                    return (Alias) expr.withChildren(newWindow);
                })
                .collect(Collectors.toList());
        return logicalWindow.withChecked(newOutputExpressions, logicalWindow.child());
    }
}
