package org.apache.doris.optimizer.search;

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


import com.google.common.collect.Lists;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptBinding;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.rule.OptRule;

import java.util.List;

/**
 * For applying rule to expression.
 *
 * +--------------------------------+
 * |                                |
 * |         ApplyingStatus         |
 * |                                |
 * +--------------------------------+
 *                |
 *                | finished
 *                V
 *        Parent StateMachine
 */
public class TaskRuleApplication extends TaskStateMachine {

    private final OptRule rule;
    private final MultiExpression mExpr;

    private TaskRuleApplication(MultiExpression mExpr, OptRule rule, TaskStateMachine parent) {
        super(CTaskType.RuleApplication, parent);
        this.mExpr = mExpr;
        this.rule = rule;
        this.currentState = new ApplyingStatus();
    }

    public static void schedule(SchedulerContext sContext, MultiExpression mExpr, OptRule rule,
                                TaskStateMachine parent) {
        sContext.schedule(new TaskRuleApplication(mExpr, rule, parent));
    }

    private class ApplyingStatus extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            if (!rule.isCompatible(mExpr.getRuleTypeDerivedFrom())) {
                setFinished();
                return;
            }
            // Transform
            final OptExpression pattern = rule.getPattern();
            OptExpression lastExpr = null;
            OptExpression extractExpr = OptBinding.bind(pattern, mExpr, lastExpr);
            final List<OptExpression> newExprs = Lists.newArrayList();
            while (extractExpr != null) {
                final OptGroup group = mExpr.getGroup();
                rule.transform(extractExpr, newExprs);
                if (rule.isApplyOnce()) {
                    break;
                }
                lastExpr = extractExpr;
                extractExpr = OptBinding.bind(pattern, mExpr, lastExpr);
            }
            // Insert into memo
            for (OptExpression expr : newExprs) {
                sContext.getMemo().copyIn(mExpr.getGroup(), expr);
            }
            setFinished();
        }
    }
}
