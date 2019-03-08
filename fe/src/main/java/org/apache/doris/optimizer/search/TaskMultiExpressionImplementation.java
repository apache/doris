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

package org.apache.doris.optimizer.search;

import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.operator.OptLogical;
import org.apache.doris.optimizer.rule.OptRule;

import java.util.List;

/**
 * For creating physical implementations of the MultiExpression and it's children group.
 *
 * +--------------------------------+  Children group are not scheduled for implementing.
 * |                                |------------------>+
 * |   ImplementingChildrenStatus   |                   | Child StateMachine
 * |                                |<------------------+
 * +--------------------------------+
 *                |
 *                |  Children have been scheduled for implementing.
 *                V
 * +--------------------------------+   Rules are not applied for MultiExpression.
 * |                                |------------------>+
 * |    ImplementingSelfStatus      |                   | Child StateMachine
 * |                                |<------------------+
 * +--------------------------------+
 *                |
 *                |  Rules have been applied for MultiExpression.
 *                V
 * +--------------------------------+
 * |                                |
 * |       CompletingState          |
 * |                                |
 * +--------------------------------+
 *                |
 *                |
 *                V
 *        Parent StateMachine
 */
public class TaskMultiExpressionImplementation extends Task {

    private final MultiExpression mExpr;
    private boolean isApplyTaskScheduled;
    private boolean isApplyTaskScheduledForChildren;

    private TaskMultiExpressionImplementation(MultiExpression mExpr, Task parent) {
        super(parent);
        this.mExpr = mExpr;
        this.isApplyTaskScheduled = false;
        this.isApplyTaskScheduledForChildren = false;
        this.nextState = new ImplementingChildrenStatus();
    }

    public static void schedule(SchedulerContext sContext, MultiExpression mExpr, Task parent) {
        sContext.schedule(new TaskMultiExpressionImplementation(mExpr, parent));
    }

    private class ImplementingChildrenStatus extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            mExpr.setStatus(MultiExpression.MEState.Implementing);
            if (!isApplyTaskScheduledForChildren) {
                for (OptGroup group : mExpr.getInputs()) {
                    TaskGroupImplementation.schedule(sContext, group, TaskMultiExpressionImplementation.this);
                }
                isApplyTaskScheduledForChildren = true;
                return;
            }

            nextState = new ImplementingSelfStatus();
        }
    }

    private class ImplementingSelfStatus extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            final OptLogical optLogical = (OptLogical) mExpr.getOp();
            if (!isApplyTaskScheduled) {
                // It's necessary to apply explore rules firstly before apply implement rules. and Scheduling
                // queue is FILO.
                final List<OptRule> candidateRulesForImplement = optLogical.getCandidateRulesForImplement();
                for (OptRule rule : candidateRulesForImplement) {
                    TaskRuleApplication.schedule(sContext, mExpr, rule, TaskMultiExpressionImplementation.this);
                }

                final List<OptRule> candidateRulesForExplore = optLogical.getCandidateRulesForExplore();
                for (OptRule rule : candidateRulesForExplore) {
                    TaskRuleApplication.schedule(sContext, mExpr, rule, TaskMultiExpressionImplementation.this);
                }

                isApplyTaskScheduled = true;
                return;
            }

            nextState = new CompletingStatus();
        }
    }

    private class CompletingStatus extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            mExpr.setStatus(MultiExpression.MEState.Implemented);
            setFinished();
        }
    }
}
