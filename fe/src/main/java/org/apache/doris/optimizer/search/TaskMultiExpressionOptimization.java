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

import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;

/**
 * For searching the best plan rooted by the multi expression under the optimization context.
 *
 * +--------------------------------+
 * |                                |    finished
 * |       InitalizationState       |------------------> Parent StateMachine
 * |                                |
 * +--------------------------------+
 *                |
 *                |  Running
 *                V
 * +--------------------------------+    Suspending
 * |                                |------------------>+
 * |      OptimizingChildGroup      |                   | Child StateMachine
 * |                                |<------------------+
 * +--------------------------------+    Resuming
 *                |
 *                |  Running
 *                V
 * +--------------------------------+
 * |                                |
 * |        AddingEnforcer          |
 * |                                |
 * +--------------------------------+
 *                |
 *                |  Running
 *                V
 * +--------------------------------+
 * |                                |
 * |        OptimizingSelf          |
 * |                                |
 * +--------------------------------+
 *                |
 *                |  Running
 *                V
 * +--------------------------------+
 * |                                |
 * |    CompletingOptimization      |
 * |                                |
 * +--------------------------------+
 *                |
 *                |  finished
 *                V
 *        Parent StateMachine
 */
public class TaskMultiExpressionOptimization extends TaskStateMachine {

    private final MultiExpression mExpr;
    private final OptimizationContext optContext;

    private TaskMultiExpressionOptimization(MultiExpression mExpr, OptimizationContext optContext,
                                            TaskStateMachine parent) {
        super(CTaskType.MultiExpressionOptimization, parent);
        this.mExpr = mExpr;
        this.optContext = optContext;
        this.currentState = new InitalizationState();
    }

    public static void schedule(SchedulerContext sContext, MultiExpression mExpr,
                                OptimizationContext optContext, TaskStateMachine parent) {
        sContext.schedule(new TaskMultiExpressionOptimization(mExpr, optContext, parent));
    }

    private class InitalizationState extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            final RequestProperty requestProperty = optContext.getRequestProperty();
            if (checkProperty(requestProperty)) {
                setFinished();
                return;
            }
            if (canPrune()) {
                setFinished();
                return;
            }
            currentState = new OptimizingChildGroup();
            setRunning();
        }

        private boolean checkProperty(RequestProperty requestProperty) {
            return false;
        }

        private boolean canPrune() {
            return false;
        }
    }

    private class OptimizingChildGroup extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            boolean hasNew = false;
            for (OptGroup group : mExpr.getInputs()) {
                if (!group.isOptimized()) {
                    final OptimizationContext childOptimzationContext = deriveChildOptConext(mExpr, group);
                    TaskGroupOptimization.schedule(sContext, group, childOptimzationContext,
                            TaskMultiExpressionOptimization.this);
                    hasNew = true;
                }
            }

            if (hasNew) {
                setSuspending();
                return;
            }

            currentState = new AddingEnforcer();
            setRunning();
        }

        private OptimizationContext deriveChildOptConext(MultiExpression parent, OptGroup child) {
            return new OptimizationContext();
        }
    }


    private class AddingEnforcer extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            currentState = new OptimizingSelf();
            setRunning();
        }
    }


    private class OptimizingSelf extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            currentState = new CompletingOptimization();
            setRunning();
        }
    }

    private class CompletingOptimization extends TaskState {

        @Override
        public void handle(SchedulerContext sContext) {
            setFinished();
        }
    }
}
