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
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * For searching best Plan in the group under the optimization context.
 *
 * +--------------------------------+  Group is not implemented
 * |                                |------------------>+
 * |        InitalizingState        |                   | Child StateMachine
 * |                                |<------------------+
 * +--------------------------------+
 *                |
 *                |  Group is implemented
 *                V
 * +--------------------------------+   MultiExpression is not implemented.
 * |                                |------------------>+
 * | OptimizingMultiExpressionState |                   | Child StateMachine
 * |                                |<------------------+
 * +--------------------------------+
 *                |
 *                |  MultiExpression are all implemented.
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
public class TaskGroupOptimization extends Task {
    private final static Logger LOG = LogManager.getLogger(TaskGroupOptimization.class);

    private final OptGroup group;
    private final OptimizationContext optContext;
    private int lastMexprIndex;

    public static void schedule(SearchContext sContext, OptGroup group,
                                OptimizationContext optContext, Task parent) {
        sContext.schedule(new TaskGroupOptimization(group, optContext, parent));
    }

    private TaskGroupOptimization(OptGroup group, OptimizationContext optContext, Task parent) {
        super(parent);
        this.group = group;
        this.optContext = optContext;
        this.nextState = new InitalizingState();
        this.lastMexprIndex = 0;
    }

    private class InitalizingState extends TaskState {

        @Override
        public void handle(SearchContext sContext) {
            if (!group.isImplemented()) {
                TaskGroupImplementation.schedule(sContext, group, TaskGroupOptimization.this);
                return;
            }
            group.setStatus(OptGroup.GState.Optimizing);
            nextState = new OptimizingMultiExpressionState();
        }
    }

    private class OptimizingMultiExpressionState extends TaskState {

        @Override
        public void handle(SearchContext sContext) {
            boolean hasNew = false;
            for (; lastMexprIndex < group.getMultiExpressions().size(); lastMexprIndex++) {
                final MultiExpression mExpr = group.getMultiExpressions().get(lastMexprIndex);
                if (!mExpr.getOp().isPhysical() && sContext.getSearchVariables().isExecuteOptimization()
                        && mExpr.isImplemented()) {
                    continue;
                }

                TaskMultiExpressionOptimization.schedule(sContext, mExpr,
                        optContext,TaskGroupOptimization.this);
                hasNew = true;
            }

            if (hasNew) {
                return;
            }

            nextState = new CompletingState();
        }
    }

    private class CompletingState extends TaskState {

        @Override
        public void handle(SearchContext sContext) {
            group.setStatus(OptGroup.GState.Optimized);
            setFinished();
        }
    }
}
