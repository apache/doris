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

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.RequiredPhysicalProperty;
import org.apache.doris.optimizer.operator.OptPhysicalDistribution;
import org.apache.doris.optimizer.operator.OptPhysicalSort;
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
    private final MultiExpression originMExpr;
    private final OptimizationContext optContext;
    private MultiExpression lastMExpr;

    public static void schedule(SearchContext sContext, OptGroup group, MultiExpression originMExpr,
                                OptimizationContext optContext, Task parent) {
        sContext.schedule(new TaskGroupOptimization(group, originMExpr, optContext, parent));
    }

    private TaskGroupOptimization(OptGroup group, MultiExpression originMExpr,
                                  OptimizationContext optContext, Task parent) {
        super(parent);
        this.group = group;
        this.originMExpr = originMExpr;
        this.optContext = optContext;
        this.nextState = new InitalizingState();
        this.lastMExpr = null;
        this.group.insert(optContext);
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
            boolean isScheduling;
            if (sContext.getSearchVariables().isExecuteOptimization()) {
                isScheduling = scheduleOptimizingMExpr(sContext);
            } else {
                isScheduling = scheduleExploringMExpr(sContext);
            }

            if (isScheduling) {
                return;
            }
            nextState = new CompletingState();
        }

        private boolean scheduleExploringMExpr(SearchContext sContext) {
            MultiExpression nextMExpr;
            if (lastMExpr == null) {
                nextMExpr = group.getFirstLogicalMultiExpression();
                Preconditions.checkNotNull(nextMExpr);
            } else {
                nextMExpr = group.nextLogicalExpr(lastMExpr);
            }

            boolean isScheduling = false;
            while (nextMExpr != null) {
                    TaskMultiExpressionOptimization.schedule(sContext, nextMExpr,
                            optContext, TaskGroupOptimization.this);
                    isScheduling = true;
                lastMExpr = nextMExpr;
                nextMExpr = group.nextPhysicalExpr(nextMExpr);
            }
            return isScheduling;
        }

        private boolean scheduleOptimizingMExpr(SearchContext sContext) {
            MultiExpression nextMExpr;
            if (lastMExpr == null) {
                nextMExpr = group.getFirstPhysicalMultiExpression();
                Preconditions.checkNotNull(nextMExpr);
            } else {
                nextMExpr = group.nextPhysicalExpr(lastMExpr);
            }

            boolean isScheduling = false;
            while (nextMExpr != null) {
                if (isSatifisyRequiredProperty(nextMExpr, sContext)) {
                    TaskMultiExpressionOptimization.schedule(sContext, nextMExpr,
                            optContext, TaskGroupOptimization.this);
                    isScheduling = true;
                }
                lastMExpr = nextMExpr;
                nextMExpr = group.nextPhysicalExpr(nextMExpr);
            }
            return isScheduling;
        }

        private boolean isSatifisyRequiredProperty(MultiExpression mExpr, SearchContext sContext) {
            if (!sContext.getSearchVariables().isExecuteOptimization()) {
                return true;
            }

            if (originMExpr == mExpr) {
                return false;
            }

            final RequiredPhysicalProperty requiredProperty = optContext.getRequiredPhysicalProperty();
            if (mExpr.getOp() instanceof OptPhysicalDistribution) {
                final OptPhysicalDistribution distribution = (OptPhysicalDistribution)mExpr.getOp();
                return requiredProperty.getDistributionProperty()
                        .isSatisfy(distribution.getDistributionSpec(null));
            }

            if (mExpr.getOp() instanceof OptPhysicalSort) {
                final OptPhysicalSort sort = (OptPhysicalSort)mExpr.getOp();
                return requiredProperty.getOrderProperty().isSatisfy(sort.getOrderSpec(null));
            }
            return true;
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
