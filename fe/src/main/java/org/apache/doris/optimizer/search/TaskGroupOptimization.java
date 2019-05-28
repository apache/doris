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
    private final OptimizationContext optContext;
    private MultiExpression lastMExpr;

    public static void schedule(SearchContext sContext, OptGroup group,
                                OptimizationContext optContext, Task parent) {
        sContext.schedule(new TaskGroupOptimization(group, optContext, parent));
    }

    private TaskGroupOptimization(OptGroup group, OptimizationContext optContext, Task parent) {
        super(parent);
        this.group = group;
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
            boolean isSchedulingMExprTask = false;
            while (lastMExpr == null || lastMExpr.next() != null) {
                if (sContext.getSearchVariables().isExecuteOptimization()) {
                    if (lastMExpr == null) {
                        lastMExpr = group.getFirstPhysicalMultiExpression();
                    } else {
                        lastMExpr = group.nextPhysicalExpr(lastMExpr);
                    }
                } else {
                    // Only explore.
                    if (lastMExpr == null) {
                        lastMExpr = group.getFirstLogicalMultiExpression();
                    } else {
                        lastMExpr = group.nextLogicalExpr(lastMExpr);
                    }
                }
                if (isSatifisyRequiredProperty(lastMExpr, sContext)) {
                    TaskMultiExpressionOptimization.schedule(sContext, lastMExpr,
                            optContext, TaskGroupOptimization.this);
                    isSchedulingMExprTask = true;
                }
            }

            if (isSchedulingMExprTask) {
                return;
            }

            nextState = new CompletingState();
        }


        private boolean isSatifisyRequiredProperty(MultiExpression mExpr, SearchContext sContext) {
            if (!sContext.getSearchVariables().isExecuteOptimization()) {
                return true;
            }

            final RequiredPhysicalProperty requiredProperty = optContext.getReqdPhyProp();
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
