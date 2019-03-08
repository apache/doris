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
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.OptMemo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Stack;

public class SchedulerContextImp implements SchedulerContext {
    private final static Logger LOG = LogManager.getLogger(SchedulerContextImp.class);

    private final Stack<TaskStateMachine> tasks;
    private final OptMemo memo;
    private long startSearchingTime;
    private long taskTotal;
    private long tasksSuspendTotal;
    private long tasksResumeTotal;

    private SchedulerContextImp(OptMemo memo) {
        this.memo = memo;
        this.tasks = new Stack<>();
    }

    public static SchedulerContext create(OptMemo memo, OptGroup firstGroup, OptimizationContext oContext) {
        final SchedulerContext sContext = new SchedulerContextImp(memo);
        TaskGroupOptimization.schedule(sContext, firstGroup, oContext, null);
        return sContext;
    }

    private void resetEnv() {
        this.tasks.clear();
        this.startSearchingTime = System.currentTimeMillis();
        this.taskTotal = 0;
        this.tasksSuspendTotal = 0;
        this.tasksResumeTotal = 0;
    }

    private void printSearchInfo() {
        Preconditions.checkState(tasksResumeTotal == tasksSuspendTotal);
        Preconditions.checkState(taskTotal > 0);
        final long finishSearchingTime = System.currentTimeMillis();
        final StringBuilder strBuilder = new StringBuilder("Searching finished, ");
        strBuilder.append(" time:").append(finishSearchingTime - startSearchingTime)
                .append(" task total:").append(taskTotal)
                .append(" suspend total:").append(tasksSuspendTotal)
                .append(" resume total:").append(tasksResumeTotal);
        LOG.info(strBuilder.toString());
    }

    private void preExecute(TaskStateMachine task) {
        final TaskStateMachine parent = task.getParent();
        if (parent != null && !parent.isFinished()) {
            parent.increaseRefByChildren();
        }
    }

    /**
     * @param task: the task finished execute()
     * @return true: parent will resume, false: parent won't resume.
     */
    private boolean finishExecute(TaskStateMachine task) {
        boolean resumeParent = false;
        final TaskStateMachine parent = task.getParent();
        if (parent != null && !parent.isFinished()) {
            parent.decreaseRefByChildren();
            if (!parent.isRefByChildren()) {
                resumeParent = true;
            }
        }
        return resumeParent;
    }

    @Override
    public void execute() {
        resetEnv();
        while (true) {
            final TaskStateMachine task = tasks.pop();
            if (task == null) {
                // Searching finished.
                break;
            }
            preExecute(task);
            task.execute(this);
            if (task.isFinished() && finishExecute(task)) {
                // Schedule parent task again.
                tasks.push(task.getParent());
                tasksResumeTotal++;
                continue;
            } else {
                // The task will be scheduled again by the last
                // child when all children finished.
                tasksSuspendTotal++;
            }
        }
        printSearchInfo();
    }

    @Override
    public void schedule(TaskStateMachine task) {
        tasks.push(task);
        taskTotal++;
    }

    @Override
    public OptMemo getMemo() { return memo; }
}
