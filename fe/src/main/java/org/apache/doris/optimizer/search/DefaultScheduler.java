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
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.Stack;

/**
 * It is responsible for scheduling all the tasks, this is is FILO,
 */
public class DefaultScheduler implements Scheduler {
    private final static Logger LOG = LogManager.getLogger(DefaultScheduler.class);

    private final Stack<Task> tasks;
    private final Set<Task> pendingTasks;
    private long startSearchingTime;
    private long taskTotal;
    private long tasksSuspendByChildrenTotal;
    private long tasksResumeFromChildrenTotal;
    private long tasksSuspendBySelfTotal;

    private DefaultScheduler() {
        this.tasks = new Stack<>();
        this.tasks.clear();
        this.pendingTasks = Sets.newHashSet();
        resetEnv();
    }

    public static Scheduler create() {
        return new DefaultScheduler();
    }

    private void resetEnv() {
        this.startSearchingTime = System.currentTimeMillis();
        this.taskTotal = 0;
        this.tasksSuspendByChildrenTotal = 0;
        this.tasksResumeFromChildrenTotal = 0;
        this.tasksSuspendBySelfTotal = 0;
    }

    /**
     * @param task: the task finished execute()
     * @return true: parent will resume, false: parent won't resume.
     */
    private boolean resumeParent(Task task) {
        final Task parent = task.getParent();
        if (parent != null && !parent.isFinished()) {
            parent.decreaseRefByChildren();
            if (!parent.isRefByChildren()) {
                return true;
            }
        }
        return false;
    }

    private void printSearchInfo() {
        Preconditions.checkState(tasksResumeFromChildrenTotal == tasksSuspendByChildrenTotal);
        Preconditions.checkState(taskTotal > 0);
        final long finishSearchingTime = System.currentTimeMillis();
        final StringBuilder strBuilder = new StringBuilder("Searching finished:\n");
        strBuilder.append(" time:").append(finishSearchingTime - startSearchingTime).append("\n")
                .append(" task total:").append(taskTotal).append("\n")
                .append(" suspend by children total:").append(tasksSuspendByChildrenTotal).append("\n")
                .append(" resume by children total:").append(tasksResumeFromChildrenTotal).append("\n")
                .append(" suspend and resume by selft:").append(tasksSuspendBySelfTotal);
        LOG.info(strBuilder.toString());
    }

    @Override
    public void run(SearchContext sContext) {
        while (true) {
            if (tasks.isEmpty()) {
                // Searching finished.
                break;
            }
            final Task task = tasks.pop();
            task.execute(sContext);
            if (task.isFinished()) {
                if (resumeParent(task)) {
                    // Schedule parent task again.
                    tasks.push(task.getParent());
                    pendingTasks.remove(task.getParent());
                    tasksResumeFromChildrenTotal++;
                }
                continue;
            } else {
                if (tasks.isEmpty() || tasks.peek().getParent() != task) {
                    // Task does't schedules any other tasks and does't finishs, need to be scheduled again.
                    tasks.push(task);
                    tasksSuspendBySelfTotal++;
                } else {
                    // The task will be scheduled again by the last
                    // child when all children finished.
                    tasksSuspendByChildrenTotal++;
                    pendingTasks.add(task);
                }
            }
        }
        printSearchInfo();
    }

    @Override
    public void add(Task task) {
        Preconditions.checkNotNull(task);
        final Task parent = task.getParent();
        if (parent != null && !parent.isFinished()) {
            // Only when the task is firstly scheduled need to call this.
            parent.increaseRefByChildren();
        }
        tasks.push(task);
        taskTotal++;
    }

}
