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

import com.google.common.base.Preconditions;

/**
 * Base class for Cascades search task.
 */
abstract public class TaskStateMachine {

    protected final CTaskType type;
    // Parent that schedule this.
    protected final TaskStateMachine parent;
    // The reference count by child.
    private int refCountbyChildren;
    // Current state, it must be assigned by derived class.
    protected TaskState currentState;


    public TaskStateMachine(CTaskType type, TaskStateMachine parent) {
        this.type = type;
        this.parent = parent;
        this.refCountbyChildren = 0;
    }

    public TaskStateMachine getParent() {
        return parent;
    }

    public void increaseRefByChildren() {
        Preconditions.checkState(refCountbyChildren >= 0);
        refCountbyChildren++;
    }

    public void decreaseRefByChildren() {
        refCountbyChildren--;
        Preconditions.checkState(refCountbyChildren >= 0);
    }

    public boolean isRefByChildren() {
        Preconditions.checkState(refCountbyChildren >= 0);
        return refCountbyChildren != 0;
    }

    public boolean execute(SchedulerContext sContext) {
        if (currentState.isFinished()) {
            return true;
        }

        if (currentState.isSuspending()) {
            currentState.setResuming();
        }

        while (true) {
            currentState.handle(sContext);

            if (currentState.isFinished()) {
                // State Machine finished.
                break;
            }

            if (currentState.isSuspending()) {
                // State Machine suspend.
                break;
            }
        }

        return currentState.isFinished();
    }

    public boolean isFinished() {
        return currentState.isFinished();
    };

    public enum CTaskType {
        GroupOptimization,
        MultiExpressionOptimization,
        GroupImplementation,
        MultiExpressionImplementation,
        RuleApplication
    }
}
