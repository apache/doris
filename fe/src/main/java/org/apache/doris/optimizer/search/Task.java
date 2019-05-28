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
import org.apache.doris.optimizer.OptMemo;

/**
 * Base class for Cascades search task.
 */
public abstract class Task {

    // Parent that schedule this.
    protected final Task parent;
    // The reference count by children.
    private int refCountbyChildren;
    // Used to determine where the current state machine is running
    protected TaskState nextState;

    public Task(Task parent) {
        this.parent = parent;
        this.refCountbyChildren = 0;
    }

    public boolean isFinished() { return nextState == null ; }
    public void setFinished() { nextState = null; }
    public Task getParent() { return parent; }

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

    public boolean execute(SearchContext sContext) {
        while (true) {
            final TaskState lastState = nextState;
            nextState.handle(sContext);
            // In two cases, the current execution will exit.
            // 1. Task state does't change, it indicates that
            // current state machine needs to be suspended.
            // 2. Current State machine has finished.
            if (lastState == nextState || isFinished()) {
                break;
            }
        }
        return isFinished();
    }

    protected void setInitMemoStatus(SearchContext sContext) {
        final OptMemo memo = sContext.getMemo();

    }

    // Base class for StateMacheine's state.
    protected abstract class TaskState {
        public abstract void handle(SearchContext sContext);
    }

    protected static class TaskExecutionInfo {
        private long groupsNumBefore;
        private long groupsNumFinished;
        private long mExprsNumBefore;
        private long mExprsNumFinished;

        public TaskExecutionInfo() {
        }

        public long getGroupsNumBefore() {
            return groupsNumBefore;
        }

        public void setGroupsNumBefore(long groupsNumBefore) {
            this.groupsNumBefore = groupsNumBefore;
        }

        public long getGroupsNumFinished() {
            return groupsNumFinished;
        }

        public void setGroupsNumFinished(long groupsNumFinished) {
            this.groupsNumFinished = groupsNumFinished;
        }

        public long getmExprsNumBefore() {
            return mExprsNumBefore;
        }

        public void setmExprsNumBefore(long mExprsNumBefore) {
            this.mExprsNumBefore = mExprsNumBefore;
        }

        public long getmExprsNumFinished() {
            return mExprsNumFinished;
        }

        public void setmExprsNumFinished(long mExprsNumFinished) {
            this.mExprsNumFinished = mExprsNumFinished;
        }
    }
}
