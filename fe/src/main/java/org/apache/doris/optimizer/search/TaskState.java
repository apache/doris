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

/**
 * Base class for task StateMacheine's state.
 */
public abstract class TaskState {

    // The followings are all events that StateMachine can use.

    // Current TaskStateMachine is suspended and wait to be resumed
    // by child TaskStateMachine.
    private boolean isSuspending;
    // TaskStateMachine has been finished.
    private boolean isFinished;
    // TaskStateMachine resume from Suspending.
    private boolean isResuming;
    // TaskStateMachine is running.
    private boolean isRunning;

    public TaskState() {
        resetState();
    }

    public abstract void handle(SchedulerContext sContext);

    public boolean isSuspending() { return isSuspending; }
    public boolean isFinished() { return isFinished; }
    public boolean isResuming() { return isResuming; }
    public boolean isRunning() {
        return isRunning;
    }

    public void setSuspending() {
        isSuspending = true;
        isRunning = false;
        isResuming = false;
        isFinished = false;
    }

    public void setRunning() {
        isSuspending = false;
        isRunning = true;
        isResuming = false;
        isFinished = false;
    }

    public void setResuming() {
        isSuspending = false;
        isRunning = true;
        isResuming = true;
        isFinished = false;
    }

    public void setFinished() {
        isSuspending = false;
        isRunning = false;
        isResuming = false;
        isFinished = true;
    }

    public void resetState() {
        this.isSuspending = false;
        this.isFinished = false;
        this.isResuming = false;
        this.isRunning = false;
    }
}
