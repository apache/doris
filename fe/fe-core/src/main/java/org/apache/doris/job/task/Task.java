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

package org.apache.doris.job.task;

import org.apache.doris.job.exception.JobException;
import org.apache.doris.thrift.TRow;

/**
 * The Task interface represents a task that can be executed and managed by a scheduler.
 * All extension tasks must implement this interface.
 * The methods defined in this interface are automatically called by the scheduler before and after the execution
 * of the run method.
 */
public interface Task {

    /**
     * This method is called before the task is executed.
     * Implementations can use this method to perform any necessary setup or initialization.
     */
    void before() throws JobException;

    /**
     * This method contains the main logic of the task.
     * Implementations should define the specific actions to be performed by the task.
     */
    void run() throws JobException;

    /**
     * This method is called when the task fails to execute successfully.
     * Implementations can use this method to handle any failure scenarios.
     */
    void onFail() throws JobException;

    /**
     * This method is called when the task fails to execute successfully, with an additional error message.
     * Implementations can use this method to handle any failure scenarios and provide a custom error message.
     *
     * @param msg The error message associated with the failure.
     */
    void onFail(String msg) throws JobException;

    /**
     * This method is called when the task executes successfully.
     * Implementations can use this method to handle successful execution scenarios.
     */
    void onSuccess() throws JobException;

    /**
     * This method is called to cancel the execution of the task.
     * Implementations should define the necessary steps to cancel the task.
     *
     * @param needWaitCancelComplete Do we need to wait for the cancellation to be completed.
     */
    void cancel(boolean needWaitCancelComplete) throws JobException;

    /**
     * get info for tvf `tasks`
     * @return TRow
     */
    TRow getTvfInfo(String jobName);
}
