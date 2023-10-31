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

package org.apache.doris.scheduler.executor;

import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.job.ExecutorResult;
import org.apache.doris.scheduler.job.Job;

/**
 * This interface represents a callback for an event registration. All event registrations
 * must implement this interface to provide an execution method.
 * We will persist JobExecutor in the database, and then execute it when the scheduler starts.
 * We use Gson to serialize and deserialize JobExecutor. so the implementation of JobExecutor needs to be serializable.
 * You can see @org.apache.doris.persist.gson.GsonUtils.java for details.When you implement JobExecutor,pls make sure
 * you can serialize and deserialize it.
 */
@FunctionalInterface
public interface JobExecutor<T, C> {

    /**
     * Executes the event job and returns the result.
     * Exceptions will be caught internally, so there is no need to define or throw them separately.
     *
     * @param job         The event job to execute.
     * @param dataContext The data context of the event job. if you need to pass parameters to the event job,
     *                    you can use it.
     * @return The result of the event job execution.
     */
    ExecutorResult<T> execute(Job job, C dataContext) throws JobException;
}

