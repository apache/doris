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

/**
 * A functional interface for executing a memory task.
 */
public interface TransientTaskExecutor {

    /**
     * Executes the memory task.
     * Exceptions will be caught internally, so there is no need to define or throw them separately.
     */
    void execute() throws JobException;

    /**
     * Cancel the memory task.
     */
    void cancel() throws JobException;

    Long getId();
}
