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

package org.apache.doris.job.common;

public enum JobStatus {


    /**
     * When the task is not started, the initial state will be triggered.
     * The initial state can be started
     */
    RUNNING,
    /**
     * When the task execution encounters an exception or manually suspends the task,
     * the pause state will be triggered.
     * Pause state can be resumed
     */
    PAUSED,
    /**
     * When the task is manually stopped, the stop state will be triggered.
     * The stop state cannot be resumed
     */
    STOPPED,
    /**
     * When the task is finished, the finished state will be triggered.
     */
    FINISHED
}
