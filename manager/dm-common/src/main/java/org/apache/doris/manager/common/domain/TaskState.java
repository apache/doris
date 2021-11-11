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

package org.apache.doris.manager.common.domain;

public enum TaskState {
    // Task has been queued for execution by the driver
    INITIALIZED,
    // Task has been queued for execution by the driver
    QUEUED,
    // Task is currently running
    RUNNING,
    // Task has completed
    FINISHED,
    // Task state is unkown
    UNKNOWN;

    public boolean typeIsRunning() {
        return this == INITIALIZED || this == QUEUED || this == RUNNING;
    }
}
