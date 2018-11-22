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

package org.apache.doris.load.routineload;


/**
 * Routine load task info is the task info include the only id (signature).
 * For the kafka type of task info, it also include partitions which will be obtained data in this task.
 * The routine load task info and routine load task are the same thing logically.
 * Differently, routine load task is a agent task include backendId which will execute this task.
 */
public class RoutineLoadTaskInfo {

    private String id;
    private String jobId;

    private long createTimeMs;
    private long loadStartTimeMs;

    public RoutineLoadTaskInfo(String id, String jobId) {
        this.id = id;
        this.jobId = jobId;
        this.createTimeMs = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setLoadStartTimeMs(long loadStartTimeMs) {
        this.loadStartTimeMs = loadStartTimeMs;
    }

    public long getLoadStartTimeMs() {
        return loadStartTimeMs;
    }
}
