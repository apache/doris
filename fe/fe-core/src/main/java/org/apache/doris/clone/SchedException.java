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

package org.apache.doris.clone;

public class SchedException extends Exception {
    private static final long serialVersionUID = 4233856721704062083L;

    public enum Status {
        SCHEDULE_FAILED, // failed to schedule the tablet, this should only happen in scheduling pending tablets.
        RUNNING_FAILED, // failed to running the clone task, this should only happen in handling running tablets.
        UNRECOVERABLE, // unable to go on, the tablet should be removed from tablet scheduler.
        FINISHED // schedule is done, remove the tablet from tablet scheduler with status FINISHED
    }

    private Status status;

    public SchedException(Status status, String errorMsg) {
        super(errorMsg);
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
