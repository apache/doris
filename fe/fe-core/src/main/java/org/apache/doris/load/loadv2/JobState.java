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

package org.apache.doris.load.loadv2;

// JobState will be persisted in meta data by name, so the order of these state is not important
public enum JobState {
    UNKNOWN, // this is only for ISSUE #2354
    PENDING, // init state
    ETL,     // load data partition, sort and aggregation with etl cluster
    LOADING, // job is running
    COMMITTED, // transaction is committed but not visible
    FINISHED, // transaction is visible and job is finished
    CANCELLED, // transaction is aborted and job is cancelled
    RETRY;

    public boolean isFinalState() {
        return this == FINISHED || this == CANCELLED;
    }
}
