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

package org.apache.doris.load;

public enum ExportJobState {

    /**
     * the initial state of export job.
     */
    PENDING,

    /**
     * When the export job is waiting to be schedule.
     */
    IN_QUEUE,

    /**
     * When the export job is exporting, the EXPORTING state will be triggered.
     */
    EXPORTING,

    /**
     * When the export job is finished, the FINISHED state will be triggered.
     */
    FINISHED,

    /**
     * When the export job is cancelled, the CANCELLED state will be triggered.
     */
    CANCELLED,
}
