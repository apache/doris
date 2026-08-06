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

package org.apache.doris.statistics;

/**
 * Control-flow signal thrown by an analysis task when it decides to skip
 * statistics collection for a specific column (e.g. a string column contains
 * at least one row whose byte length exceeds
 * {@code Config.statistics_max_string_column_length}).
 *
 * This is NOT an error. The task that catches this exception should mark
 * itself as FINISHED (not FAILED) and surface the skip reason via
 * {@code info.message} / {@code SHOW ANALYZE}.
 */
public class AnalyzeSkipException extends RuntimeException {

    public AnalyzeSkipException(String message) {
        super(message);
    }

    public AnalyzeSkipException(String message, Throwable cause) {
        super(message, cause);
    }
}
