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

package org.apache.doris.mtmv.ivm;

/**
 * The stage of a refresh task of an IVM MV that a failure happened in, which is the reason a failed task reports
 * when no {@link IvmFailureReason} names it.
 *
 * <p>An {@link IvmFailureReason} names the failures of the incremental refresh itself, which a request that may
 * not fall back fails with. Every other failure is told apart by where it was thrown rather than by its message,
 * so that the reasons stay a fixed set.
 */
public enum IvmRefreshFailureStage {
    /** The MV's query could not be analyzed against its base tables, or can no longer be used. */
    QUERY_UNUSABLE,
    /** The MV's partitions could not be synchronized with the partitions of its base tables. */
    PARTITION_SYNC_FAILED,
    /** A refresh attempt failed while it ran. */
    EXECUTION_FAILED,
    /** Any other stage: resolving the request, deciding the attempts, or after every attempt gave up. */
    OTHER
}
