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

package org.apache.doris.datasource.lance.job;

/**
 * Independent metadata-refresh state of a durable Lance index job. Stored and
 * transitioned independently from the mutation state: refresh success/failure
 * only sets DONE/FAILED and never changes the mutation outcome.
 * <pre>
 * NOT_REQUIRED | REQUIRED -&gt; RUNNING -&gt; DONE | FAILED, FAILED -&gt; RUNNING (retry)
 * </pre>
 * Refresh replays through the existing idempotent external-table refresh path;
 * a failed refresh may be retried and still holds the same-name fence.
 */
public enum LanceIndexJobRefreshState {
    /** No refresh is owed (e.g. a proven pre-invocation failure with no relevant metadata change). */
    NOT_REQUIRED,
    /** A refresh of the authoritative metadata is owed before the fence may be released. */
    REQUIRED,
    /** A refresh attempt is in flight. */
    RUNNING,
    /** The required refresh finished; the fence may be released. */
    DONE,
    /** The refresh attempt failed; retry through the idempotent path is allowed. */
    FAILED
}
