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
 * Compact durable mutation lifecycle:
 * <pre>
 * PENDING -&gt; RUNNING -&gt; COMMITTED
 *                     -&gt; NOT_COMMITTED
 *                     -&gt; UNKNOWN
 * </pre>
 * All three outcomes are terminal. UNKNOWN has no outgoing transition:
 * metadata never changes it to a known outcome, and FORCE_RELEASE (a later
 * delivery slice) only releases the fence/quota/slot without rewriting the
 * outcome.
 */
public enum LanceIndexJobMutationState {
    /** The request and fence are durable; no execute send may have occurred. */
    PENDING,
    /** The durable dispatch boundary has been crossed; the one-shot call may execute or may already have executed. */
    RUNNING,
    /** A complete identity-matched typed success proves this job committed. */
    COMMITTED,
    /** A complete trusted result proves this job did not commit. */
    NOT_COMMITTED,
    /** Doris cannot safely prove whether this job committed. */
    UNKNOWN;

    public boolean isTerminal() {
        return this == COMMITTED || this == NOT_COMMITTED || this == UNKNOWN;
    }
}
