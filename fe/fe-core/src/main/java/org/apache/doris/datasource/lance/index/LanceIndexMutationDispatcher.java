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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;

/**
 * The dispatch seam of the synchronous Lance index mutation path: one request handed to one
 * backend, one blocking answer. This is the only network edge of the path and the seam a later
 * slice evolves into submit-and-callback without touching the executor's contract.
 *
 * <p>Contract of every implementation:
 *
 * <ul>
 * <li><b>Single send.</b> One invocation id is sent at most once, to exactly the backend passed
 *     in. No implementation retries on any failure - a transport error, a timeout, or a non-OK
 *     answer all terminate the invocation. Retrying is structurally impossible to make safe
 *     here (a resend can commit twice or replace an index the first send created), so the
 *     executor relies on this seam never producing a second send.
 * <li><b>Budget-bounded wait.</b> {@code timeoutMillis} is the caller's remaining statement
 *     budget and must bound the whole call. The production implementation applies it as the
 *     pooled connection's socket timeout.
 * <li><b>Interrupt awareness, best-effort.</b> The blocking thrift read cannot be interrupted:
 *     a cancelled statement cannot unblock an in-flight call, and cancelling never rolls back
 *     a possibly-committed mutation. The executor honors its cancel signal only at phase
 *     boundaries before the dispatch; after the dispatch the timeout is the only way the call
 *     returns.
 * <li><b>Raw failures.</b> Every failure propagates unwrapped to the classifier
 *     ({@link LanceIndexMutationExecutor}), which decides the outcome; message text never
 *     participates in classification.
 * </ul>
 */
public interface LanceIndexMutationDispatcher {

    /**
     * Sends one mutation request to {@code backend} and blocks for its answer, bounded by
     * {@code timeoutMillis}. Returns the backend's complete answer (whose own status decides
     * the classification), or throws whatever transport or protocol failure occurred.
     */
    TLanceIndexMutationResult dispatch(TLanceIndexMutationRequest request, Backend backend,
            long timeoutMillis) throws Exception;
}
