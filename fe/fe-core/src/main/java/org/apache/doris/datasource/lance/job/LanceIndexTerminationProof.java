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
 * Evidence that releases a possible-live worker slot. Termination proof
 * releases only that slot: it neither changes an UNKNOWN outcome nor releases
 * the same-name fence. Deadlines bound wait/runtime but never prove
 * termination.
 */
public enum LanceIndexTerminationProof {
    /** No proof; the worker may still be running. */
    NONE,
    /** The supervisor reaped the exact matching child process. */
    CHILD_REAPED,
    /** The recorded BE process epoch no longer exists (the BE process was replaced). */
    BE_PROCESS_EPOCH_GONE,
    /**
     * The dispatch is proven never to have been enqueued on the backend — a
     * clean pre-enqueue error status, a client-pool borrow failure before any
     * byte of the call, or an UNKNOWN_METHOD answer from an old backend — so
     * no worker ever existed for it. FE-side only: it never travels on the
     * wire, because a backend cannot prove its own non-enqueue this way.
     */
    NOT_ENQUEUED,
    /**
     * The invocation provably never exec'd the worker program. Two producers
     * keep this distinct from {@code NOT_ENQUEUED}: the backend supervisor,
     * through the termination-report channel, for a post-enqueue rejection
     * before fork or a forked child killed before its barrier release (this is
     * the only proof value that travels on the wire, as value 3); and the FE
     * itself, for a pre-send failure with determined-never-sent evidence local
     * to the FE (a dispatch-payload bound violation after the durable dispatch
     * identity exists), where the backend was never consulted at all.
     */
    NEVER_LAUNCHED
}
