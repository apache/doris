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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.qe.ConnectContext;

/**
 * The cluster-level collaboration of {@link BaselineManager}: the leadership fence of
 * every durable write and the journal synchronization a follower performs after a
 * forwarded GLOBAL DDL.
 *
 * Substituted implementations default to LEADER and a no-op sync (a store-level test
 * double drives its own protocol); the live implementation consults
 * {@code Env#getCurrentEnv} and the master before reporting.
 */
public interface BaselineClusterGateway {

    /**
     * Whether this FE currently owns the SPM write epoch: the manager refuses a durable
     * write from a demoted FE (the new master allocates ids from the same watermark).
     */
    default boolean isLeader() {
        return true;
    }

    /**
     * Synchronizes this FE's metadata with the master (the same strong-consistency
     * mechanism a forwarded DDL and {@code syncJournalIfNeeded} use), so a local read
     * performed afterwards includes the forwarded GLOBAL DDL the master already
     * completed. A failure surfaces as a retryable error.
     */
    default void syncJournalWithMaster(ConnectContext ctx) {
    }
}
