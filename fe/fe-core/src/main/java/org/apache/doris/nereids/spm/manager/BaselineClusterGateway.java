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

/** Cluster operations used by baseline management, separated from persistence for testing. */
public interface BaselineClusterGateway {

    /**
     * Whether this FE currently owns the SPM write epoch: the manager refuses a durable
     * write from a demoted FE (the new master allocates ids from the same watermark).
     */
    default boolean isLeader() {
        return true;
    }

    /**
     * Synchronizes the local FE journal with the master when implemented by the live gateway. Internal-table row
     * visibility is confirmed separately by baseline persistence reads. The default implementation is a no-op.
     */
    default void syncJournalWithMaster(ConnectContext ctx) {
    }
}
