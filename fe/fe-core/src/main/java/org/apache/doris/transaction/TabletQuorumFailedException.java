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

package org.apache.doris.transaction;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import java.util.Set;

public class TabletQuorumFailedException extends TransactionException {

    private static final String TABLET_QUORUM_FAILED_MSG = "Failed to commit txn %s. "
            + "Tablet [%s] success replica num %s is less than quorum "
            + "replica num %s while error backends %s";

    private long tabletId;
    private Set<Long> errorBackendIdsForTablet = Sets.newHashSet();

    public TabletQuorumFailedException(long transactionId, long tabletId,
                                       int successReplicaNum, int quorumReplicaNum,
                                       Set<Long> errorBackendIdsForTablet) {
        super(String.format(TABLET_QUORUM_FAILED_MSG, transactionId, tabletId,
                            successReplicaNum, quorumReplicaNum,
                            Joiner.on(",").join(errorBackendIdsForTablet)),
              transactionId);
        this.tabletId = tabletId;
        this.errorBackendIdsForTablet = errorBackendIdsForTablet;
    }
}
