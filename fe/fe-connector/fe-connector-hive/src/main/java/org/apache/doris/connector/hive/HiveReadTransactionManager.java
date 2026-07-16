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

package org.apache.doris.connector.hive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the read transaction of each query against transactional (ACID) Hive tables. For each query a
 * {@link HiveReadTransaction} is registered (which opens the metastore transaction); when the query
 * finishes it is deregistered (which commits the transaction, releasing the shared read lock).
 *
 * <p>Plugin-side port of fe-core {@code HiveTransactionMgr}. It is <b>plugin-owned and dormant</b> until
 * the read cutover: today transactional-hive reads still flow through fe-core
 * {@code HiveScanNode}/{@code Env.getCurrentHiveTransactionMgr()}. At cutover the query-finish trigger
 * (fe-core {@code QueryFinishCallbackRegistry}) is wired to {@link #deregister} and the legacy
 * {@code Env} manager is removed. Until then nothing invokes this manager on a live path.</p>
 */
public class HiveReadTransactionManager {
    private static final Logger LOG = LogManager.getLogger(HiveReadTransactionManager.class);

    private final Map<String, HiveReadTransaction> txnMap = new ConcurrentHashMap<>();

    /**
     * Opens the transaction (see {@link HiveReadTransaction#begin}) and registers it under its query id, so
     * the query-finish path can find and commit it. Mirrors fe-core: one {@code HiveReadTransaction} is
     * created per transactional-table scan (each pins its own table's write-id snapshot), keyed by query id.
     */
    public void register(HiveReadTransaction txn) {
        txn.begin();
        txnMap.put(txn.getQueryId(), txn);
    }

    public void deregister(String queryId) {
        HiveReadTransaction txn = txnMap.remove(queryId);
        if (txn != null) {
            try {
                txn.commit();
            } catch (RuntimeException e) {
                LOG.warn("failed to commit hive read txn: " + queryId, e);
            }
        }
    }
}
