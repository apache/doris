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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.UserException;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * HiveTransactionMgr is used to manage hive transaction.
 * For each query, it will register a HiveTransaction.
 * When query is finished, it will deregister the HiveTransaction.
 */
public class HiveTransactionMgr {
    private static final Logger LOG = LogManager.getLogger(HiveTransactionMgr.class);
    private Map<String, HiveTransaction> txnMap = Maps.newConcurrentMap();

    public HiveTransactionMgr() {
    }

    public void register(HiveTransaction txn) throws UserException {
        txn.begin();
        txnMap.put(txn.getQueryId(), txn);
    }

    public void deregister(String queryId) {
        HiveTransaction txn = txnMap.remove(queryId);
        if (txn != null) {
            try {
                txn.commit();
            } catch (UserException e) {
                LOG.warn("failed to commit hive txn: " + queryId, e);
            }
        }
    }
}
