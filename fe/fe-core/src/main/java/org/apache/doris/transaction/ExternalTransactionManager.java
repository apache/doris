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

import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalTransactionManager {

    private final Map<Long, Transaction> transactions = new ConcurrentHashMap<>();
    private final TransactionIdGenerator idGenerator = new TransactionIdGenerator();

    public Long getNextTransactionId() {
        return idGenerator.getNextTransactionId();
    }

    public void setEditLog(EditLog editLog) {
        this.idGenerator.setEditLog(editLog);
    }

    public void beginTransaction(Long id, Transaction transaction) {
        transactions.put(id, transaction);
    }

    public void beginInsert(Long id) {
        Transaction transaction = transactions.get(id);
        if (transaction == null) {
            throw new RuntimeException("Can't find transaction for " + id);
        }
        transaction.beginInsert();
    }

    public <T> void finishInsert(Long id, List<T> commitInfos) {
        Transaction transaction = transactions.get(id);
        if (transaction == null) {
            throw new RuntimeException("Can't find transaction for " + id);
        }
        transaction.finishInsert(commitInfos);
    }

    public void commit(Long id) throws UserException {
        Transaction transaction = transactions.get(id);
        if (transaction == null) {
            throw new RuntimeException("Can't find transaction for " + id);
        }
        transaction.commit();
        transactions.remove(id);
    }

    public void rollback(Long id) {
        Transaction transaction = transactions.get(id);
        if (transaction == null) {
            throw new RuntimeException("Can't find transaction for " + id);
        }
        transaction.rollback();
        transactions.remove(id);
    }
}
