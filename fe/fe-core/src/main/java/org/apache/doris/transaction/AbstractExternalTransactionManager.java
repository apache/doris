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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractExternalTransactionManager<T extends Transaction> implements TransactionManager {
    private static final Logger LOG = LogManager.getLogger(AbstractExternalTransactionManager.class);
    private final Map<Long, T> transactions = new ConcurrentHashMap<>();
    protected final ExternalMetadataOps ops;

    public AbstractExternalTransactionManager(ExternalMetadataOps ops) {
        this.ops = ops;
    }

    abstract T createTransaction();

    @Override
    public long begin() {
        long id = Env.getCurrentEnv().getNextId();
        T transaction = createTransaction();
        transactions.put(id, transaction);
        Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().putTxnById(id, transaction);
        return id;
    }

    @Override
    public void commit(long id) throws UserException {
        getTransactionWithException(id).commit();
        transactions.remove(id);
        Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().removeTxnById(id);
    }

    @Override
    public void rollback(long id) {
        try {
            getTransactionWithException(id).rollback();
        } catch (TransactionNotFoundException e) {
            LOG.warn(e.getMessage(), e);
        } finally {
            transactions.remove(id);
            Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().removeTxnById(id);
        }
    }

    @Override
    public Transaction getTransaction(long id) throws UserException {
        return getTransactionWithException(id);
    }

    private Transaction getTransactionWithException(long id) throws TransactionNotFoundException {
        Transaction txn = transactions.get(id);
        if (txn == null) {
            throw new TransactionNotFoundException("Can't find transaction for " + id);
        }
        return txn;
    }
}
