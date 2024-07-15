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
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.datasource.iceberg.IcebergTransaction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IcebergTransactionManager implements TransactionManager {

    private final Map<Long, IcebergTransaction> transactions = new ConcurrentHashMap<>();
    private final IcebergMetadataOps ops;

    public IcebergTransactionManager(IcebergMetadataOps ops) {
        this.ops = ops;
    }

    @Override
    public long begin() {
        long id = Env.getCurrentEnv().getNextId();
        IcebergTransaction icebergTransaction = new IcebergTransaction(ops);
        transactions.put(id, icebergTransaction);
        return id;
    }

    @Override
    public void commit(long id) throws UserException {
        getTransactionWithException(id).commit();
        transactions.remove(id);
    }

    @Override
    public void rollback(long id) {
        try {
            getTransactionWithException(id).rollback();
        } finally {
            transactions.remove(id);
        }
    }

    @Override
    public IcebergTransaction getTransaction(long id) {
        return getTransactionWithException(id);
    }

    public IcebergTransaction getTransactionWithException(long id) {
        IcebergTransaction icebergTransaction = transactions.get(id);
        if (icebergTransaction == null) {
            throw new RuntimeException("Can't find transaction for " + id);
        }
        return icebergTransaction;
    }
}
