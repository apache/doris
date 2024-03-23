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
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import java.util.List;

public class ExternalTransaction implements Transaction {

    private final ExternalMetadataOps metadataOps;
    private final long txnId;
    private final String dbName;
    private final String tbName;

    public ExternalTransaction(long txnId, ExternalMetadataOps metadataOps, String dbName, String tbName) {
        this.txnId = txnId;
        this.metadataOps = metadataOps;
        this.dbName = dbName;
        this.tbName = tbName;
    }

    @Override
    public void beginInsert() {
        metadataOps.beginInsert(txnId, dbName, tbName);
    }

    @Override
    public <T> void finishInsert(List<T> commitInfos) {
        metadataOps.finishInsert(txnId, commitInfos);
    }

    @Override
    public void commit() throws UserException {
        metadataOps.commit(txnId);
    }

    @Override
    public void rollback() {
        metadataOps.rollback(txnId);
    }
}
