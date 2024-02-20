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

import org.apache.doris.analysis.TableName;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ValidWriteIdList;

import java.util.List;

/**
 * HiveTransaction is used to save info of a hive transaction.
 * Used when reading hive transactional table.
 * Each HiveTransaction is bound to a query.
 */
public class HiveTransaction {
    private final String queryId;
    private final String user;
    private final HMSExternalTable hiveTable;

    private final boolean isFullAcid;

    private long txnId;
    private List<String> partitionNames = Lists.newArrayList();

    ValidWriteIdList validWriteIdList = null;

    public HiveTransaction(String queryId, String user, HMSExternalTable hiveTable, boolean isFullAcid) {
        this.queryId = queryId;
        this.user = user;
        this.hiveTable = hiveTable;
        this.isFullAcid = isFullAcid;
    }

    public String getQueryId() {
        return queryId;
    }

    public void addPartition(String partitionName) {
        this.partitionNames.add(partitionName);
    }

    public boolean isFullAcid() {
        return isFullAcid;
    }

    public ValidWriteIdList getValidWriteIds(HMSCachedClient client) {
        if (validWriteIdList == null) {
            TableName tableName = new TableName(hiveTable.getCatalog().getName(), hiveTable.getDbName(),
                    hiveTable.getName());
            client.acquireSharedLock(queryId, txnId, user, tableName, partitionNames, 5000);
            validWriteIdList = client.getValidWriteIds(tableName.getDb() + "." + tableName.getTbl(), txnId);
        }
        return validWriteIdList;
    }

    public void begin() throws UserException {
        try {
            this.txnId = ((HMSExternalCatalog) hiveTable.getCatalog()).getClient().openTxn(user);
        } catch (RuntimeException e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    public void commit() throws UserException {
        try {
            ((HMSExternalCatalog) hiveTable.getCatalog()).getClient().commitTxn(txnId);
        } catch (RuntimeException e) {
            throw new UserException(e.getMessage(), e);
        }
    }
}
