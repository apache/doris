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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;

import java.util.List;
import java.util.Map;

/**
 * HiveAcidTransaction is used to maintain the information of a transaction table during the query process.
 * <p>
 * Each HiveAcidTransaction is bound to a specific scanNode during the plan process.
 * A plan may contain multiple HiveAcidTransaction, such as join.
 */
public class HiveAcidTransaction {
    private final TUniqueId queryId;
    private final long hmsTxnId;
    private final String user;
    private final HMSExternalTable hiveTable;

    private Map<String, String> txnValidIds = null;

    public HiveAcidTransaction(TUniqueId queryId, long hmsTxnId, String user, HMSExternalTable hiveTable) {
        this.queryId = queryId;
        this.hmsTxnId = hmsTxnId;
        this.user = user;
        this.hiveTable = hiveTable;
    }

    public Map<String, String> getValidWriteIds(HMSCachedClient client, List<String> partitionNames) {
        if (txnValidIds == null) {
            TableName tableName = new TableName(hiveTable.getCatalog().getName(), hiveTable.getDbName(),
                    hiveTable.getName());
            client.acquireSharedLock(DebugUtil.printId(queryId), hmsTxnId, user, tableName, partitionNames, 5000);
            txnValidIds = client.getValidWriteIds(tableName.getDb() + "." + tableName.getTbl(), hmsTxnId);
        }
        return txnValidIds;
    }
}
