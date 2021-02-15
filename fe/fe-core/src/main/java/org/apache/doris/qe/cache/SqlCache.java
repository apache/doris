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

package org.apache.doris.qe.cache;

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlCache extends Cache {
    private static final Logger LOG = LogManager.getLogger(SqlCache.class);

    public SqlCache(TUniqueId queryId, SelectStmt selectStmt) {
        super(queryId, selectStmt);
    }

    public void setCacheInfo(CacheAnalyzer.CacheTable latestTable) {
        this.latestTable = latestTable;
    }

    public InternalService.PFetchCacheResult getCacheData(Status status) {
        InternalService.PFetchCacheRequest request = InternalService.PFetchCacheRequest.newBuilder()
                .setSqlKey(CacheProxy.getMd5(selectStmt.toSql()))
                .addParams(InternalService.PCacheParam.newBuilder()
                        .setPartitionKey(latestTable.latestPartitionId)
                        .setLastVersion(latestTable.latestVersion)
                        .setLastVersionTime(latestTable.latestTime))
                .build();

        InternalService.PFetchCacheResult cacheResult = proxy.fetchCache(request, 10000, status);
        if (status.ok() && cacheResult != null) {
            cacheResult = cacheResult.toBuilder().setAllCount(1).build();
            MetricRepo.COUNTER_CACHE_HIT_SQL.increase(1L);
            hitRange = HitRange.Full;
        }
        return cacheResult;
    }

    public SelectStmt getRewriteStmt() {
        return null;
    }

    public void copyRowBatch(RowBatch rowBatch) {
        if (rowBatchBuilder == null) {
            rowBatchBuilder = new RowBatchBuilder(CacheAnalyzer.CacheMode.Sql);
        }
        rowBatchBuilder.copyRowData(rowBatch);
    }

    public void updateCache() {
        if (!super.checkRowLimit()) {
            return;
        }

        InternalService.PUpdateCacheRequest updateRequest = rowBatchBuilder.buildSqlUpdateRequest(selectStmt.toSql(),
                latestTable.latestPartitionId, latestTable.latestVersion, latestTable.latestTime);
        if (updateRequest.getValuesCount() > 0) {
            CacheBeProxy proxy = new CacheBeProxy();
            Status status = new Status();
            proxy.updateCache(updateRequest, CacheProxy.UPDATE_TIMEOUT, status);
            int rowCount = 0;
            int dataSize = 0;
            for (InternalService.PCacheValue value : updateRequest.getValuesList()) {
                rowCount += value.getRowsCount();
                dataSize += value.getDataSize();
            }
            LOG.info("update cache model {}, queryid {}, sqlkey {}, value count {}, row count {}, data size {}",
                    CacheAnalyzer.CacheMode.Sql, DebugUtil.printId(queryId), DebugUtil.printId(updateRequest.getSqlKey()),
                    updateRequest.getValuesCount(), rowCount, dataSize);
        }
    }
}
