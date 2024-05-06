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
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlCache extends Cache {
    private static final Logger LOG = LogManager.getLogger(SqlCache.class);

    private String originSql;
    private PUniqueId cacheMd5;

    public SqlCache(TUniqueId queryId, SelectStmt selectStmt) {
        super(queryId, selectStmt);
    }

    // For SetOperationStmt and Nereids
    public SqlCache(TUniqueId queryId, String originSql) {
        super(queryId);
        this.originSql = originSql;
    }

    public void setCacheInfo(CacheAnalyzer.CacheTable latestTable, String allViewExpandStmtListStr) {
        this.latestTable = latestTable;
        this.allViewExpandStmtListStr = allViewExpandStmtListStr;
        this.cacheMd5 = null;
    }

    public PUniqueId getOrComputeCacheMd5() {
        if (cacheMd5 == null) {
            cacheMd5 = CacheProxy.getMd5(getSqlWithViewStmt());
        }
        return cacheMd5;
    }

    public void setCacheMd5(PUniqueId cacheMd5) {
        this.cacheMd5 = cacheMd5;
    }

    public String getSqlWithViewStmt() {
        String originSql = selectStmt != null ? selectStmt.toSql() : this.originSql;
        String cacheKey = originSql + "|" + allViewExpandStmtListStr;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cache key: {}", cacheKey);
        }
        return cacheKey;
    }

    public long getLatestId() {
        return latestTable.latestPartitionId;
    }

    public long getLatestTime() {
        return latestTable.latestPartitionTime;
    }

    public long getLatestVersion() {
        return latestTable.latestPartitionVersion;
    }

    public long getSumOfPartitionNum() {
        return latestTable.sumOfPartitionNum;
    }

    public static Backend findCacheBe(PUniqueId cacheMd5) {
        return CacheCoordinator.getInstance().findBackend(cacheMd5);
    }

    public static InternalService.PFetchCacheResult getCacheData(CacheProxy proxy,
            PUniqueId cacheKeyMd5, long latestPartitionId, long latestPartitionVersion,
            long latestPartitionTime, long sumOfPartitionNum, Status status) {
        InternalService.PFetchCacheRequest request = InternalService.PFetchCacheRequest.newBuilder()
                .setSqlKey(cacheKeyMd5)
                .addParams(InternalService.PCacheParam.newBuilder()
                        .setPartitionKey(latestPartitionId)
                        .setLastVersion(latestPartitionVersion)
                        .setLastVersionTime(latestPartitionTime)
                        .setPartitionNum(sumOfPartitionNum))
                .build();

        InternalService.PFetchCacheResult cacheResult = proxy.fetchCache(request, 10000, status);
        if (status.ok() && cacheResult != null && cacheResult.getStatus() == InternalService.PCacheStatus.CACHE_OK) {
            cacheResult = cacheResult.toBuilder().setAllCount(1).build();
        }
        return cacheResult;
    }

    public InternalService.PFetchCacheResult getCacheData(Status status) {
        InternalService.PFetchCacheResult cacheResult = getCacheData(proxy, getOrComputeCacheMd5(),
                latestTable.latestPartitionId, latestTable.latestPartitionVersion,
                latestTable.latestPartitionTime, latestTable.sumOfPartitionNum, status);
        if (status.ok() && cacheResult != null && cacheResult.getStatus() == InternalService.PCacheStatus.CACHE_OK) {
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
        if (!super.checkRowLimit()) {
            return;
        }
        rowBatchBuilder.copyRowData(rowBatch);
    }

    public void updateCache() {
        if (!super.checkRowLimit()) {
            return;
        }

        PUniqueId cacheKeyMd5 = getOrComputeCacheMd5();
        InternalService.PUpdateCacheRequest updateRequest =
                rowBatchBuilder.buildSqlUpdateRequest(cacheKeyMd5,
                        latestTable.latestPartitionId,
                        latestTable.latestPartitionVersion,
                        latestTable.latestPartitionTime,
                        latestTable.sumOfPartitionNum
                );
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
                    CacheAnalyzer.CacheMode.Sql, DebugUtil.printId(queryId),
                    DebugUtil.printId(updateRequest.getSqlKey()),
                    updateRequest.getValuesCount(), rowCount, dataSize);
        }
    }
}
