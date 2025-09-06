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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.SqlCacheContext.ScanTable;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Analyze which caching mode a SQL is suitable for
 * 1. T + 1 update is suitable for SQL mode
 * 2. Partition by date, update the data of the day in near real time, which is suitable for Partition mode
 */
public class CacheAnalyzer {
    private static final Logger LOG = LogManager.getLogger(CacheAnalyzer.class);

    /**
     * NoNeed : disable config or variable, not query, not scan table etc.
     */
    public enum CacheMode {
        NoNeed,
        None,
        TTL,
        Sql,
        Partition
    }

    private ConnectContext context;
    private boolean enableSqlCache = false;
    private boolean enablePartitionCache = false;
    private TUniqueId queryId;
    private CacheMode cacheMode;
    private CacheTable latestTable;
    private StatementBase parsedStmt;
    private List<ScanNode> scanNodes;
    private RangePartitionInfo partitionInfo;
    private Column partColumn;
    private CompoundPredicate partitionPredicate;
    private Cache cache;
    private final Set<String> allViewStmtSet;
    private String allViewExpandStmtListStr;
    private Planner planner;
    private List<ScanTable> scanTables = Lists.newArrayList();

    public Cache getCache() {
        return cache;
    }

    public CacheAnalyzer(ConnectContext context, StatementBase parsedStmt, Planner planner) {
        this.context = context;
        this.queryId = context.queryId();
        this.parsedStmt = parsedStmt;
        this.scanNodes = planner.getScanNodes();
        this.latestTable = new CacheTable();
        this.allViewStmtSet = new HashSet<>();
        this.planner = planner;
        checkCacheConfig();
    }

    //for unit test
    public CacheAnalyzer(ConnectContext context, StatementBase parsedStmt, List<ScanNode> scanNodes) {
        this.context = context;
        this.parsedStmt = parsedStmt;
        this.scanNodes = scanNodes;
        this.allViewStmtSet = new HashSet<>();
        checkCacheConfig();
    }

    private void checkCacheConfig() {
        if (Config.cache_enable_sql_mode) {
            if (context.getSessionVariable().isEnableSqlCache()) {
                enableSqlCache = true;
            }
        }
        // alread remove the entrance of partition cache, so we force set to false
        enablePartitionCache = false;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public CacheMode getCacheMode() {
        return cacheMode;
    }

    public Planner getPlanner() {
        return planner;
    }

    public class CacheTable implements Comparable<CacheTable> {
        public TableIf table;
        public long latestPartitionId;
        public long latestPartitionVersion;
        public long latestPartitionTime;
        public long partitionNum;
        public long sumOfPartitionNum;

        public CacheTable() {
            table = null;
            latestPartitionId = 0;
            latestPartitionVersion = 0;
            latestPartitionTime = 0;
            partitionNum = 0;
            sumOfPartitionNum = 0;
        }

        @Override
        public int compareTo(CacheTable table) {
            return Long.compare(table.latestPartitionTime, this.latestPartitionTime);
        }

        public void debug() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("table {}, partition id {}, ver {}, time {},"
                                + "partition num {}, sumOfPartitionNum: {}",
                        table.getName(), latestPartitionId, latestPartitionVersion, latestPartitionTime,
                        partitionNum, sumOfPartitionNum);
            }
        }
    }

    public boolean enableCache() {
        return enableSqlCache || enablePartitionCache;
    }

    public boolean enableSqlCache() {
        return enableSqlCache;
    }

    public boolean enablePartitionCache() {
        return enablePartitionCache;
    }

    public static boolean canUseCache(SessionVariable sessionVariable) {
        return (sessionVariable.isEnableSqlCache()) && commonCacheCondition(sessionVariable);
    }

    public static boolean canUseSqlCache(SessionVariable sessionVariable) {
        return sessionVariable.isEnableSqlCache() && commonCacheCondition(sessionVariable);
    }

    public static boolean commonCacheCondition(SessionVariable sessionVariable) {
        return sessionVariable.getSqlSelectLimit() < 0 && sessionVariable.getDefaultOrderByLimit() < 0
                && !sessionVariable.dryRunQuery;
    }

    public void checkCacheModeForNereids(long now) {
        cacheMode = innerCheckCacheModeForNereids(now);
    }


    private CacheMode innerCheckCacheModeForNereids(long now) {
        // only sql cache
        if (!enableSqlCache()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("sql cache is disabled. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.NoNeed;
        }
        if (!(parsedStmt instanceof LogicalPlanAdapter) || scanNodes.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not a select stmt or no scan node. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.NoNeed;
        }

        //Check the last version time of the table
        List<CacheTable> tblTimeList = buildCacheTableList();
        if (CollectionUtils.isEmpty(tblTimeList)) {
            return CacheMode.None;
        }
        latestTable = tblTimeList.get(0);
        long sumOfPartitionNum = 0;
        for (CacheTable cacheTable : tblTimeList) {
            sumOfPartitionNum += cacheTable.partitionNum;
        }
        latestTable.sumOfPartitionNum = sumOfPartitionNum;
        latestTable.debug();

        if (((LogicalPlanAdapter) parsedStmt).getStatementContext().getParsedStatement().isExplain()) {
            return CacheMode.NoNeed;
        }

        boolean isNewAllViewExpandStmtListStr = allViewExpandStmtListStr == null;
        if (isNewAllViewExpandStmtListStr) {
            allViewStmtSet.addAll(((LogicalPlanAdapter) parsedStmt).getViewDdlSqls());
            allViewExpandStmtListStr = StringUtils.join(allViewStmtSet, "|");
        }

        if (now == 0) {
            now = nowtime();
        }

        if (enableSqlCache()
                && (now - latestTable.latestPartitionTime) >= Config.cache_last_version_interval_second * 1000L) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query cache time :{},{},{}", now, latestTable.latestPartitionTime,
                        Config.cache_last_version_interval_second * 1000);
            }

            String originStmt = ((LogicalPlanAdapter) parsedStmt).getStatementContext()
                    .getOriginStatement().originStmt;
            cache = new SqlCache(this.queryId, originStmt);
            SqlCache sqlCache = (SqlCache) cache;
            PUniqueId existsMd5 = null;
            if (planner instanceof NereidsPlanner) {
                NereidsPlanner nereidsPlanner = (NereidsPlanner) planner;
                Optional<SqlCacheContext> sqlCacheContext = nereidsPlanner
                        .getCascadesContext()
                        .getStatementContext()
                        .getSqlCacheContext();
                if (sqlCacheContext.isPresent()) {
                    existsMd5 = sqlCacheContext.get().getOrComputeCacheKeyMd5();
                }
            }

            sqlCache.setCacheInfo(this.latestTable, allViewExpandStmtListStr);
            sqlCache.setCacheMd5(existsMd5);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_SQL_CACHE_ADDED.increase(1L);
            }
            return CacheMode.Sql;
        }
        return CacheMode.None;
    }

    private List<CacheTable> buildCacheTableList() {
        try {
            // Check the last version time of the table
            MetricRepo.COUNTER_QUERY_TABLE.increase(1L);
            long olapScanNodeSize = 0;
            long hiveScanNodeSize = 0;
            for (ScanNode scanNode : scanNodes) {
                if (scanNode instanceof OlapScanNode) {
                    olapScanNodeSize++;
                } else if (scanNode instanceof HiveScanNode) {
                    hiveScanNodeSize++;
                }
            }
            if (olapScanNodeSize > 0) {
                MetricRepo.COUNTER_QUERY_OLAP_TABLE.increase(1L);
            }
            if (hiveScanNodeSize > 0) {
                MetricRepo.COUNTER_QUERY_HIVE_TABLE.increase(1L);
            }

            if (!(olapScanNodeSize == scanNodes.size() || hiveScanNodeSize == scanNodes.size())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("only support olap/hive table with non-federated query, "
                            + "other types are not supported now, queryId {}", DebugUtil.printId(queryId));
                }
                return Collections.emptyList();
            }

            List<CacheTable> tblTimeList = Lists.newArrayList();
            for (int i = 0; i < scanNodes.size(); i++) {
                ScanNode node = scanNodes.get(i);
                CacheTable cTable = node instanceof OlapScanNode
                        ? buildCacheTableForOlapScanNode((OlapScanNode) node)
                        : buildCacheTableForHiveScanNode((HiveScanNode) node);
                tblTimeList.add(cTable);
            }
            Collections.sort(tblTimeList);
            return tblTimeList;
        } catch (Throwable t) {
            return new ArrayList<>();
        }
    }

    public InternalService.PFetchCacheResult getCacheData() throws UserException {
        try {
            if (parsedStmt instanceof LogicalPlanAdapter) {
                cacheMode = innerCheckCacheModeForNereids(0);
            } else {
                return null;
            }
        } catch (NullPointerException e) {
            LOG.error("getCacheData error", e);
            return null;
        }

        if (cacheMode == CacheMode.NoNeed) {
            return null;
        }
        if (cacheMode == CacheMode.None) {
            return null;
        }
        Status status = new Status();
        InternalService.PFetchCacheResult cacheResult = cache.getCacheData(status);
        if (status.ok() && cacheResult != null && cacheResult.getStatus() == InternalService.PCacheStatus.CACHE_OK) {
            int rowCount = 0;
            int dataSize = 0;
            for (InternalService.PCacheValue value : cacheResult.getValuesList()) {
                rowCount += value.getRowsCount();
                dataSize += value.getDataSize();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("hit cache, mode {}, queryid {}, all count {}, value count {}, row count {}, data size {}",
                        cacheMode, DebugUtil.printId(queryId),
                        cacheResult.getAllCount(), cacheResult.getValuesCount(),
                        rowCount, dataSize);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("miss cache, mode {}, queryid {}, code {}, msg {}", cacheMode,
                        DebugUtil.printId(queryId), status.getErrorCode(), status.getErrorMsg());
            }
            cacheResult = null;
        }
        return cacheResult;
    }

    public long nowtime() {
        return System.currentTimeMillis();
    }

    /**
     * Only support case 1
     * 1.key >= a and key <= b
     * 2.key = a or key = b
     * 3.key in(a,b,c)
     */
    private void getPartitionKeyFromWhereClause(Expr expr, Column partColumn,
                                                List<CompoundPredicate> compoundPredicates) {
        if (expr == null) {
            return;
        }
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) expr;
            if (cp.getOp() == CompoundPredicate.Operator.AND) {
                if (cp.getChildren().size() == 2 && cp.getChild(0) instanceof BinaryPredicate
                        && cp.getChild(1) instanceof BinaryPredicate) {
                    BinaryPredicate leftPre = (BinaryPredicate) cp.getChild(0);
                    BinaryPredicate rightPre = (BinaryPredicate) cp.getChild(1);
                    String leftColumn = getColumnName(leftPre);
                    String rightColumn = getColumnName(rightPre);
                    if (leftColumn.equalsIgnoreCase(partColumn.getName())
                            && rightColumn.equalsIgnoreCase(partColumn.getName())) {
                        compoundPredicates.add(cp);
                    }
                }
            }
            for (Expr subExpr : expr.getChildren()) {
                getPartitionKeyFromWhereClause(subExpr, partColumn, compoundPredicates);
            }
        }
    }

    private String getColumnName(BinaryPredicate predicate) {
        SlotRef slot = null;
        if (predicate.getChild(0) instanceof SlotRef) {
            slot = (SlotRef) predicate.getChild(0);
        } else if (predicate.getChild(0) instanceof CastExpr) {
            CastExpr expr = (CastExpr) predicate.getChild(0);
            if (expr.getChild(0) instanceof SlotRef) {
                slot = (SlotRef) expr.getChild(0);
            }
        }

        if (slot != null) {
            return slot.getColumnName();
        }
        return "";
    }

    private CacheTable buildCacheTableForOlapScanNode(OlapScanNode node) {
        CacheTable cacheTable = new CacheTable();
        OlapTable olapTable = node.getOlapTable();
        cacheTable.partitionNum = node.getSelectedPartitionIds().size();
        cacheTable.table = olapTable;

        DatabaseIf database = olapTable.getDatabase();
        CatalogIf catalog = database.getCatalog();
        ScanTable scanTable = new ScanTable(
                new FullTableName(catalog.getName(), database.getFullName(), olapTable.getName()));
        scanTables.add(scanTable);

        Collection<Long> partitionIds = node.getSelectedPartitionIds();
        try {
            olapTable.getVersionInBatchForCloudMode(partitionIds);
        } catch (RpcException e) {
            LOG.warn("Failed to get version in batch for cloud mode, partitions {}.", partitionIds, e);
        }

        for (Long partitionId : node.getSelectedPartitionIds()) {
            Partition partition = olapTable.getPartition(partitionId);
            scanTable.addScanPartition(partitionId);
            if (partition.getVisibleVersionTime() >= cacheTable.latestPartitionTime) {
                cacheTable.latestPartitionId = partition.getId();
                cacheTable.latestPartitionTime = partition.getVisibleVersionTime();
                cacheTable.latestPartitionVersion = partition.getCachedVisibleVersion();
            }
        }
        return cacheTable;
    }

    private CacheTable buildCacheTableForHiveScanNode(HiveScanNode node) {
        CacheTable cacheTable = new CacheTable();
        cacheTable.table = node.getTargetTable();
        cacheTable.partitionNum = node.getSelectedPartitionNum();
        cacheTable.latestPartitionTime = cacheTable.table.getUpdateTime();
        TableIf tableIf = cacheTable.table;
        DatabaseIf database = tableIf.getDatabase();
        CatalogIf catalog = database.getCatalog();
        ScanTable scanTable = new ScanTable(new FullTableName(
                catalog.getName(), database.getFullName(), tableIf.getName()));
        scanTables.add(scanTable);
        return cacheTable;
    }

    public Cache.HitRange getHitRange() {
        if (cacheMode == CacheMode.None) {
            return Cache.HitRange.None;
        }
        return cache.getHitRange();
    }

    public void copyRowBatch(RowBatch rowBatch) {
        if (cacheMode == CacheMode.None || cacheMode == CacheMode.NoNeed) {
            return;
        }
        cache.copyRowBatch(rowBatch);
    }

    public void updateCache() {
        if (cacheMode == CacheMode.None || cacheMode == CacheMode.NoNeed) {
            return;
        }
        cache.updateCache();
    }

    public List<ScanTable> getScanTables() {
        return scanTables;
    }

    public CacheTable getLatestTable() {
        return latestTable;
    }

    public boolean isEqualViewString(List<TableIf> views) {
        Set<String> viewSet = Sets.newHashSet();
        for (TableIf view : views) {
            if (view instanceof View) {
                viewSet.add(((View) view).getInlineViewDef());
            } else if (view instanceof HMSExternalTable) {
                viewSet.add(((HMSExternalTable) view).getViewText());
            } else {
                return false;
            }
        }

        return StringUtils.equals(allViewExpandStmtListStr, StringUtils.join(viewSet, "|"));
    }
}
