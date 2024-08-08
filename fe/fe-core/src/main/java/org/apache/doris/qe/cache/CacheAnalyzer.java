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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
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
    private SelectStmt selectStmt;
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

    /**
     * Check cache mode with SQL and table
     * 1、Only Olap table
     * 2、The update time of the table is before Config.last_version_interval_time
     * 2、PartitionType is PartitionType.RANGE, and partition key has only one column
     * 4、Partition key must be included in the group by clause
     * 5、Where clause must contain only one partition key predicate
     * CacheMode.Sql
     * xxx FROM user_profile, updated before Config.last_version_interval_time
     * CacheMode.Partition, partition by event_date, only the partition of today will be updated.
     * SELECT xxx FROM app_event WHERE event_date >= 20191201 AND event_date <= 20191207 GROUP BY event_date
     * SELECT xxx FROM app_event INNER JOIN user_Profile ON app_event.user_id = user_profile.user_id xxx
     * SELECT xxx FROM app_event INNER JOIN user_profile ON xxx INNER JOIN site_channel ON xxx
     */
    public void checkCacheMode(long now) {
        cacheMode = innerCheckCacheMode(now);
    }

    public void checkCacheModeForNereids(long now) {
        cacheMode = innerCheckCacheModeForNereids(now);
    }

    private CacheMode innerCheckCacheMode(long now) {
        if (!enableCache()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cache is disabled. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.NoNeed;
        }
        if (!(parsedStmt instanceof SelectStmt) || scanNodes.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not a select stmt or no scan node. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.NoNeed;
        }
        this.selectStmt = (SelectStmt) parsedStmt;

        List<CacheTable> tblTimeList = buildCacheTableList();
        if (CollectionUtils.isEmpty(tblTimeList)) {
            return CacheMode.None;
        }
        latestTable = tblTimeList.get(0);
        latestTable.sumOfPartitionNum = tblTimeList.stream().mapToLong(item -> item.partitionNum).sum();
        latestTable.debug();

        addAllViewStmt(selectStmt);
        if (allViewExpandStmtListStr == null) {
            allViewExpandStmtListStr = StringUtils.join(allViewStmtSet, "|");
        }

        if (now == 0) {
            now = nowtime();
        }
        if (enableSqlCache()
                && (now - latestTable.latestPartitionTime) >= Config.cache_last_version_interval_second * 1000L) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query cache time:{},{},{}", now, latestTable.latestPartitionTime,
                        Config.cache_last_version_interval_second * 1000);
            }
            cache = new SqlCache(this.queryId, this.selectStmt);
            ((SqlCache) cache).setCacheInfo(this.latestTable, allViewExpandStmtListStr);
            MetricRepo.COUNTER_CACHE_ADDED_SQL.increase(1L);
            return CacheMode.Sql;
        }

        // TODO:wxy support partition cache for hive table later
        if (!(latestTable.table instanceof OlapTable)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("only support partition cache for olap table now. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.None;
        }
        if (!enablePartitionCache()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("partition query cache is disabled. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.None;
        }

        //Check if selectStmt matches partition key
        //Only one table can be updated in Config.cache_last_version_interval_second range
        for (int i = 1; i < tblTimeList.size(); i++) {
            if ((now - tblTimeList.get(i).latestPartitionTime) < Config.cache_last_version_interval_second * 1000L) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("the time of other tables is newer than {} s, queryid {}",
                            Config.cache_last_version_interval_second, DebugUtil.printId(queryId));
                }
                return CacheMode.None;
            }
        }
        OlapTable olapTable = (OlapTable) latestTable.table;
        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("the partition of OlapTable not RANGE type, queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.None;
        }
        partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        List<Column> columns = partitionInfo.getPartitionColumns();
        //Partition key has only one column
        if (columns.size() != 1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("more than one partition column {}, queryid {}", columns.size(),
                        DebugUtil.printId(queryId));
            }
            return CacheMode.None;
        }
        partColumn = columns.get(0);
        //Check if group expr contain partition column
        if (!checkGroupByPartitionKey(this.selectStmt, partColumn)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("group by columns does not contains all partition column, queryid {}",
                        DebugUtil.printId(queryId));
            }
            return CacheMode.None;
        }
        //Check if whereClause have one CompoundPredicate of partition column
        List<CompoundPredicate> compoundPredicates = Lists.newArrayList();
        getPartitionKeyFromSelectStmt(this.selectStmt, partColumn, compoundPredicates);
        if (compoundPredicates.size() != 1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("empty or more than one predicates contain partition column, queryid {}",
                        DebugUtil.printId(queryId));
            }
            return CacheMode.None;
        }
        partitionPredicate = compoundPredicates.get(0);
        cache = new PartitionCache(this.queryId, this.selectStmt);
        ((PartitionCache) cache).setCacheInfo(this.latestTable, this.partitionInfo, this.partColumn,
                this.partitionPredicate, allViewExpandStmtListStr);
        MetricRepo.COUNTER_CACHE_ADDED_PARTITION.increase(1L);
        return CacheMode.Partition;
    }

    private CacheMode innerCheckCacheModeSetOperation(long now) {
        // only sql cache
        if (!enableSqlCache()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("sql cache is disabled. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.NoNeed;
        }
        if (!(parsedStmt instanceof SetOperationStmt) || scanNodes.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not a set operation stmt or no scan node. queryid {}", DebugUtil.printId(queryId));
            }
            return CacheMode.NoNeed;
        }

        //Check the last version time of the table
        List<CacheTable> tblTimeList = buildCacheTableList();
        if (CollectionUtils.isEmpty(tblTimeList)) {
            return CacheMode.None;
        }
        latestTable = tblTimeList.get(0);
        latestTable.sumOfPartitionNum = tblTimeList.stream().mapToLong(item -> item.partitionNum).sum();
        latestTable.debug();

        addAllViewStmt((SetOperationStmt) parsedStmt);
        String allViewExpandStmtListStr = StringUtils.join(allViewStmtSet, "|");

        if (now == 0) {
            now = nowtime();
        }
        if (enableSqlCache()
                && (now - latestTable.latestPartitionTime) >= Config.cache_last_version_interval_second * 1000L) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query cache time:{},{},{}", now, latestTable.latestPartitionTime,
                        Config.cache_last_version_interval_second * 1000);
            }
            cache = new SqlCache(this.queryId, parsedStmt.toSql());
            ((SqlCache) cache).setCacheInfo(this.latestTable, allViewExpandStmtListStr);
            MetricRepo.COUNTER_CACHE_ADDED_SQL.increase(1L);
            return CacheMode.Sql;
        }
        return CacheMode.None;
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
            MetricRepo.COUNTER_CACHE_ADDED_SQL.increase(1L);
            return CacheMode.Sql;
        }
        return CacheMode.None;
    }

    private List<CacheTable> buildCacheTableList() {
        //Check the last version time of the table
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
                LOG.debug("only support olap/hive table with non-federated query, other types are not supported now, "
                        + "queryId {}", DebugUtil.printId(queryId));
            }
            return Collections.emptyList();
        }

        List<CacheTable> tblTimeList = Lists.newArrayList();
        for (int i = 0; i < scanNodes.size(); i++) {
            ScanNode node = scanNodes.get(i);
            if (enablePartitionCache()
                    && (node instanceof OlapScanNode)
                    && ((OlapScanNode) node).getSelectedPartitionNum() > 1
                    && selectStmt != null
                    && selectStmt.hasGroupByClause()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("more than one partition scanned when qeury has agg, "
                                    + "partition cache cannot use, queryid {}",
                            DebugUtil.printId(queryId));
                }
                return Collections.emptyList();
            }
            CacheTable cTable = node instanceof OlapScanNode
                    ? buildCacheTableForOlapScanNode((OlapScanNode) node)
                    : buildCacheTableForHiveScanNode((HiveScanNode) node);
            tblTimeList.add(cTable);
        }
        Collections.sort(tblTimeList);
        return tblTimeList;
    }

    public InternalService.PFetchCacheResult getCacheData() throws UserException {
        if (parsedStmt instanceof LogicalPlanAdapter) {
            cacheMode = innerCheckCacheModeForNereids(0);
        } else if (parsedStmt instanceof SelectStmt) {
            cacheMode = innerCheckCacheMode(0);
        } else if (parsedStmt instanceof SetOperationStmt) {
            cacheMode = innerCheckCacheModeSetOperation(0);
        } else {
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

    private void getPartitionKeyFromSelectStmt(SelectStmt stmt, Column partColumn,
                                               List<CompoundPredicate> compoundPredicates) {
        getPartitionKeyFromWhereClause(stmt.getWhereClause(), partColumn, compoundPredicates);
        List<TableRef> tableRefs = stmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    getPartitionKeyFromSelectStmt((SelectStmt) queryStmt, partColumn, compoundPredicates);
                }
            }
        }
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

    /**
     * Check the selectStmt and tableRefs always group by partition key
     * 1. At least one group by
     * 2. group by must contain partition key
     */
    private boolean checkGroupByPartitionKey(SelectStmt stmt, Column partColumn) {
        List<AggregateInfo> aggInfoList = Lists.newArrayList();
        getAggInfoList(stmt, aggInfoList);
        int groupbyCount = 0;
        for (AggregateInfo aggInfo : aggInfoList) {
            /*
            Support COUNT(DISTINCT xxx) now，next version will remove the code
            if (aggInfo.isDistinctAgg()) {
                return false;
            }*/
            ArrayList<Expr> groupExprs = aggInfo.getGroupingExprs();
            if (groupExprs == null) {
                continue;
            }
            groupbyCount += 1;
            boolean matched = false;
            for (Expr groupExpr : groupExprs) {
                if (!(groupExpr instanceof SlotRef)) {
                    continue;
                }

                SlotRef slot = (SlotRef) groupExpr;
                if (partColumn.getName().equals(slot.getColumnName())) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return groupbyCount > 0 ? true : false;
    }

    private void getAggInfoList(SelectStmt stmt, List<AggregateInfo> aggInfoList) {
        AggregateInfo aggInfo = stmt.getAggInfo();
        if (aggInfo != null) {
            aggInfoList.add(aggInfo);
        }
        List<TableRef> tableRefs = stmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    getAggInfoList((SelectStmt) queryStmt, aggInfoList);
                }
            }
        }
    }

    private CacheTable buildCacheTableForOlapScanNode(OlapScanNode node) {
        CacheTable cacheTable = new CacheTable();
        OlapTable olapTable = node.getOlapTable();
        cacheTable.partitionNum = node.getSelectedPartitionIds().size();
        cacheTable.table = olapTable;

        DatabaseIf database = olapTable.getDatabase();
        CatalogIf catalog = database.getCatalog();
        ScanTable scanTable = new ScanTable(
                new FullTableName(catalog.getName(), database.getFullName(), olapTable.getName()),
                olapTable.getVisibleVersion());
        scanTables.add(scanTable);

        Collection<Long> partitionIds = node.getSelectedPartitionIds();
        olapTable.getVersionInBatchForCloudMode(partitionIds);

        for (Long partitionId : node.getSelectedPartitionIds()) {
            Partition partition = olapTable.getPartition(partitionId);
            scanTable.addScanPartition(partitionId);
            if (partition.getVisibleVersionTime() >= cacheTable.latestPartitionTime) {
                cacheTable.latestPartitionId = partition.getId();
                cacheTable.latestPartitionTime = partition.getVisibleVersionTime();
                cacheTable.latestPartitionVersion = partition.getVisibleVersion(true);
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
                catalog.getName(), database.getFullName(), tableIf.getName()), 0);
        scanTables.add(scanTable);
        return cacheTable;
    }

    private void addAllViewStmt(List<TableRef> tblRefs) {
        for (TableRef tblRef : tblRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                if (inlineViewRef.isLocalView()) {
                    Collection<View> views = inlineViewRef.getAnalyzer().getLocalViews().values();
                    for (View view : views) {
                        addAllViewStmt(view.getQueryStmt());
                    }
                } else {
                    addAllViewStmt(inlineViewRef.getViewStmt());
                    allViewStmtSet.add(inlineViewRef.getView().getInlineViewDef());
                }
                addAllViewStmt(inlineViewRef.getQueryStmt());
            }
        }
    }

    private void addAllViewStmt(QueryStmt queryStmt) {
        if (queryStmt instanceof SelectStmt) {
            addAllViewStmt(((SelectStmt) queryStmt).getTableRefs());
        } else if (queryStmt instanceof SetOperationStmt) {
            for (SetOperationStmt.SetOperand operand : ((SetOperationStmt) queryStmt).getOperands()) {
                addAllViewStmt(operand.getQueryStmt());
            }
        }
    }

    public Cache.HitRange getHitRange() {
        if (cacheMode == CacheMode.None) {
            return Cache.HitRange.None;
        }
        return cache.getHitRange();
    }

    public SelectStmt getRewriteStmt() {
        if (cacheMode != CacheMode.Partition) {
            return null;
        }
        return cache.getRewriteStmt();
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
