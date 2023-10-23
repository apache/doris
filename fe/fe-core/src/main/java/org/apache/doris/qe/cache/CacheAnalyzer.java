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
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    private Set<String> allViewStmtSet;

    public Cache getCache() {
        return cache;
    }

    public CacheAnalyzer(ConnectContext context, StatementBase parsedStmt, Planner planner) {
        this.context = context;
        this.queryId = context.queryId();
        this.parsedStmt = parsedStmt;
        scanNodes = planner.getScanNodes();
        latestTable = new CacheTable();
        allViewStmtSet = new HashSet<>();
        checkCacheConfig();
    }

    //for unit test
    public CacheAnalyzer(ConnectContext context, StatementBase parsedStmt, List<ScanNode> scanNodes) {
        this.context = context;
        this.parsedStmt = parsedStmt;
        this.scanNodes = scanNodes;
        allViewStmtSet = new HashSet<>();
        checkCacheConfig();
    }

    private void checkCacheConfig() {
        if (Config.cache_enable_sql_mode) {
            if (context.getSessionVariable().isEnableSqlCache()) {
                enableSqlCache = true;
            }
        }
        if (Config.cache_enable_partition_mode) {
            if (context.getSessionVariable().isEnablePartitionCache()) {
                enablePartitionCache = true;
            }
        }
    }

    public CacheMode getCacheMode() {
        return cacheMode;
    }

    public class CacheTable implements Comparable<CacheTable> {
        public TableIf table;
        public long latestPartitionId;
        public long latestVersion;
        public long latestTime;
        public long partitionNum;
        public long sumOfPartitionNum;

        public CacheTable() {
            table = null;
            latestPartitionId = 0;
            latestVersion = 0;
            latestTime = 0;
            partitionNum = 0;
            sumOfPartitionNum = 0;
        }

        @Override
        public int compareTo(CacheTable table) {
            return Long.compare(table.latestTime, this.latestTime);
        }

        public void debug() {
            LOG.debug("table {}, partition id {}, ver {}, time {}, partition num {}, sumOfPartitionNum: {}",
                    table.getName(), latestPartitionId, latestVersion, latestTime, partitionNum, sumOfPartitionNum);
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
            LOG.debug("cache is disabled. queryid {}", DebugUtil.printId(queryId));
            return CacheMode.NoNeed;
        }
        if (!(parsedStmt instanceof SelectStmt) || scanNodes.size() == 0) {
            LOG.debug("not a select stmt or no scan node. queryid {}", DebugUtil.printId(queryId));
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
        String allViewExpandStmtListStr = StringUtils.join(allViewStmtSet, "|");

        if (now == 0) {
            now = nowtime();
        }
        if (enableSqlCache()
                && (now - latestTable.latestTime) >= Config.cache_last_version_interval_second * 1000L) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query cache time:{},{},{}", now, latestTable.latestTime,
                        Config.cache_last_version_interval_second * 1000);
            }
            cache = new SqlCache(this.queryId, this.selectStmt);
            ((SqlCache) cache).setCacheInfo(this.latestTable, allViewExpandStmtListStr);
            MetricRepo.COUNTER_CACHE_ADDED_SQL.increase(1L);
            return CacheMode.Sql;
        }

        // TODO:wxy support partition cache for hive table later
        if (!(latestTable.table instanceof OlapTable)) {
            LOG.debug("only support partition cache for olap table now. queryid {}", DebugUtil.printId(queryId));
            return CacheMode.None;
        }
        if (!enablePartitionCache()) {
            LOG.debug("partition query cache is disabled. queryid {}", DebugUtil.printId(queryId));
            return CacheMode.None;
        }

        //Check if selectStmt matches partition key
        //Only one table can be updated in Config.cache_last_version_interval_second range
        for (int i = 1; i < tblTimeList.size(); i++) {
            if ((now - tblTimeList.get(i).latestTime) < Config.cache_last_version_interval_second * 1000L) {
                LOG.debug("the time of other tables is newer than {} s, queryid {}",
                        Config.cache_last_version_interval_second, DebugUtil.printId(queryId));
                return CacheMode.None;
            }
        }
        OlapTable olapTable = (OlapTable) latestTable.table;
        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
            LOG.debug("the partition of OlapTable not RANGE type, queryid {}", DebugUtil.printId(queryId));
            return CacheMode.None;
        }
        partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        List<Column> columns = partitionInfo.getPartitionColumns();
        //Partition key has only one column
        if (columns.size() != 1) {
            LOG.debug("more than one partition column {}, queryid {}", columns.size(),
                    DebugUtil.printId(queryId));
            return CacheMode.None;
        }
        partColumn = columns.get(0);
        //Check if group expr contain partition column
        if (!checkGroupByPartitionKey(this.selectStmt, partColumn)) {
            LOG.debug("group by columns does not contains all partition column, queryid {}",
                    DebugUtil.printId(queryId));
            return CacheMode.None;
        }
        //Check if whereClause have one CompoundPredicate of partition column
        List<CompoundPredicate> compoundPredicates = Lists.newArrayList();
        getPartitionKeyFromSelectStmt(this.selectStmt, partColumn, compoundPredicates);
        if (compoundPredicates.size() != 1) {
            LOG.debug("empty or more than one predicates contain partition column, queryid {}",
                    DebugUtil.printId(queryId));
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
                && (now - latestTable.latestTime) >= Config.cache_last_version_interval_second * 1000L) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query cache time:{},{},{}", now, latestTable.latestTime,
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
        if (!(parsedStmt instanceof LogicalPlanAdapter) || scanNodes.size() == 0) {
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
        latestTable.sumOfPartitionNum = tblTimeList.stream().mapToLong(item -> item.partitionNum).sum();
        latestTable.debug();

        if (((LogicalPlanAdapter) parsedStmt).getStatementContext().getParsedStatement().isExplain()) {
            return CacheMode.NoNeed;
        }

        allViewStmtSet.addAll(((LogicalPlanAdapter) parsedStmt).getViewDdlSqls());
        String allViewExpandStmtListStr = StringUtils.join(allViewStmtSet, "|");

        if (now == 0) {
            now = nowtime();
        }

        if (enableSqlCache()
                && (now - latestTable.latestTime) >= Config.cache_last_version_interval_second * 1000L) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query cache time :{},{},{}", now, latestTable.latestTime,
                        Config.cache_last_version_interval_second * 1000);
            }
            cache = new SqlCache(this.queryId, ((LogicalPlanAdapter) parsedStmt).getStatementContext()
                    .getOriginStatement().originStmt);
            ((SqlCache) cache).setCacheInfo(this.latestTable, allViewExpandStmtListStr);
            MetricRepo.COUNTER_CACHE_ADDED_SQL.increase(1L);
            return CacheMode.Sql;
        }
        return CacheMode.None;
    }

    private List<CacheTable> buildCacheTableList() {
        //Check the last version time of the table
        MetricRepo.COUNTER_QUERY_TABLE.increase(1L);
        long olapScanNodeSize = scanNodes.stream().filter(node -> node instanceof OlapScanNode).count();
        long hiveScanNodeSize = scanNodes.stream().filter(node -> node instanceof HiveScanNode).count();
        if (olapScanNodeSize > 0) {
            MetricRepo.COUNTER_QUERY_OLAP_TABLE.increase(1L);
        }
        if (hiveScanNodeSize > 0) {
            MetricRepo.COUNTER_QUERY_HIVE_TABLE.increase(1L);
        }

        if (!(olapScanNodeSize == scanNodes.size() || hiveScanNodeSize == scanNodes.size())) {
            LOG.debug("only support olap/hive table with non-federated query, other types are not supported now, "
                    + "queryId {}", DebugUtil.printId(queryId));
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
                LOG.debug("more than one partition scanned when qeury has agg, partition cache cannot use, queryid {}",
                        DebugUtil.printId(queryId));
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
            LOG.debug("hit cache, mode {}, queryid {}, all count {}, value count {}, row count {}, data size {}",
                    cacheMode, DebugUtil.printId(queryId),
                    cacheResult.getAllCount(), cacheResult.getValuesCount(),
                    rowCount, dataSize);
        } else {
            LOG.debug("miss cache, mode {}, queryid {}, code {}, msg {}", cacheMode,
                    DebugUtil.printId(queryId), status.getErrorCode(), status.getErrorMsg());
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
        for (Long partitionId : node.getSelectedPartitionIds()) {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition.getVisibleVersionTime() >= cacheTable.latestTime) {
                cacheTable.latestPartitionId = partition.getId();
                cacheTable.latestTime = partition.getVisibleVersionTime();
                cacheTable.latestVersion = partition.getVisibleVersion();
            }
        }
        return cacheTable;
    }

    private CacheTable buildCacheTableForHiveScanNode(HiveScanNode node) {
        CacheTable cacheTable = new CacheTable();
        cacheTable.table = node.getTargetTable();
        cacheTable.partitionNum = node.getReadPartitionNum();
        cacheTable.latestTime = cacheTable.table.getUpdateTime();
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
}

