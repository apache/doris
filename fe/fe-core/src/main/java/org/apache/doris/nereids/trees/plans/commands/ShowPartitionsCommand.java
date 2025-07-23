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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.partition.Partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * show partitions command
 */
public class ShowPartitionsCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionId").add("PartitionName")
            .add("VisibleVersion").add("VisibleVersionTime")
            .add("State").add("PartitionKey").add("Range").add("DistributionKey")
            .add("Buckets").add("ReplicationNum").add("StorageMedium").add("CooldownTime").add("RemoteStoragePolicy")
            .add("LastConsistencyCheckTime").add("DataSize").add("IsInMemory").add("ReplicaAllocation")
            .add("IsMutable").add("SyncWithBaseTables").add("UnsyncTables").add("CommittedVersion")
            .add("RowCount")
            .build();
    public static final String FILTER_PARTITION_NAME = "PartitionName";
    private static final Logger LOG = LogManager.getLogger(ShowPartitionsCommand.class);
    private static final String FILTER_PARTITION_ID = "PartitionId";
    private static final String FILTER_STATE = "State";
    private static final String FILTER_BUCKETS = "Buckets";
    private static final String FILTER_REPLICATION_NUM = "ReplicationNum";
    private static final String FILTER_LAST_CONSISTENCY_CHECK_TIME = "LastConsistencyCheckTime";
    private final TableNameInfo tableName;
    private final Expression wildWhere;
    private final long limit;
    private final long offset;
    private final List<OrderKey> orderKeys;
    private boolean isTempPartition;
    private CatalogIf catalog;
    private ProcNodeInterface node;
    private Map<String, Expression> filterMap;
    private ArrayList<OrderByPair> orderByPairs;

    /**
     * constructor for show partitions
     */
    public ShowPartitionsCommand(TableNameInfo tableName, Expression wildWhere,
                                     List<OrderKey> orderKeys, long limit, long offset, boolean isTempPartition) {
        super(PlanType.SHOW_PARTITIONS_COMMAND);
        this.tableName = tableName;
        this.wildWhere = wildWhere;
        if (wildWhere != null) {
            this.filterMap = new HashMap<>();
        }
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
        this.isTempPartition = isTempPartition;
    }

    public CatalogIf getCatalog() {
        return catalog;
    }

    private void analyzeSubExpression(Expression subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        if (subExpr instanceof CompoundPredicate) {
            if (!(subExpr instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            for (Expression child : subExpr.children()) {
                analyzeSubExpression(child);
            }
            return;
        }

        boolean isNotExpr = false;
        if (subExpr instanceof Not) {
            isNotExpr = true;
            subExpr = subExpr.child(0);
            if (!(subExpr instanceof EqualTo)) {
                throw new AnalysisException("Only operator =|>=|<=|>|<|!=|like are supported.");
            }
        }

        if (!(subExpr.child(0) instanceof UnboundSlot)) {
            throw new AnalysisException("Only allow column in filter");
        }
        String leftKey = ((UnboundSlot) subExpr.child(0)).getName();
        if (catalog instanceof HMSExternalCatalog && !leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME)) {
            throw new AnalysisException(String.format("Only %s column supported in where clause for this catalog",
                    FILTER_PARTITION_NAME));
        }

        // FILTER_LAST_CONSISTENCY_CHECK_TIME != 'abc'
        if (subExpr instanceof ComparisonPredicate) {
            if (leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME) || leftKey.equalsIgnoreCase(FILTER_STATE)) {
                if (!(subExpr instanceof EqualTo)) {
                    throw new AnalysisException(String.format("Only operator =|like are supported for %s", leftKey));
                }
            } else if (leftKey.equalsIgnoreCase(FILTER_LAST_CONSISTENCY_CHECK_TIME)) {
                if (!(subExpr.child(1) instanceof StringLikeLiteral)) {
                    throw new AnalysisException("Where clause : LastConsistencyCheckTime =|>=|<=|>|<|!= "
                        + "\"2019-12-22|2019-12-22 22:22:00\"");
                }
                Expression left = subExpr.child(0);
                Expression right = subExpr.child(1).castTo(Config.enable_date_conversion
                        ? DateTimeV2Type.MAX : DateTimeType.INSTANCE);
                subExpr.withChildren(left, right);
            } else if (!leftKey.equalsIgnoreCase(FILTER_PARTITION_ID) && !leftKey.equalsIgnoreCase(FILTER_BUCKETS)
                    && !leftKey.equalsIgnoreCase(FILTER_REPLICATION_NUM)) {
                throw new AnalysisException("Only the columns of PartitionId/PartitionName/"
                    + "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
            }
        } else if (subExpr instanceof Like) {
            if (!leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME) && !leftKey.equalsIgnoreCase(FILTER_STATE)) {
                throw new AnalysisException("Where clause : PartitionName|State like \"p20191012|NORMAL\"");
            }
        } else {
            throw new AnalysisException("Only operator =|>=|<=|>|<|!=|like are supported.");
        }

        filterMap.put(leftKey.toLowerCase(), isNotExpr ? new Not(subExpr) : subExpr);
    }

    protected void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(ctx.getDatabase());
        }
        tableName.analyze(ctx);

        catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
        }

        // disallow unsupported catalog
        if (!(catalog.isInternalCatalog() || catalog instanceof HMSExternalCatalog
                || catalog instanceof MaxComputeExternalCatalog || catalog instanceof IcebergExternalCatalog
                || catalog instanceof PaimonExternalCatalog)) {
            throw new AnalysisException(String.format("Catalog of type '%s' is not allowed in ShowPartitionsCommand",
                    catalog.getType()));
        }

        // where
        if (wildWhere != null) {
            analyzeSubExpression(wildWhere);
        }

        // order by
        if (orderKeys != null && !orderKeys.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderKey orderKey : orderKeys) {
                if (!(orderKey.getExpr() instanceof UnboundSlot)) {
                    throw new AnalysisException("Should order by column");
                }
                UnboundSlot slot = (UnboundSlot) orderKey.getExpr();
                String colName = slot.getName();
                if (catalog instanceof HMSExternalCatalog && !colName.equalsIgnoreCase(FILTER_PARTITION_NAME)) {
                    throw new AnalysisException("External table only support Order By on PartitionName");
                }

                // analyze column
                int index = -1;
                for (String title : TITLE_NAMES) {
                    if (title.equalsIgnoreCase(colName)) {
                        index = TITLE_NAMES.indexOf(title);
                    }
                }
                if (index == -1) {
                    throw new AnalysisException("Title name[" + colName + "] does not exist");
                }
                OrderByPair orderByPair = new OrderByPair(index, !orderKey.isAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    protected void analyze() throws UserException {
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), catalog.getName(), dbName,
                tblName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW PARTITIONS",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), dbName + ": " + tblName);
        }

        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        TableIf table = db.getTableOrMetaException(tblName, TableType.OLAP,
                TableType.HMS_EXTERNAL_TABLE, TableType.MAX_COMPUTE_EXTERNAL_TABLE,
                TableType.ICEBERG_EXTERNAL_TABLE, TableType.PAIMON_EXTERNAL_TABLE);

        if (table instanceof HMSExternalTable) {
            if (((HMSExternalTable) table).isView()) {
                throw new AnalysisException("Table " + tblName + " is not a partitioned table");
            }
            if (CollectionUtils.isEmpty(((HMSExternalTable) table).getPartitionColumns())) {
                throw new AnalysisException("Table " + tblName + " is not a partitioned table");
            }
            return;
        }

        if (table instanceof MaxComputeExternalTable) {
            if (((MaxComputeExternalTable) table).getOdpsTable().getPartitions().isEmpty()) {
                throw new AnalysisException("Table " + tblName + " is not a partitioned table");
            }
            return;
        }

        if (table instanceof IcebergExternalTable) {
            if (!((IcebergExternalTable) table).isValidRelatedTable()) {
                throw new AnalysisException("Table " + tblName + " is not a supported partition table");
            }
            return;
        }

        if (table instanceof PaimonExternalTable) {
            if (((PaimonExternalTable) table).isPartitionInvalid(Optional.empty())) {
                throw new AnalysisException("Table " + tblName + " is not a partitioned table");
            }
            return;
        }

        table.readLock();
        try {
            // build proc path
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("/dbs/");
            stringBuilder.append(db.getId());
            stringBuilder.append("/").append(table.getId());
            if (isTempPartition) {
                stringBuilder.append("/temp_partitions");
            } else {
                stringBuilder.append("/partitions");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("process SHOW PROC '{}';", stringBuilder.toString());
            }

            node = ProcService.getInstance().open(stringBuilder.toString());
        } finally {
            table.readUnlock();
        }
    }

    private ShowResultSet handleShowMaxComputeTablePartitions() {
        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) (catalog);
        List<List<String>> rows = new ArrayList<>();
        String dbName = ClusterNamespace.getNameFromFullName(tableName.getDb());
        List<String> partitionNames;
        if (limit < 0) {
            partitionNames = mcCatalog.listPartitionNames(dbName, tableName.getTbl());
        } else {
            partitionNames = mcCatalog.listPartitionNames(dbName, tableName.getTbl(), offset, limit);
        }
        for (String partition : partitionNames) {
            List<String> list = new ArrayList<>();
            list.add(partition);
            rows.add(list);
        }
        // sort by partition name
        rows.sort(Comparator.comparing(x -> x.get(0)));
        return new ShowResultSet(getMetaData(), rows);
    }

    private ShowResultSet handleShowIcebergTablePartitions() {
        IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
        String db = ClusterNamespace.getNameFromFullName(tableName.getDb());
        String tbl = tableName.getTbl();
        IcebergExternalTable icebergTable = (IcebergExternalTable) icebergCatalog.getDb(db).get().getTable(tbl).get();

        Map<String, PartitionItem> partitions = icebergTable.getAndCopyPartitionItems(Optional.empty());
        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<String, PartitionItem> entry : partitions.entrySet()) {
            List<String> row = new ArrayList<>();
            Range<PartitionKey> items = entry.getValue().getItems();
            row.add(entry.getKey());
            row.add(items.lowerEndpoint().toString());
            row.add(items.upperEndpoint().toString());
            rows.add(row);
        }
        // sort by partition name
        if (orderByPairs != null && orderByPairs.get(0).isDesc()) {
            rows.sort(Comparator.comparing(x -> x.get(0), Comparator.reverseOrder()));
        } else {
            rows.sort(Comparator.comparing(x -> x.get(0)));
        }

        rows = applyLimit(limit, offset, rows);

        return new ShowResultSet(getMetaData(), rows);
    }

    private ShowResultSet handleShowPaimonTablePartitions() throws AnalysisException {
        PaimonExternalCatalog paimonCatalog = (PaimonExternalCatalog) catalog;
        String db = ClusterNamespace.getNameFromFullName(tableName.getDb());
        String tbl = tableName.getTbl();

        PaimonExternalDatabase database = (PaimonExternalDatabase) paimonCatalog.getDb(db)
                .orElseThrow(() -> new AnalysisException("Paimon database '" + db + "' does not exist"));
        PaimonExternalTable paimonTable = database.getTable(tbl)
                .orElseThrow(() -> new AnalysisException("Paimon table '" + db + "." + tbl + "' does not exist"));

        Map<String, Partition> partitionSnapshot = paimonTable.getPartitionSnapshot(Optional.empty());
        if (partitionSnapshot == null) {
            partitionSnapshot = Collections.emptyMap();
        }

        LinkedHashSet<String> partitionColumnNames = paimonTable
                .getPartitionColumns(Optional.empty())
                .stream()
                .map(Column::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        String partitionColumnsStr = String.join(",", partitionColumnNames);

        List<List<String>> rows = partitionSnapshot
                .entrySet()
                .stream()
                .map(entry -> {
                    List<String> row = new ArrayList<>(5);
                    row.add(entry.getKey());
                    row.add(partitionColumnsStr);
                    row.add(String.valueOf(entry.getValue().recordCount()));
                    row.add(String.valueOf(entry.getValue().fileSizeInBytes()));
                    row.add(String.valueOf(entry.getValue().fileCount()));
                    return row;
                }).collect(Collectors.toList());
        // sort by partition name
        if (orderByPairs != null && orderByPairs.get(0).isDesc()) {
            rows.sort(Comparator.comparing(x -> x.get(0), Comparator.reverseOrder()));
        } else {
            rows.sort(Comparator.comparing(x -> x.get(0)));
        }

        rows = applyLimit(limit, offset, rows);

        return new ShowResultSet(getMetaData(), rows);
    }

    private ShowResultSet handleShowHMSTablePartitions() throws AnalysisException {
        HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) catalog;
        List<List<String>> rows = new ArrayList<>();
        String dbName = ClusterNamespace.getNameFromFullName(tableName.getDb());
        List<String> partitionNames;

        // catalog.getClient().listPartitionNames() returned string is the encoded string.
        // example: insert into tmp partition(pt="1=3/3") values( xxx );
        //          show partitions from tmp: pt=1%3D3%2F3
        // Need to consider whether to call `HiveUtil.toPartitionColNameAndValues` method
        ExternalTable dorisTable = hmsCatalog.getDbOrAnalysisException(dbName)
                .getTableOrAnalysisException(tableName.getTbl());

        if (limit >= 0 && offset == 0 && (orderByPairs == null || !orderByPairs.get(0).isDesc())) {
            partitionNames = hmsCatalog.getClient()
                    .listPartitionNames(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(), limit);
        } else {
            partitionNames = hmsCatalog.getClient()
                    .listPartitionNames(dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
        }

        /* Filter add rows */
        for (String partition : partitionNames) {
            List<String> list = new ArrayList<>();

            if (filterMap != null && !filterMap.isEmpty()) {
                if (!PartitionsProcDir.filterExpression(FILTER_PARTITION_NAME, partition, filterMap)) {
                    continue;
                }
            }
            list.add(partition);
            rows.add(list);
        }

        // sort by partition name
        if (orderByPairs != null && orderByPairs.get(0).isDesc()) {
            rows.sort(Comparator.comparing(x -> x.get(0), Comparator.reverseOrder()));
        } else {
            rows.sort(Comparator.comparing(x -> x.get(0)));
        }

        rows = applyLimit(limit, offset, rows);

        return new ShowResultSet(getMetaData(), rows);
    }

    protected ShowResultSet handleShowPartitions(ConnectContext ctx, StmtExecutor executor) throws UserException {
        // validate the where clause
        validate(ctx);

        // analyze catalog
        analyze();

        // get partition info
        if (catalog.isInternalCatalog()) {
            Preconditions.checkNotNull(node);
            LimitElement limitElement = null;
            if (limit > 0) {
                limitElement = new LimitElement(offset == -1L ? 0 : offset, limit);
            }
            List<List<String>> rows = ((PartitionsProcDir) node).fetchResultByExpressionFilter(filterMap,
                    orderByPairs, limitElement).getRows();
            return new ShowResultSet(getMetaData(), rows);
        } else if (catalog instanceof MaxComputeExternalCatalog) {
            return handleShowMaxComputeTablePartitions();
        } else if (catalog instanceof IcebergExternalCatalog) {
            return handleShowIcebergTablePartitions();
        } else if (catalog instanceof PaimonExternalCatalog) {
            return handleShowPaimonTablePartitions();
        } else {
            return handleShowHMSTablePartitions();
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (catalog.isInternalCatalog()) {
            ProcResult result = null;
            try {
                result = node.fetchResult();
            } catch (AnalysisException e) {
                return builder.build();
            }

            for (String col : result.getColumnNames()) {
                builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
            }
        } else if (catalog instanceof IcebergExternalCatalog) {
            builder.addColumn(new Column("Partition", ScalarType.createVarchar(60)));
            builder.addColumn(new Column("Lower Bound", ScalarType.createVarchar(100)));
            builder.addColumn(new Column("Upper Bound", ScalarType.createVarchar(100)));
        } else if (catalog instanceof PaimonExternalCatalog) {
            builder.addColumn(new Column("Partition", ScalarType.createVarchar(300)))
                    .addColumn(new Column("PartitionKey", ScalarType.createVarchar(300)))
                    .addColumn(new Column("RecordCount", ScalarType.createVarchar(300)))
                    .addColumn(new Column("FileSizeInBytes", ScalarType.createVarchar(300)))
                    .addColumn(new Column("FileCount", ScalarType.createVarchar(300)));

        } else {
            builder.addColumn(new Column("Partition", ScalarType.createVarchar(60)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowPartitions(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowPartitionsCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
