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

package org.apache.doris.analysis;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.ExportSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.external.jdbc.JdbcTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Insert into is performed to load data from the result of query stmt.
 * <p>
 * syntax:
 * INSERT INTO table_name [partition_info] [col_list] [plan_hints] query_stmt
 * <p>
 * table_name: is the name of target table
 * partition_info: PARTITION (p1,p2)
 * the partition info of target table
 * col_list: (c1,c2)
 * the column list of target table
 * plan_hints: [STREAMING,SHUFFLE_HINT]
 * The streaming plan is used by both streaming and non-streaming insert stmt.
 * The only difference is that non-streaming will record the load info in LoadManager and return label.
 * User can check the load info by show load stmt.
 */
public class NativeInsertStmt extends InsertStmt {

    private static final Logger LOG = LogManager.getLogger(InsertStmt.class);

    private static final String SHUFFLE_HINT = "SHUFFLE";
    private static final String NOSHUFFLE_HINT = "NOSHUFFLE";

    protected final TableName tblName;
    private final PartitionNames targetPartitionNames;
    // parsed from targetPartitionNames.
    private List<Long> targetPartitionIds;
    protected List<String> targetColumnNames;
    private QueryStmt queryStmt;
    private final List<String> planHints;
    private Boolean isRepartition;

    // set after parse all columns and expr in query statement
    // this result expr in the order of target table's columns
    private final List<Expr> resultExprs = Lists.newArrayList();

    private final Map<String, Expr> exprByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    protected Table targetTable;

    private DatabaseIf db;
    private long transactionId;

    // we need a new TupleDesc for olap table.
    private TupleDescriptor olapTuple;

    private DataSink dataSink;
    private DataPartition dataPartition;

    private final List<Column> targetColumns = Lists.newArrayList();

    /*
     * InsertStmt may be analyzed twice, but transaction must be only begun once.
     * So use a boolean to check if transaction already begun.
     */
    private boolean isTransactionBegin = false;

    private boolean isValuesOrConstantSelect;

    private boolean isPartialUpdate = false;

    private HashSet<String> partialUpdateCols = new HashSet<String>();

    private boolean isFromDeleteOrUpdateStmt = false;

    private InsertType insertType = InsertType.NATIVE_INSERT;

    enum InsertType {
        NATIVE_INSERT("insert_"),
        UPDATE("update_"),
        DELETE("delete_");
        private String labePrefix;

        InsertType(String labePrefix) {
            this.labePrefix = labePrefix;
        }
    }

    public NativeInsertStmt(InsertTarget target, String label, List<String> cols, InsertSource source,
            List<String> hints) {
        super(new LabelName(null, label), null, null);
        this.tblName = target.getTblName();
        this.targetPartitionNames = target.getPartitionNames();
        this.label = new LabelName(null, label);
        this.queryStmt = source.getQueryStmt();
        this.planHints = hints;
        this.targetColumnNames = cols;
        this.isValuesOrConstantSelect = (queryStmt instanceof SelectStmt
                && ((SelectStmt) queryStmt).getTableRefs().isEmpty());
    }

    // Ctor for CreateTableAsSelectStmt and InsertOverwriteTableStmt
    public NativeInsertStmt(TableName name, PartitionNames targetPartitionNames, LabelName label,
            QueryStmt queryStmt, List<String> planHints, List<String> targetColumnNames) {
        super(label, null, null);
        this.tblName = name;
        this.targetPartitionNames = targetPartitionNames;
        this.queryStmt = queryStmt;
        this.planHints = planHints;
        this.targetColumnNames = targetColumnNames;
        this.isValuesOrConstantSelect = (queryStmt instanceof SelectStmt
                && ((SelectStmt) queryStmt).getTableRefs().isEmpty());
    }

    public NativeInsertStmt(InsertTarget target, String label, List<String> cols, InsertSource source,
                            List<String> hints, boolean isPartialUpdate, InsertType insertType) {
        this(target, label, cols, source, hints);
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateCols.addAll(cols);
        this.insertType = insertType;
    }

    public boolean isValuesOrConstantSelect() {
        return isValuesOrConstantSelect;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public long getTransactionId() {
        return this.transactionId;
    }

    public Boolean isRepartition() {
        return isRepartition;
    }

    public String getDbName() {
        return tblName.getDb();
    }

    public String getTbl() {
        return tblName.getTbl();
    }

    public void getTables(Analyzer analyzer, Map<Long, TableIf> tableMap, Set<String> parentViewNameSet)
            throws AnalysisException {
        // get dbs of statement
        queryStmt.getTables(analyzer, false, tableMap, parentViewNameSet);
        tblName.analyze(analyzer);
        // disallow external catalog except JdbcExternalCatalog
        if (analyzer.getEnv().getCurrentCatalog() instanceof ExternalCatalog
                && !(analyzer.getEnv().getCurrentCatalog() instanceof JdbcExternalCatalog)) {
            Util.prohibitExternalCatalog(tblName.getCtl(), this.getClass().getSimpleName());
        }
        String dbName = tblName.getDb();
        String tableName = tblName.getTbl();
        String ctlName = tblName.getCtl();
        // check exist
        DatabaseIf db = analyzer.getEnv().getCatalogMgr().getCatalog(tblName.getCtl()).getDbOrAnalysisException(dbName);
        TableIf table = db.getTableOrAnalysisException(tblName.getTbl());

        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), ctlName, dbName, tableName, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    ctlName + ": " + dbName + ": " + tableName);
        }

        tableMap.put(table.getId(), table);
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    public void setQueryStmt(QueryStmt queryStmt) {
        this.queryStmt = queryStmt;
    }

    public boolean isExplain() {
        return queryStmt.isExplain();
    }

    public String getLabel() {
        return label.getLabelName();
    }

    public DataSink getDataSink() {
        return dataSink;
    }

    public DatabaseIf getDbObj() {
        return db;
    }

    public boolean isTransactionBegin() {
        return isTransactionBegin;
    }

    protected void preCheckAnalyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (targetTable == null) {
            tblName.analyze(analyzer);
            // disallow external catalog except JdbcExternalCatalog
            if (analyzer.getEnv().getCurrentCatalog() instanceof ExternalCatalog
                    && !(analyzer.getEnv().getCurrentCatalog() instanceof JdbcExternalCatalog)) {
                Util.prohibitExternalCatalog(tblName.getCtl(), this.getClass().getSimpleName());
            }
        }

        // Check privilege
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tblName.getCtl(), tblName.getDb(),
                        tblName.getTbl(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tblName.getCtl() + ": " + tblName.getDb() + ": " + tblName.getTbl());
        }

        // check partition
        if (targetPartitionNames != null) {
            targetPartitionNames.analyze(analyzer);
        }
    }

    /**
     * translate load related stmt to`insert into xx select xx from tvf` semantic
     */
    protected void convertSemantic(Analyzer analyzer) throws UserException {
        // do nothing
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        preCheckAnalyze(analyzer);

        convertSemantic(analyzer);

        // set target table and
        analyzeTargetTable(analyzer);

        analyzeSubquery(analyzer);

        analyzePlanHints();

        if (analyzer.getContext().isTxnModel()) {
            return;
        }

        // create data sink
        createDataSink();

        db = analyzer.getEnv().getCatalogMgr().getCatalog(tblName.getCtl()).getDbOrAnalysisException(tblName.getDb());
        // create label and begin transaction
        long timeoutSecond = ConnectContext.get().getExecTimeout();
        if (label == null || Strings.isNullOrEmpty(label.getLabelName())) {
            label = new LabelName(db.getFullName(),
                    insertType.labePrefix + DebugUtil.printId(analyzer.getContext().queryId()).replace("-", "_"));
        }
        if (!isExplain() && !isTransactionBegin) {
            if (targetTable instanceof OlapTable) {
                LoadJobSourceType sourceType = LoadJobSourceType.INSERT_STREAMING;
                transactionId = Env.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                        Lists.newArrayList(targetTable.getId()), label.getLabelName(),
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        sourceType, timeoutSecond);
            }
            isTransactionBegin = true;
        }

        // init data sink
        if (!isExplain() && targetTable instanceof OlapTable) {
            OlapTableSink sink = (OlapTableSink) dataSink;
            TUniqueId loadId = analyzer.getContext().queryId();
            int sendBatchParallelism = analyzer.getContext().getSessionVariable().getSendBatchParallelism();
            boolean isInsertStrict = analyzer.getContext().getSessionVariable().getEnableInsertStrict()
                    && !isFromDeleteOrUpdateStmt;
            sink.init(loadId, transactionId, db.getId(), timeoutSecond, sendBatchParallelism, false, isInsertStrict);
        }
    }

    protected void initTargetTable(Analyzer analyzer) throws AnalysisException {
        if (targetTable == null) {
            DatabaseIf db = analyzer.getEnv().getCatalogMgr()
                    .getCatalog(tblName.getCtl()).getDbOrAnalysisException(tblName.getDb());
            if (db instanceof Database) {
                targetTable = (Table) db.getTableOrAnalysisException(tblName.getTbl());
            } else if (db instanceof JdbcExternalDatabase) {
                JdbcExternalTable jdbcTable = (JdbcExternalTable) db.getTableOrAnalysisException(tblName.getTbl());
                targetTable = jdbcTable.getJdbcTable();
            } else {
                throw new AnalysisException("Not support insert target table.");
            }
        }
    }

    private void analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
        // Get table
        initTargetTable(analyzer);

        if (targetTable instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) targetTable;

            // partition
            if (targetPartitionNames != null) {
                targetPartitionIds = Lists.newArrayList();
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
                }
                for (String partName : targetPartitionNames.getPartitionNames()) {
                    Partition part = olapTable.getPartition(partName, targetPartitionNames.isTemp());
                    if (part == null) {
                        ErrorReport.reportAnalysisException(
                                ErrorCode.ERR_UNKNOWN_PARTITION, partName, targetTable.getName());
                    }
                    targetPartitionIds.add(part.getId());
                }
            }

            if (olapTable.hasSequenceCol() && olapTable.getSequenceMapCol() != null && targetColumnNames != null) {
                Optional<String> foundCol = targetColumnNames.stream()
                            .filter(c -> c.equalsIgnoreCase(olapTable.getSequenceMapCol())).findAny();
                Optional<Column> seqCol = olapTable.getFullSchema().stream()
                                .filter(col -> col.getName().equals(olapTable.getSequenceMapCol()))
                                .findFirst();
                if (seqCol.isPresent() && !foundCol.isPresent() && !isPartialUpdate && !isFromDeleteOrUpdateStmt
                        && !analyzer.getContext().getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
                    if (seqCol.get().getDefaultValue() == null
                                    || !seqCol.get().getDefaultValue().equals(DefaultValue.CURRENT_TIMESTAMP)) {
                        throw new AnalysisException("Table " + olapTable.getName()
                                + " has sequence column, need to specify the sequence column");
                    }
                }
            }

            if (isPartialUpdate && olapTable.hasSequenceCol() && olapTable.getSequenceMapCol() != null
                    && partialUpdateCols.contains(olapTable.getSequenceMapCol())) {
                partialUpdateCols.add(Column.SEQUENCE_COL);
            }
            // need a descriptor
            DescriptorTable descTable = analyzer.getDescTbl();
            olapTuple = descTable.createTupleDescriptor();
            for (Column col : olapTable.getFullSchema()) {
                if (isPartialUpdate && !partialUpdateCols.contains(col.getName())) {
                    continue;
                }
                SlotDescriptor slotDesc = descTable.addSlotDescriptor(olapTuple);
                slotDesc.setIsMaterialized(true);
                slotDesc.setType(col.getType());
                slotDesc.setColumn(col);
                slotDesc.setIsNullable(col.isAllowNull());
            }
        } else if (targetTable instanceof MysqlTable || targetTable instanceof OdbcTable
                || targetTable instanceof JdbcTable) {
            if (targetPartitionNames != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
            }
        } else if (targetTable instanceof BrokerTable) {
            if (targetPartitionNames != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
            }

            BrokerTable brokerTable = (BrokerTable) targetTable;
            if (!brokerTable.isWritable()) {
                throw new AnalysisException("table " + brokerTable.getName()
                        + "is not writable. path should be an dir");
            }

        } else {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_NON_INSERTABLE_TABLE, targetTable.getName(), targetTable.getType());
        }
    }

    private void checkColumnCoverage(Set<String> mentionedCols, List<Column> baseColumns)
            throws AnalysisException {

        // check columns of target table
        for (Column col : baseColumns) {
            if (col.isAutoInc()) {
                continue;
            }
            if (isPartialUpdate && !partialUpdateCols.contains(col.getName())) {
                continue;
            }
            if (mentionedCols.contains(col.getName())) {
                continue;
            }
            if (col.getDefaultValue() == null && !col.isAllowNull()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COL_NOT_MENTIONED, col.getName());
            }
        }
    }

    private void analyzeSubquery(Analyzer analyzer) throws UserException {
        // Analyze columns mentioned in the statement.
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<String> realTargetColumnNames;
        if (targetColumnNames == null) {
            if (!isFromDeleteOrUpdateStmt
                    && analyzer.getContext().getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
                throw new AnalysisException("You must explicitly specify the columns to be updated when "
                        + "updating partial columns using the INSERT statement.");
            }
            // the mentioned columns are columns which are visible to user, so here we use
            // getBaseSchema(), not getFullSchema()
            for (Column col : targetTable.getBaseSchema(false)) {
                mentionedColumns.add(col.getName());
                targetColumns.add(col);
            }
        } else {
            for (String colName : targetColumnNames) {
                Column col = targetTable.getColumn(colName);
                if (col == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName, targetTable.getName());
                }
                if (!mentionedColumns.add(colName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_FIELD_SPECIFIED_TWICE, colName);
                }
                targetColumns.add(col);
            }
            // hll column must in mentionedColumns
            for (Column col : targetTable.getBaseSchema()) {
                if (col.getType().isObjectStored() && !mentionedColumns.contains(col.getName())) {
                    throw new AnalysisException(
                            "object-stored column " + col.getName() + " must in insert into columns");
                }
            }
        }

        /*
         * When doing schema change, there may be some shadow columns. we should add
         * them to the end of targetColumns. And use 'origColIdxsForExtendCols' to save
         * the index of column in 'targetColumns' which the shadow column related to.
         * eg: origin targetColumns: (A,B,C), shadow column: __doris_shadow_B after
         * processing, targetColumns: (A, B, C, __doris_shadow_B), and
         * origColIdxsForExtendCols has 1 element: "1", which is the index of column B
         * in targetColumns.
         *
         * Rule A: If the column which the shadow column related to is not mentioned,
         * then do not add the shadow column to targetColumns. They will be filled by
         * null or default value when loading.
         *
         * When table have materialized view, there may be some materialized view columns.
         * we should add them to the end of targetColumns.
         * eg: origin targetColumns: (A,B,C), shadow column: mv_bitmap_union_C
         * after processing, targetColumns: (A, B, C, mv_bitmap_union_C), and
         * origColIdx2MVColumn has 1 element: "2, mv_bitmap_union_C"
         * will be used in as a mapping from queryStmt.getResultExprs() to targetColumns define expr
         */
        List<Pair<Integer, Column>> origColIdxsForExtendCols = Lists.newArrayList();
        for (Column column : targetTable.getFullSchema()) {
            if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                String origName = Column.removeNamePrefix(column.getName());
                for (int i = 0; i < targetColumns.size(); i++) {
                    if (targetColumns.get(i).nameEquals(origName, false)) {
                        // Rule A
                        origColIdxsForExtendCols.add(Pair.of(i, null));
                        targetColumns.add(column);
                        break;
                    }
                }
            }
            if (column.isNameWithPrefix(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)
                    || column.isNameWithPrefix(CreateMaterializedViewStmt.MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX)) {
                List<SlotRef> refColumns = column.getRefColumns();
                if (refColumns == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR,
                            column.getName(), targetTable.getName());
                }
                for (SlotRef refColumn : refColumns) {
                    String origName = refColumn.getColumnName();
                    for (int originColumnIdx = 0; originColumnIdx < targetColumns.size(); originColumnIdx++) {
                        if (targetColumns.get(originColumnIdx).nameEquals(origName, false)) {
                            origColIdxsForExtendCols.add(Pair.of(originColumnIdx, column));
                            targetColumns.add(column);
                            break;
                        }
                    }
                }
            }
        }

        // parse query statement
        queryStmt.setFromInsert(true);
        queryStmt.analyze(analyzer);

        // deal with this case: insert into tbl values();
        // should try to insert default values for all columns in tbl if set
        if (isValuesOrConstantSelect) {
            final ValueList valueList = ((SelectStmt) queryStmt).getValueList();
            if (valueList != null && valueList.getFirstRow().isEmpty() && CollectionUtils.isEmpty(targetColumnNames)) {
                final int rowSize = mentionedColumns.size();
                final List<String> colLabels = queryStmt.getColLabels();
                final List<Expr> resultExprs = queryStmt.getResultExprs();
                Preconditions.checkState(resultExprs.isEmpty(), "result exprs should be empty.");
                for (int i = 0; i < rowSize; i++) {
                    resultExprs.add(new StringLiteral(SelectStmt.DEFAULT_VALUE));
                    final DefaultValueExpr defaultValueExpr = new DefaultValueExpr();
                    valueList.getFirstRow().add(defaultValueExpr);
                    colLabels.add(defaultValueExpr.toColumnLabel());
                }
            }
        }

        // check if size of select item equal with columns mentioned in statement
        if (mentionedColumns.size() != queryStmt.getResultExprs().size()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_COUNT);
        }

        if (analyzer.getContext().getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
            trySetPartialUpdate();
        }

        // Check if all columns mentioned is enough
        checkColumnCoverage(mentionedColumns, targetTable.getBaseSchema());

        realTargetColumnNames = targetColumns.stream().map(Column::getName).collect(Collectors.toList());
        Map<String, Expr> slotToIndex = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < queryStmt.getResultExprs().size(); i++) {
            Expr expr = queryStmt.getResultExprs().get(i);
            if (!(expr instanceof StringLiteral && ((StringLiteral) expr).getValue()
                    .equals(SelectStmt.DEFAULT_VALUE))) {
                slotToIndex.put(realTargetColumnNames.get(i), queryStmt.getResultExprs().get(i)
                        .checkTypeCompatibility(targetTable.getColumn(realTargetColumnNames.get(i)).getType()));
            }
        }

        for (Column column : targetTable.getBaseSchema()) {
            if (!slotToIndex.containsKey(column.getName())) {
                if (column.getDefaultValue() == null) {
                    slotToIndex.put(column.getName(), new NullLiteral());
                } else {
                    slotToIndex.put(column.getName(), new StringLiteral(column.getDefaultValue()));
                }
            }
        }

        // handle VALUES() or SELECT constant list
        if (isValuesOrConstantSelect) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (selectStmt.getValueList() != null) {
                // INSERT INTO VALUES(...)
                List<ArrayList<Expr>> rows = selectStmt.getValueList().getRows();
                for (int rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
                    analyzeRow(analyzer, targetColumns, rows, rowIdx, origColIdxsForExtendCols, slotToIndex);
                }

                // clear these 2 structures, rebuild them using VALUES exprs
                selectStmt.getResultExprs().clear();
                selectStmt.getBaseTblResultExprs().clear();

                for (int i = 0; i < selectStmt.getValueList().getFirstRow().size(); ++i) {
                    selectStmt.getResultExprs().add(selectStmt.getValueList().getFirstRow().get(i));
                    selectStmt.getBaseTblResultExprs().add(selectStmt.getValueList().getFirstRow().get(i));
                }
            } else {
                // INSERT INTO SELECT 1,2,3 ...
                List<ArrayList<Expr>> rows = Lists.newArrayList();
                // ATTN: must copy the `selectStmt.getResultExprs()`, otherwise the following
                // `selectStmt.getResultExprs().clear();` will clear the `rows` too, causing
                // error.
                rows.add(Lists.newArrayList(selectStmt.getResultExprs()));
                analyzeRow(analyzer, targetColumns, rows, 0, origColIdxsForExtendCols, slotToIndex);
                // rows may be changed in analyzeRow(), so rebuild the result exprs
                selectStmt.getResultExprs().clear();
                for (Expr expr : rows.get(0)) {
                    selectStmt.getResultExprs().add(expr);
                }
            }
        } else {
            // INSERT INTO SELECT ... FROM tbl
            if (!origColIdxsForExtendCols.isEmpty()) {
                // extend the result expr by duplicating the related exprs
                for (Pair<Integer, Column> entry : origColIdxsForExtendCols) {
                    if (entry.second == null) {
                        queryStmt.getResultExprs().add(queryStmt.getResultExprs().get(entry.first));
                    } else {
                        //substitute define expr slot with select statement result expr
                        ExprSubstitutionMap smap = new ExprSubstitutionMap();
                        List<SlotRef> columns = entry.second.getRefColumns();
                        for (SlotRef slot : columns) {
                            smap.getLhs().add(slot);
                            smap.getRhs().add(slotToIndex.get(slot.getColumnName()));
                        }
                        Expr e = Expr.substituteList(Lists.newArrayList(entry.second.getDefineExpr()),
                                smap, analyzer, false).get(0);
                        queryStmt.getResultExprs().add(e);
                    }
                }
            }

            // check compatibility
            for (int i = 0; i < targetColumns.size(); ++i) {
                Column column = targetColumns.get(i);
                Expr expr = queryStmt.getResultExprs().get(i);
                queryStmt.getResultExprs().set(i, expr.checkTypeCompatibility(column.getType()));
            }
        }

        // expand colLabels in QueryStmt
        if (!origColIdxsForExtendCols.isEmpty()) {
            if (queryStmt.getResultExprs().size() != queryStmt.getBaseTblResultExprs().size()) {
                for (Pair<Integer, Column> entry : origColIdxsForExtendCols) {
                    if (entry.second == null) {
                        queryStmt.getBaseTblResultExprs().add(queryStmt.getBaseTblResultExprs().get(entry.first));
                    } else {
                        //substitute define expr slot with select statement result expr
                        ExprSubstitutionMap smap = new ExprSubstitutionMap();
                        List<SlotRef> columns = entry.second.getRefColumns();
                        for (SlotRef slot : columns) {
                            smap.getLhs().add(slot);
                            smap.getRhs().add(slotToIndex.get(slot.getColumnName()));
                        }
                        Expr e = Expr.substituteList(Lists.newArrayList(entry.second.getDefineExpr()),
                                smap, analyzer, false).get(0);
                        queryStmt.getBaseTblResultExprs().add(e);
                    }
                }
            }

            if (queryStmt.getResultExprs().size() != queryStmt.getColLabels().size()) {
                for (Pair<Integer, Column> entry : origColIdxsForExtendCols) {
                    queryStmt.getColLabels().add(queryStmt.getColLabels().get(entry.first));
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            for (Expr expr : queryStmt.getResultExprs()) {
                LOG.debug("final result expr: {}, {}", expr, System.identityHashCode(expr));
            }
            for (Expr expr : queryStmt.getBaseTblResultExprs()) {
                LOG.debug("final base table result expr: {}, {}", expr, System.identityHashCode(expr));
            }
            for (String colLabel : queryStmt.getColLabels()) {
                LOG.debug("final col label: {}", colLabel);
            }
        }
    }

    private void analyzeRow(Analyzer analyzer, List<Column> targetColumns, List<ArrayList<Expr>> rows,
            int rowIdx, List<Pair<Integer, Column>> origColIdxsForExtendCols, Map<String, Expr> slotToIndex)
            throws AnalysisException {
        // 1. check number of fields if equal with first row
        // targetColumns contains some shadow columns, which is added by system,
        // so we should minus this
        if (rows.get(rowIdx).size() != targetColumns.size() - origColIdxsForExtendCols.size()) {
            throw new AnalysisException("Column count doesn't match value count at row " + (rowIdx + 1));
        }

        ArrayList<Expr> row = rows.get(rowIdx);
        if (!origColIdxsForExtendCols.isEmpty()) {
            /**
             * we should extend the row for shadow columns.
             * eg:
             *      the origin row has exprs: (expr1, expr2, expr3), and targetColumns is (A, B, C, __doris_shadow_b)
             *      after processing, extentedRow is (expr1, expr2, expr3, expr2)
             */
            ArrayList<Expr> extentedRow = Lists.newArrayList();
            extentedRow.addAll(row);

            for (Pair<Integer, Column> entry : origColIdxsForExtendCols) {
                if (entry != null) {
                    if (entry.second == null) {
                        extentedRow.add(extentedRow.get(entry.first));
                    } else {
                        ExprSubstitutionMap smap = new ExprSubstitutionMap();
                        List<SlotRef> columns = entry.second.getRefColumns();
                        for (SlotRef slot : columns) {
                            smap.getLhs().add(slot);
                            smap.getRhs().add(slotToIndex.get(slot.getColumnName()));
                        }
                        extentedRow.add(Expr.substituteList(Lists.newArrayList(entry.second.getDefineExpr()),
                                smap, analyzer, false).get(0));
                    }
                }
            }

            row = extentedRow;
            rows.set(rowIdx, row);
        }
        // check the compatibility of expr in row and column in targetColumns
        for (int i = 0; i < row.size(); ++i) {
            Expr expr = row.get(i);
            Column col = targetColumns.get(i);

            if (expr instanceof DefaultValueExpr) {
                if (targetColumns.get(i).getDefaultValue() == null) {
                    throw new AnalysisException("Column has no default value, column="
                            + targetColumns.get(i).getName());
                }
                expr = new StringLiteral(targetColumns.get(i).getDefaultValue());
            }
            if (expr instanceof Subquery) {
                throw new AnalysisException("Insert values can not be query");
            }

            expr.analyze(analyzer);

            row.set(i, expr.checkTypeCompatibility(col.getType()));
        }
    }

    private void analyzePlanHints() throws AnalysisException {
        if (planHints == null) {
            return;
        }
        for (String hint : planHints) {
            if (SHUFFLE_HINT.equalsIgnoreCase(hint)) {
                if (!targetTable.isPartitioned()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_INSERT_HINT_NOT_SUPPORT);
                }
                if (isRepartition != null && !isRepartition) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PLAN_HINT_CONFILT, hint);
                }
                isRepartition = Boolean.TRUE;
            } else if (NOSHUFFLE_HINT.equalsIgnoreCase(hint)) {
                if (!targetTable.isPartitioned()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_INSERT_HINT_NOT_SUPPORT);
                }
                if (isRepartition != null && isRepartition) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PLAN_HINT_CONFILT, hint);
                }
                isRepartition = Boolean.FALSE;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PLAN_HINT, hint);
            }
        }
    }

    public void prepareExpressions() throws UserException {
        List<Expr> selectList = Expr.cloneList(queryStmt.getResultExprs());
        // check type compatibility
        int numCols = targetColumns.size();
        for (int i = 0; i < numCols; ++i) {
            Column col = targetColumns.get(i);
            Expr expr = selectList.get(i).checkTypeCompatibility(col.getType());
            selectList.set(i, expr);
            exprByName.put(col.getName(), expr);
        }

        List<Pair<String, Expr>> resultExprByName = Lists.newArrayList();
        Map<String, Expr> slotToIndex = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        // reorder resultExprs in table column order
        for (Column col : targetTable.getFullSchema()) {
            if (isPartialUpdate && !partialUpdateCols.contains(col.getName())) {
                continue;
            }
            Expr targetExpr;
            if (exprByName.containsKey(col.getName())) {
                targetExpr = exprByName.get(col.getName());
            } else if (targetTable.getType().equals(TableIf.TableType.JDBC_EXTERNAL_TABLE)) {
                // For JdbcTable,we do not need to generate plans for columns that are not specified at write time
                continue;
            } else {
                // process sequence col, map sequence column to other column
                if (targetTable instanceof OlapTable && ((OlapTable) targetTable).hasSequenceCol()
                        && col.getName().equals(Column.SEQUENCE_COL)
                        && ((OlapTable) targetTable).getSequenceMapCol() != null) {
                    if (resultExprByName.stream().map(Pair::key)
                            .anyMatch(key -> key.equals(((OlapTable) targetTable).getSequenceMapCol()))) {
                        resultExprByName.add(Pair.of(Column.SEQUENCE_COL,
                                resultExprByName.stream()
                                        .filter(p -> p.key().equals(((OlapTable) targetTable).getSequenceMapCol()))
                                        .map(Pair::value).findFirst().orElse(null)));
                    }
                    continue;
                } else if (col.getDefineExpr() != null) {
                    // substitute define expr slot with select statement result expr
                    ExprSubstitutionMap smap = new ExprSubstitutionMap();
                    List<SlotRef> columns = col.getRefColumns();
                    for (SlotRef slot : columns) {
                        smap.getLhs().add(slot);
                        smap.getRhs().add(slotToIndex.get(slot.getColumnName()));
                    }
                    targetExpr = Expr
                            .substituteList(Lists.newArrayList(col.getDefineExpr().clone()), smap, analyzer, false)
                            .get(0);
                } else if (col.getDefaultValue() == null) {
                    targetExpr = NullLiteral.create(col.getType());
                } else {
                    if (col.getDefaultValueExprDef() != null) {
                        targetExpr = col.getDefaultValueExpr();
                    } else {
                        StringLiteral defaultValueExpr;
                        defaultValueExpr = new StringLiteral(col.getDefaultValue());
                        targetExpr = defaultValueExpr.checkTypeCompatibility(col.getType());
                    }
                }
            }
            resultExprByName.add(Pair.of(col.getName(), targetExpr));
            slotToIndex.put(col.getName(), targetExpr);
        }
        resultExprs.addAll(resultExprByName.stream().map(Pair::value).collect(Collectors.toList()));
    }

    private DataSink createDataSink() throws AnalysisException {
        if (dataSink != null) {
            return dataSink;
        }
        if (targetTable instanceof OlapTable) {
            dataSink = new OlapTableSink((OlapTable) targetTable, olapTuple, targetPartitionIds,
                    analyzer.getContext().getSessionVariable().isEnableSingleReplicaInsert());
            OlapTableSink sink = (OlapTableSink) dataSink;
            sink.setPartialUpdateInputColumns(isPartialUpdate, partialUpdateCols);
            dataPartition = dataSink.getOutputPartition();
        } else if (targetTable instanceof BrokerTable) {
            BrokerTable table = (BrokerTable) targetTable;
            // TODO(lingbin): think use which one if have more than one path
            // Map<String, String> brokerProperties = Maps.newHashMap();
            // BrokerDesc brokerDesc = new BrokerDesc("test_broker", brokerProperties);
            BrokerDesc brokerDesc = new BrokerDesc(table.getBrokerName(), table.getBrokerProperties());
            dataSink = new ExportSink(
                    table.getWritablePath(),
                    table.getColumnSeparator(),
                    table.getLineDelimiter(),
                    brokerDesc);
            dataPartition = dataSink.getOutputPartition();
        } else if (targetTable instanceof JdbcTable) {
            //for JdbcTable,we need to pass the currently written column to `JdbcTableSink`
            //to generate the prepare insert statment
            List<String> insertCols = Lists.newArrayList();
            for (Column column : targetColumns) {
                insertCols.add(column.getName());
            }
            dataSink = new JdbcTableSink((JdbcTable) targetTable, insertCols);
            dataPartition = DataPartition.UNPARTITIONED;
        } else {
            dataSink = DataSink.createDataSink(targetTable);
            dataPartition = DataPartition.UNPARTITIONED;
        }
        return dataSink;
    }

    public void complete() throws UserException {
        if (!isExplain() && targetTable instanceof OlapTable) {
            ((OlapTableSink) dataSink).complete(analyzer);
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(db.getId(), transactionId);
            if (txnState == null) {
                throw new DdlException("txn does not exist: " + transactionId);
            }
            txnState.addTableIndexes((OlapTable) targetTable);
            if (!isFromDeleteOrUpdateStmt && isPartialUpdate) {
                txnState.setSchemaForPartialUpdate((OlapTable) targetTable);
            }
        }
    }

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    @Override
    public List<? extends DataDesc> getDataDescList() {
        throw new UnsupportedOperationException("only invoked for external load currently");
    }

    @Override
    public ResourceDesc getResourceDesc() {
        throw new UnsupportedOperationException("only invoked for external load currently");
    }

    @Override
    public LoadType getLoadType() {
        return LoadType.NATIVE_INSERT;
    }

    @Override
    public NativeInsertStmt getNativeInsertStmt() {
        return this;
    }

    @Override
    public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        queryStmt.rewriteExprs(rewriter);
    }

    @Override
    public void foldConstant(ExprRewriter rewriter, TQueryOptions tQueryOptions) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        queryStmt.foldConstant(rewriter, tQueryOptions);
    }

    @Override
    public List<Expr> getResultExprs() {
        return resultExprs;
    }

    @Override
    public void reset() {
        super.reset();
        if (targetPartitionIds != null) {
            targetPartitionIds.clear();
        }
        queryStmt.reset();
        resultExprs.clear();
        exprByName.clear();
        dataSink = null;
        dataPartition = null;
        targetColumns.clear();
    }

    protected void resetPrepare() {
        label = null;
        isTransactionBegin = false;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (isExplain()) {
            return RedirectStatus.NO_FORWARD;
        } else {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }
    }

    public void setIsFromDeleteOrUpdateStmt(boolean isFromDeleteOrUpdateStmt) {
        this.isFromDeleteOrUpdateStmt = isFromDeleteOrUpdateStmt;
    }

    private void trySetPartialUpdate() throws UserException {
        if (isFromDeleteOrUpdateStmt || isPartialUpdate || !(targetTable instanceof OlapTable)) {
            return;
        }
        OlapTable olapTable = (OlapTable) targetTable;
        if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
            throw new UserException("Partial update is only allowed in unique table with merge-on-write enabled.");
        }
        for (Column col : olapTable.getFullSchema()) {
            boolean exists = false;
            for (Column insertCol : targetColumns) {
                if (insertCol.getName() != null && insertCol.getName().equals(col.getName())) {
                    if (!col.isVisible() && !Column.DELETE_SIGN.equals(col.getName())) {
                        throw new UserException("Partial update should not include invisible column except"
                                    + " delete sign column: " + col.getName());
                    }
                    exists = true;
                    break;
                }
            }
            if (col.isKey() && !exists) {
                throw new UserException("Partial update should include all key columns, missing: " + col.getName());
            }
        }

        isPartialUpdate = true;
        partialUpdateCols.addAll(targetColumnNames);
        if (isPartialUpdate && olapTable.hasSequenceCol() && olapTable.getSequenceMapCol() != null
                && partialUpdateCols.contains(olapTable.getSequenceMapCol())) {
            partialUpdateCols.add(Column.SEQUENCE_COL);
        }
        // we should re-generate olapTuple
        DescriptorTable descTable = analyzer.getDescTbl();
        olapTuple = descTable.createTupleDescriptor();
        for (Column col : olapTable.getFullSchema()) {
            if (!partialUpdateCols.contains(col.getName())) {
                continue;
            }
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(olapTuple);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(col.getType());
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
        }
    }
}
