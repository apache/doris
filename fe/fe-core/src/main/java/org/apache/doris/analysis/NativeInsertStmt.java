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
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalDatabase;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.datasource.jdbc.sink.JdbcTableSink;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.ExportSink;
import org.apache.doris.planner.GroupCommitBlockSink;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.ExecuteEnv;
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
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
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
@Deprecated
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

    // Used for group commit insert
    private boolean isGroupCommit = false;
    private int baseSchemaVersion = -1;
    private TUniqueId loadId = null;
    private long tableId = -1;
    public boolean isGroupCommitStreamLoadSql = false;
    private GroupCommitPlanner groupCommitPlanner;
    private boolean reuseGroupCommitPlan = false;

    private boolean isFromDeleteOrUpdateStmt = false;

    private InsertType insertType = InsertType.NATIVE_INSERT;

    boolean hasEmptyTargetColumns = false;
    private boolean allowAutoPartition = true;
    private boolean withAutoDetectOverwrite = false;

    enum InsertType {
        NATIVE_INSERT("insert_"),
        UPDATE("update_"),
        DELETE("delete_");
        private String labePrefix;

        InsertType(String labePrefix) {
            this.labePrefix = labePrefix;
        }
    }

    public NativeInsertStmt(NativeInsertStmt other) {
        super(other.label, null, null);
        this.tblName = other.tblName;
        this.targetPartitionNames = other.targetPartitionNames;
        this.label = other.label;
        this.queryStmt = other.queryStmt;
        this.planHints = other.planHints;
        this.targetColumnNames = other.targetColumnNames;
        this.isValuesOrConstantSelect = other.isValuesOrConstantSelect;
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

    // Ctor of group commit in sql parser
    public NativeInsertStmt(long tableId, String label, List<String> cols, InsertSource source,
            List<String> hints) {
        this(new InsertTarget(new TableName(null, null, null), null), label, cols, source, hints);
        this.tableId = tableId;
    }

    // Ctor for CreateTableAsSelectStmt and InsertOverwriteTableStmt
    public NativeInsertStmt(TableName name, PartitionNames targetPartitionNames, LabelName label,
            QueryStmt queryStmt, List<String> planHints, List<String> targetColumnNames, boolean allowAutoPartition) {
        super(label, null, null);
        this.tblName = name;
        this.targetPartitionNames = targetPartitionNames;
        this.queryStmt = queryStmt;
        this.planHints = planHints;
        this.targetColumnNames = targetColumnNames;
        this.allowAutoPartition = allowAutoPartition;
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

    public List<String> getTargetColumnNames() {
        return targetColumnNames;
    }

    public void getTables(Analyzer analyzer, Map<Long, TableIf> tableMap, Set<String> parentViewNameSet)
            throws AnalysisException {
        if (tableId != -1) {
            TableIf table = Env.getCurrentInternalCatalog().getTableByTableId(tableId);
            Preconditions.checkState(table instanceof OlapTable);
            OlapTable olapTable = (OlapTable) table;
            tblName.setDb(olapTable.getDatabase().getFullName());
            tblName.setTbl(olapTable.getName());
            if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS || olapTable.getTableProperty().storeRowColumn()) {
                List<Column> columns = Lists.newArrayList(olapTable.getBaseSchema(true));
                targetColumnNames = columns.stream().map(c -> c.getName()).collect(Collectors.toList());
            }
        }

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

    public NativeInsertStmt withAutoDetectOverwrite() {
        this.withAutoDetectOverwrite = true;
        return this;
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
        db = analyzer.getEnv().getCatalogMgr().getCatalog(tblName.getCtl()).getDbOrAnalysisException(tblName.getDb());

        analyzeGroupCommit(analyzer);
        if (isGroupCommit()) {
            return;
        }

        analyzeSubquery(analyzer, false);

        analyzePlanHints();

        if (analyzer.getContext().isTxnModel()) {
            return;
        }

        // create data sink
        createDataSink();

        // create label and begin transaction
        long timeoutSecond = ConnectContext.get().getExecTimeout();
        if (label == null || Strings.isNullOrEmpty(label.getLabelName())) {
            label = new LabelName(db.getFullName(),
                    insertType.labePrefix + DebugUtil.printId(analyzer.getContext().queryId()).replace("-", "_"));
        }
        if (!isExplain() && !isTransactionBegin && !isGroupCommitStreamLoadSql) {
            if (targetTable instanceof OlapTable) {
                LoadJobSourceType sourceType = LoadJobSourceType.INSERT_STREAMING;
                transactionId = Env.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                        Lists.newArrayList(targetTable.getId()), label.getLabelName(),
                        new TxnCoordinator(TxnSourceType.FE, 0,
                                FrontendOptions.getLocalHostAddress(),
                                ExecuteEnv.getInstance().getStartupTime()),
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
            sink.init(loadId, transactionId, db.getId(), timeoutSecond,
                    sendBatchParallelism, false, isInsertStrict, timeoutSecond);
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

            // For Unique Key table with sequence column (which default value is not CURRENT_TIMESTAMP),
            // user MUST specify the sequence column while inserting data
            //
            // case1: create table by `function_column.sequence_col`
            //        a) insert with column list, must include the sequence map column
            //        b) insert without column list, already contains the column, don't need to check
            // case2: create table by `function_column.sequence_type`
            //        a) insert with column list, must include the hidden column __DORIS_SEQUENCE_COL__
            //        b) insert without column list, don't include the hidden column __DORIS_SEQUENCE_COL__
            //           by default, will fail.
            if (olapTable.hasSequenceCol()) {
                boolean haveInputSeqCol = false;
                Optional<Column> seqColInTable = Optional.empty();
                if (olapTable.getSequenceMapCol() != null) {
                    if (targetColumnNames != null) {
                        if (targetColumnNames.stream()
                                .anyMatch(c -> c.equalsIgnoreCase(olapTable.getSequenceMapCol()))) {
                            haveInputSeqCol = true; // case1.a
                        }
                    } else {
                        haveInputSeqCol = true; // case1.b
                    }
                    seqColInTable = olapTable.getFullSchema().stream()
                            .filter(col -> col.getName().equalsIgnoreCase(olapTable.getSequenceMapCol())).findFirst();
                } else {
                    if (targetColumnNames != null) {
                        if (targetColumnNames.stream()
                                .anyMatch(c -> c.equalsIgnoreCase(Column.SEQUENCE_COL))) {
                            haveInputSeqCol = true; // case2.a
                        } // else case2.b
                    }
                }

                if (!haveInputSeqCol && !isPartialUpdate && !isFromDeleteOrUpdateStmt
                        && !analyzer.getContext().getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
                    if (!seqColInTable.isPresent() || seqColInTable.get().getDefaultValue() == null
                            || !seqColInTable.get().getDefaultValue()
                            .equalsIgnoreCase(DefaultValue.CURRENT_TIMESTAMP)) {
                        throw new AnalysisException("Table " + olapTable.getName()
                                + " has sequence column, need to specify the sequence column");
                    }
                }
            }

            if (isPartialUpdate && olapTable.hasSequenceCol() && olapTable.getSequenceMapCol() != null
                    && partialUpdateCols.stream().anyMatch(c -> c.equalsIgnoreCase(olapTable.getSequenceMapCol()))) {
                partialUpdateCols.add(Column.SEQUENCE_COL);
            }
            // need a descriptor
            DescriptorTable descTable = analyzer.getDescTbl();
            olapTuple = descTable.createTupleDescriptor();
            for (Column col : olapTable.getFullSchema()) {
                if (isPartialUpdate && partialUpdateCols.stream().noneMatch(c -> c.equalsIgnoreCase(col.getName()))) {
                    continue;
                }
                SlotDescriptor slotDesc = descTable.addSlotDescriptor(olapTuple);
                slotDesc.setIsMaterialized(true);
                slotDesc.setType(col.getType());
                slotDesc.setColumn(col);
                slotDesc.setIsNullable(col.isAllowNull());
                slotDesc.setAutoInc(col.isAutoInc());
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

    private void analyzeSubquery(Analyzer analyzer, boolean skipCheck) throws UserException {
        // Analyze columns mentioned in the statement.
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (targetColumnNames == null) {
            hasEmptyTargetColumns = true;
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
                if (col.getType().isObjectStored() && !col.hasDefaultValue()
                        && !mentionedColumns.contains(col.getName())) {
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
        if (!ConnectContext.get().isTxnModel()) {
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
                        || column.isNameWithPrefix(
                        CreateMaterializedViewStmt.MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX)) {
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

        if (analyzer.getContext().getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
            trySetPartialUpdate();
        }

        // check if size of select item equal with columns mentioned in statement
        if (mentionedColumns.size() != queryStmt.getResultExprs().size()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_COUNT);
        }

        // Check if all columns mentioned is enough
        // For JdbcTable, it is allowed to insert without specifying all columns and without checking
        if (!(targetTable instanceof JdbcTable)) {
            checkColumnCoverage(mentionedColumns, targetTable.getBaseSchema());
        }

        List<String> realTargetColumnNames = targetColumns.stream().map(Column::getName).collect(Collectors.toList());

        // handle VALUES() or SELECT constant list
        if (isValuesOrConstantSelect) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (selectStmt.getValueList() != null) {
                // INSERT INTO VALUES(...)
                List<ArrayList<Expr>> rows = selectStmt.getValueList().getRows();
                for (int rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
                    // Only check for JdbcTable
                    if (targetTable instanceof JdbcTable) {
                        // Check for NULL values in not-nullable columns
                        for (int colIdx = 0; colIdx < targetColumns.size(); ++colIdx) {
                            Column column = targetColumns.get(colIdx);
                            // Ensure rows.get(rowIdx) has enough columns to match targetColumns
                            if (colIdx < rows.get(rowIdx).size()) {
                                Expr expr = rows.get(rowIdx).get(colIdx);
                                if (!column.isAllowNull() && expr instanceof NullLiteral) {
                                    throw new AnalysisException("Column `" + column.getName()
                                            + "` is not nullable, but the inserted value is nullable.");
                                }
                            }
                        }
                    }
                    analyzeRow(analyzer, targetColumns, rows, rowIdx, origColIdxsForExtendCols, realTargetColumnNames,
                            skipCheck);
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
                analyzeRow(analyzer, targetColumns, rows, 0, origColIdxsForExtendCols, realTargetColumnNames,
                        skipCheck);
                // rows may be changed in analyzeRow(), so rebuild the result exprs
                selectStmt.getResultExprs().clear();

                // For JdbcTable, need to check whether there is a NULL value inserted into the NOT NULL column
                if (targetTable instanceof JdbcTable) {
                    for (int colIdx = 0; colIdx < targetColumns.size(); ++colIdx) {
                        Column column = targetColumns.get(colIdx);
                        Expr expr = rows.get(0).get(colIdx);
                        if (!column.isAllowNull() && expr instanceof NullLiteral) {
                            throw new AnalysisException("Column `" + column.getName()
                                    + "` is not nullable, but the inserted value is nullable.");
                        }
                    }
                }

                for (Expr expr : rows.get(0)) {
                    selectStmt.getResultExprs().add(expr);
                }
            }
        } else {
            // INSERT INTO SELECT ... FROM tbl
            if (!origColIdxsForExtendCols.isEmpty()) {
                // extend the result expr by duplicating the related exprs
                Map<String, Expr> slotToIndex = buildSlotToIndex(queryStmt.getResultExprs(), realTargetColumnNames,
                        analyzer);
                for (Pair<Integer, Column> entry : origColIdxsForExtendCols) {
                    if (entry.second == null) {
                        queryStmt.getResultExprs().add(queryStmt.getResultExprs().get(entry.first));
                    } else {
                        // substitute define expr slot with select statement result expr
                        ExprSubstitutionMap smap = new ExprSubstitutionMap();
                        List<SlotRef> columns = entry.second.getRefColumns();
                        for (SlotRef slot : columns) {
                            smap.getLhs().add(slot);
                            smap.getRhs()
                                    .add(Load.getExprFromDesc(analyzer, slotToIndex.get(slot.getColumnName()), slot));
                        }
                        Expr e = entry.second.getDefineExpr().clone(smap);
                        e.analyze(analyzer);
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
                Map<String, Expr> slotToIndex = buildSlotToIndex(queryStmt.getBaseTblResultExprs(),
                        realTargetColumnNames, analyzer);
                for (Pair<Integer, Column> entry : origColIdxsForExtendCols) {
                    if (entry.second == null) {
                        queryStmt.getBaseTblResultExprs().add(queryStmt.getBaseTblResultExprs().get(entry.first));
                    } else {
                        // substitute define expr slot with select statement result expr
                        ExprSubstitutionMap smap = new ExprSubstitutionMap();
                        List<SlotRef> columns = entry.second.getRefColumns();
                        for (SlotRef slot : columns) {
                            smap.getLhs().add(slot);
                            smap.getRhs()
                                    .add(Load.getExprFromDesc(analyzer, slotToIndex.get(slot.getColumnName()), slot));
                        }
                        Expr e = entry.second.getDefineExpr().clone(smap);
                        e.analyze(analyzer);
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("final result expr: {}, {}", expr, System.identityHashCode(expr));
                }
            }
            for (Expr expr : queryStmt.getBaseTblResultExprs()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("final base table result expr: {}, {}", expr, System.identityHashCode(expr));
                }
            }
            for (String colLabel : queryStmt.getColLabels()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("final col label: {}", colLabel);
                }
            }
        }
    }

    private Map<String, Expr> buildSlotToIndex(ArrayList<Expr> row, List<String> realTargetColumnNames,
            Analyzer analyzer) throws AnalysisException {
        Map<String, Expr> slotToIndex = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < row.size(); i++) {
            Expr expr = row.get(i);
            expr.analyze(analyzer);
            if (expr instanceof DefaultValueExpr || expr instanceof StringLiteral
                    && ((StringLiteral) expr).getValue().equals(SelectStmt.DEFAULT_VALUE)) {
                continue;
            }
            expr.analyze(analyzer);
            slotToIndex.put(realTargetColumnNames.get(i),
                    expr.checkTypeCompatibility(targetTable.getColumn(realTargetColumnNames.get(i)).getType()));
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
        return slotToIndex;
    }

    private void analyzeRow(Analyzer analyzer, List<Column> targetColumns, List<ArrayList<Expr>> rows, int rowIdx,
            List<Pair<Integer, Column>> origColIdxsForExtendCols, List<String> realTargetColumnNames, boolean skipCheck)
            throws AnalysisException {
        // 1. check number of fields if equal with first row
        // targetColumns contains some shadow columns, which is added by system,
        // so we should minus this
        if (rows.get(rowIdx).size() != targetColumns.size() - origColIdxsForExtendCols.size()) {
            throw new AnalysisException("Column count doesn't match value count at row " + (rowIdx + 1));
        }
        if (skipCheck) {
            return;
        }

        ArrayList<Expr> row = rows.get(rowIdx);
        Map<String, Expr> slotToIndex = buildSlotToIndex(row, realTargetColumnNames, analyzer);

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
                            smap.getRhs()
                                    .add(Load.getExprFromDesc(analyzer, slotToIndex.get(slot.getColumnName()), slot));
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
                if (targetColumns.get(i).getDefaultValue() == null && !targetColumns.get(i).isAllowNull()
                        && !targetColumns.get(i).isAutoInc()) {
                    throw new AnalysisException("Column has no default value, column="
                            + targetColumns.get(i).getName());
                }
                if (targetColumns.get(i).getDefaultValue() == null) {
                    expr = new NullLiteral();
                } else {
                    expr = new StringLiteral(targetColumns.get(i).getDefaultValue());
                }
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
                if (!targetTable.isPartitionedTable()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_INSERT_HINT_NOT_SUPPORT);
                }
                if (isRepartition != null && !isRepartition) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PLAN_HINT_CONFILT, hint);
                }
                isRepartition = Boolean.TRUE;
            } else if (NOSHUFFLE_HINT.equalsIgnoreCase(hint)) {
                if (!targetTable.isPartitionedTable()) {
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
            exprByName.put(col.getName(), selectList.get(i));
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
                            .anyMatch(key -> key.equalsIgnoreCase(((OlapTable) targetTable).getSequenceMapCol()))) {
                        resultExprByName.add(Pair.of(Column.SEQUENCE_COL,
                                resultExprByName.stream()
                                        .filter(p -> p.key()
                                                .equalsIgnoreCase(((OlapTable) targetTable).getSequenceMapCol()))
                                        .map(Pair::value).findFirst().orElse(null)));
                    }
                    continue;
                } else if (col.getDefineExpr() != null) {
                    targetExpr = col.getDefineExpr().clone();
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

            List<SlotRef> columns = col.getRefColumns();
            if (columns != null) {
                // substitute define expr slot with select statement result expr
                ExprSubstitutionMap smap = new ExprSubstitutionMap();
                for (SlotRef slot : columns) {
                    smap.getLhs().add(slot);
                    smap.getRhs().add(Load.getExprFromDesc(analyzer, slotToIndex.get(slot.getColumnName()), slot));
                }
                targetExpr = targetExpr.clone(smap);
                targetExpr.analyze(analyzer);
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
            OlapTableSink sink;
            final boolean enableSingleReplicaLoad =
                    analyzer.getContext().getSessionVariable().isEnableMemtableOnSinkNode()
                    ? false : analyzer.getContext().getSessionVariable().isEnableSingleReplicaInsert();
            if (isGroupCommitStreamLoadSql) {
                sink = new GroupCommitBlockSink((OlapTable) targetTable, olapTuple,
                        targetPartitionIds, enableSingleReplicaLoad,
                        ConnectContext.get().getSessionVariable().getGroupCommit(), 0);
            } else {
                sink = new OlapTableSink((OlapTable) targetTable, olapTuple, targetPartitionIds,
                        enableSingleReplicaLoad);
            }
            dataSink = sink;
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
            // For JdbcTable, reorder targetColumns to match the order in targetTable.getFullSchema()
            List<String> insertCols = new ArrayList<>();
            Set<String> targetColumnNames = targetColumns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toSet());

            for (Column column : targetTable.getFullSchema()) {
                if (targetColumnNames.contains(column.getName())) {
                    insertCols.add(column.getName());
                }
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
            if (!allowAutoPartition) {
                ((OlapTableSink) dataSink).setAutoPartition(false);
            }
            if (!isGroupCommitStreamLoadSql) {
                // add table indexes to transaction state
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(db.getId(), transactionId);
                if (txnState == null) {
                    throw new DdlException("txn does not exist: " + transactionId);
                }
                txnState.addTableIndexes((OlapTable) targetTable);
                if (isPartialUpdate) {
                    txnState.setSchemaForPartialUpdate((OlapTable) targetTable);
                }
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
    public void rewriteElementAtToSlot(ExprRewriter rewriter, TQueryOptions tQueryOptions) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        queryStmt.rewriteElementAtToSlot(rewriter, tQueryOptions);
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

    public void resetPrepare() {
        label = null;
        isTransactionBegin = false;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (isExplain() || isGroupCommit() || (ConnectContext.get() != null && ConnectContext.get().isTxnModel())) {
            return RedirectStatus.NO_FORWARD;
        } else {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }
    }

    public void analyzeGroupCommit(Analyzer analyzer) throws AnalysisException {
        // check if http stream meets group commit requirements.
        // If not meets, throw exception (consider fallback to non group commit mode).
        if (isGroupCommitStreamLoadSql) {
            if (targetTable != null && (targetTable instanceof OlapTable)
                    && !((OlapTable) targetTable).getTableProperty().getUseSchemaLightChange()) {
                throw new AnalysisException(
                        "table light_schema_change is false, can't do http_stream with group commit mode");
            }
            return;
        }
        // check if 'insert into' meets group commit requirements. If meets, set isGroupCommit to true
        if (isGroupCommit) {
            return;
        }
        try {
            tblName.analyze(analyzer);
            initTargetTable(analyzer);
        } catch (Throwable e) {
            LOG.warn("analyze group commit failed", e);
            return;
        }
        ConnectContext ctx = ConnectContext.get();
        List<Pair<BooleanSupplier, Supplier<String>>> conditions = new ArrayList<>();
        conditions.add(Pair.of(() -> ctx.getSessionVariable().isEnableInsertGroupCommit(),
                () -> "group_commit session variable: " + ctx.getSessionVariable().groupCommit));
        conditions.add(Pair.of(() -> !isExplain(), () -> "isExplain"));
        conditions.add(Pair.of(() -> !ctx.getSessionVariable().isEnableUniqueKeyPartialUpdate(),
                () -> "enableUniqueKeyPartialUpdate"));
        conditions.add(Pair.of(() -> !ctx.isTxnModel(), () -> "isTxnModel"));
        conditions.add(Pair.of(() -> targetTable instanceof OlapTable,
                () -> "not olapTable, class: " + targetTable.getClass().getName()));
        conditions.add(Pair.of(() -> ((OlapTable) targetTable).getTableProperty().getUseSchemaLightChange(),
                () -> "notUseSchemaLightChange"));
        conditions.add(Pair.of(() -> !targetTable.getQualifiedDbName().equalsIgnoreCase(FeConstants.INTERNAL_DB_NAME),
                () -> "db is internal"));
        conditions.add(
                Pair.of(() -> targetPartitionNames == null, () -> "targetPartitionNames: " + targetPartitionNames));
        conditions.add(Pair.of(() -> ctx.getSessionVariable().getSqlMode() != SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES,
                () -> "sqlMode: " + ctx.getSessionVariable().getSqlMode()));
        conditions.add(Pair.of(() -> queryStmt instanceof SelectStmt,
                () -> "queryStmt is not SelectStmt, class: " + queryStmt.getClass().getName()));
        conditions.add(Pair.of(() -> ((SelectStmt) queryStmt).getTableRefs().isEmpty(),
                () -> "tableRefs is not empty: " + ((SelectStmt) queryStmt).getTableRefs()));
        conditions.add(
                Pair.of(() -> (label == null || Strings.isNullOrEmpty(label.getLabelName())), () -> "label: " + label));
        conditions.add(
                Pair.of(() -> (analyzer == null || analyzer != null && !analyzer.isReAnalyze()), () -> "analyzer"));
        boolean match = conditions.stream().allMatch(p -> p.first.getAsBoolean());
        if (match) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (selectStmt.getValueList() != null) {
                for (List<Expr> row : selectStmt.getValueList().getRows()) {
                    for (Expr expr : row) {
                        if (!(expr instanceof LiteralExpr)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("group commit is off for query_id: {}, table: {}, "
                                                + "because not literal expr: {}, row: {}",
                                        DebugUtil.printId(ctx.queryId()), targetTable.getName(), expr, row);
                            }
                            return;
                        }
                    }
                }
                // Does not support: insert into tbl values();
                if (selectStmt.getValueList().getFirstRow().isEmpty() && CollectionUtils.isEmpty(targetColumnNames)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("group commit is off for query_id: {}, table: {}, because first row: {}, "
                                        + "target columns: {}", DebugUtil.printId(ctx.queryId()), targetTable.getName(),
                                selectStmt.getValueList().getFirstRow(), targetColumnNames);
                    }
                    return;
                }
            } else {
                SelectList selectList = selectStmt.getSelectList();
                if (selectList != null) {
                    List<SelectListItem> items = selectList.getItems();
                    if (items != null) {
                        for (SelectListItem item : items) {
                            if (item.getExpr() != null && !(item.getExpr() instanceof LiteralExpr)) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("group commit is off for query_id: {}, table: {}, "
                                                    + "because not literal expr: {}, row: {}",
                                            DebugUtil.printId(ctx.queryId()), targetTable.getName(), item.getExpr(),
                                            item);
                                }
                                return;
                            }
                        }
                    }
                }
            }
            isGroupCommit = true;
        } else {
            if (LOG.isDebugEnabled()) {
                for (Pair<BooleanSupplier, Supplier<String>> pair : conditions) {
                    if (pair.first.getAsBoolean() == false) {
                        LOG.debug("group commit is off for query_id: {}, table: {}, because: {}",
                                DebugUtil.printId(ctx.queryId()), targetTable.getName(), pair.second.get());
                        break;
                    }
                }
            }
        }
    }

    public boolean isGroupCommit() {
        return isGroupCommit;
    }

    public boolean isReuseGroupCommitPlan() {
        return reuseGroupCommitPlan;
    }

    public GroupCommitPlanner planForGroupCommit(TUniqueId queryId) throws UserException, TException {
        OlapTable olapTable = (OlapTable) getTargetTable();
        olapTable.readLock();
        try {
            if (groupCommitPlanner != null && olapTable.getBaseSchemaVersion() == baseSchemaVersion) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("reuse group commit plan, table={}", olapTable);
                }
                reuseGroupCommitPlan = true;
                return groupCommitPlanner;
            }
            reuseGroupCommitPlan = false;
            if (!targetColumns.isEmpty()) {
                Analyzer analyzerTmp = analyzer;
                reset();
                this.analyzer = analyzerTmp;
            }
            analyzeSubquery(analyzer, true);
            groupCommitPlanner = EnvFactory.getInstance().createGroupCommitPlanner((Database) db, olapTable,
                    targetColumnNames, queryId, ConnectContext.get().getSessionVariable().getGroupCommit());
            // save plan message to be reused for prepare stmt
            loadId = queryId;
            baseSchemaVersion = olapTable.getBaseSchemaVersion();
            return groupCommitPlanner;
        } finally {
            olapTable.readUnlock();
        }
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public int getBaseSchemaVersion() {
        return baseSchemaVersion;
    }

    public void setIsFromDeleteOrUpdateStmt(boolean isFromDeleteOrUpdateStmt) {
        this.isFromDeleteOrUpdateStmt = isFromDeleteOrUpdateStmt;
    }

    private void trySetPartialUpdate() throws UserException {
        if (isFromDeleteOrUpdateStmt || isPartialUpdate || !(targetTable instanceof OlapTable)) {
            return;
        }
        OlapTable olapTable = (OlapTable) targetTable;
        if (olapTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            return;
        }
        // when enable_unique_key_partial_update = true,
        // only unique table with MOW insert with target columns can consider be a partial update,
        // and unique table without MOW, insert will be like a normal insert.
        // when enable_unique_key_partial_update = false,
        // unique table with MOW, insert will be a normal insert, and column that not set will insert default value.
        if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
            return;
        }
        if (hasEmptyTargetColumns) {
            return;
        }
        boolean hasMissingColExceptAutoIncKey = false;
        for (Column col : olapTable.getFullSchema()) {
            boolean exists = false;
            for (Column insertCol : targetColumns) {
                if (insertCol.getName() != null && insertCol.getName().equalsIgnoreCase(col.getName())) {
                    if (!col.isVisible() && !Column.DELETE_SIGN.equals(col.getName())) {
                        throw new UserException("Partial update should not include invisible column except"
                                    + " delete sign column: " + col.getName());
                    }
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                if (col.isKey() && !col.isAutoInc()) {
                    throw new UserException("Partial update should include all key columns, missing: " + col.getName());
                }
                if (!(col.isKey() && col.isAutoInc()) && col.isVisible()) {
                    hasMissingColExceptAutoIncKey = true;
                }
            }
        }
        if (!hasMissingColExceptAutoIncKey) {
            return;
        }

        isPartialUpdate = true;
        for (String name : targetColumnNames) {
            Column column = olapTable.getFullSchema().stream()
                    .filter(col -> col.getName().equalsIgnoreCase(name)).findFirst().get();
            partialUpdateCols.add(column.getName());
        }
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

    public boolean containTargetColumnName(String columnName) {
        return targetColumnNames != null && targetColumnNames.stream()
                .anyMatch(col -> col.equalsIgnoreCase(columnName));
    }
}
