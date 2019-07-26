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

import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.ExportSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Insert into is performed to load data from the result of query stmt.
 *
 * syntax:
 *   INSERT INTO table_name [partition_info] [col_list] [plan_hints] query_stmt
 *
 *   table_name: is the name of target table
 *   partition_info: PARTITION (p1,p2)
 *     the partition info of target table
 *   col_list: (c1,c2)
 *     the column list of target table
 *   plan_hints: [STREAMING,SHUFFLE_HINT]
 *     The streaming plan is used by both streaming and non-streaming insert stmt.
 *     The only difference is that non-streaming will record the load info in LoadManager and return label.
 *     User can check the load info by show load stmt.
 */
public class InsertStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(InsertStmt.class);

    public static final String SHUFFLE_HINT = "SHUFFLE";
    public static final String NOSHUFFLE_HINT = "NOSHUFFLE";
    public static final String STREAMING = "STREAMING";

    private final TableName tblName;
    private final Set<String> targetPartitions;
    private final List<String> targetColumnNames;
    private final QueryStmt queryStmt;
    private final List<String> planHints;
    private Boolean isRepartition;
    private boolean isStreaming = false;

    private Map<Long, Integer> indexIdToSchemaHash = null;

    // set after parse all columns and expr in query statement
    // this result expr in the order of target table's columns
    private ArrayList<Expr> resultExprs = Lists.newArrayList();

    private Map<String, Expr> exprByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private Table targetTable;

    private Database db;
    private long transactionId;

    // we need a new TupleDesc for olap table.
    private TupleDescriptor olapTuple;

    private DataSink dataSink;
    private DataPartition dataPartition;

    List<Column> targetColumns = Lists.newArrayList();

    public InsertStmt(InsertTarget target, List<String> cols, InsertSource source, List<String> hints) {
        this.tblName = target.getTblName();
        List<String> tmpPartitions = target.getPartitions();
        if (tmpPartitions != null) {
            targetPartitions = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            targetPartitions.addAll(tmpPartitions);
        } else {
            targetPartitions = null;
        }
        this.queryStmt = source.getQueryStmt();
        this.planHints = hints;
        targetColumnNames = cols;
    }

    // Ctor for CreateTableAsSelectStmt
    public InsertStmt(TableName name, QueryStmt queryStmt) {
        this.tblName = name;
        this.targetPartitions = null;
        this.targetColumnNames = null;
        this.queryStmt = queryStmt;
        this.planHints = null;
    }

    public TupleDescriptor getOlapTuple() {
        return olapTuple;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public Map<Long, Integer> getIndexIdToSchemaHash() {
        return this.indexIdToSchemaHash;
    }

    public long getTransactionId() {
        return this.transactionId;
    }

    public Boolean isRepartition() {
        return isRepartition;
    }

    public String getDb() {
        return tblName.getDb();
    }

    // TODO(zc): used to get all dbs for lock
    public void getDbs(Analyzer analyzer, Map<String, Database> dbs) throws AnalysisException {
        // get dbs of statement
        queryStmt.getDbs(analyzer, dbs);
        // get db of target table
        tblName.analyze(analyzer);
        String dbName = tblName.getDb();

        // check exist
        Database db = analyzer.getCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check access
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tblName.getDb(), tblName.getTbl(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(), tblName.getTbl());
        }

        dbs.put(dbName, db);
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }


    @Override
    public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
        Preconditions.checkState(isAnalyzed());
        queryStmt.rewriteExprs(rewriter);
    }

    @Override
    public boolean isExplain() {
        return queryStmt.isExplain();
    }

    public boolean isStreaming() {
        return isStreaming;
    }

    // Only valid when this statement is streaming
    public OlapTableSink getOlapTableSink() {
        return (OlapTableSink) dataSink;
    }

    public Database getDbObj() {
        return db;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (targetTable == null) {
            tblName.analyze(analyzer);
        }

        // Check privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tblName.getDb(),
                                                                tblName.getTbl(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(), tblName.getTbl());
        }

        // check partition
        if (targetPartitions != null && targetPartitions.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_ON_NONPARTITIONED);
        }

        // set target table and
        analyzeTargetTable(analyzer);

        analyzeSubquery(analyzer);

        analyzePlanHints(analyzer);

        // create data sink
        createDataSink();

        if (targetTable instanceof OlapTable) {
            String dbName = tblName.getDb();
            // check exist
            db = analyzer.getCatalog().getDb(dbName);
            // although the insert stmt maybe failed at next stage, but has to begin transaction here
            // if get transactionid at add job stage, the transaction id maybe a little larger, it maybe error at alter job to check
            // if all previous job finished
            UUID uuid = UUID.randomUUID();
            String jobLabel = "insert_" + uuid;
            LoadJobSourceType sourceType = LoadJobSourceType.INSERT_STREAMING;
            long timeoutSecond = ConnectContext.get().getSessionVariable().getQueryTimeoutS();
            transactionId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                    jobLabel, "FE: " + FrontendOptions.getLocalHostAddress(), sourceType, timeoutSecond);
            OlapTableSink sink = (OlapTableSink) dataSink;
            TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            sink.init(loadId, transactionId, db.getId());
        }
    }

    private void analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
        // Get table
        if (targetTable == null) {
            targetTable = analyzer.getTable(tblName);
            if (targetTable == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName.getTbl());
            }
        }

        if (targetTable instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) targetTable;

            if (olapTable.getPartitions().size() == 0) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_EMPTY_PARTITION_IN_TABLE, tblName);
            }

            // partition
            if (targetPartitions != null) {
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
                }
                for (String partName : targetPartitions) {
                    Partition part = olapTable.getPartition(partName);
                    if (part == null) {
                        ErrorReport.reportAnalysisException(
                                ErrorCode.ERR_UNKNOWN_PARTITION, partName, targetTable.getName());
                    }
                }
            }
            // need a descriptor
            DescriptorTable descTable = analyzer.getDescTbl();
            olapTuple = descTable.createTupleDescriptor();
            for (Column col : olapTable.getBaseSchema()) {
                SlotDescriptor slotDesc = descTable.addSlotDescriptor(olapTuple);
                slotDesc.setIsMaterialized(true);
                slotDesc.setType(col.getType());
                slotDesc.setColumn(col);
                if (true == col.isAllowNull()) {
                    slotDesc.setIsNullable(true);
                } else {
                    slotDesc.setIsNullable(false);
                }
            }
            // will use it during create load job
            indexIdToSchemaHash = olapTable.getIndexIdToSchemaHash();
        } else if (targetTable instanceof MysqlTable) {
            if (targetPartitions != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
            }
        } else if (targetTable instanceof BrokerTable) {
            if (targetPartitions != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
            }

            BrokerTable brokerTable = (BrokerTable) targetTable;
            List<String> paths = brokerTable.getPaths();

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
            if (mentionedCols.contains(col.getName())) {
                continue;
            }
            if (col.getDefaultValue() == null && !col.isAllowNull()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COL_NOT_MENTIONED, col.getName());
            }
        }
    }

    public void analyzeSubquery(Analyzer analyzer) throws UserException {
        // Analyze columns mentioned in the statement.
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (targetColumnNames == null) {
            for (Column col : targetTable.getBaseSchema()) {
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
        }

        // parse query statement
        queryStmt.setFromInsert(true);
        queryStmt.analyze(analyzer);

        // check if size of select item equal with columns mentioned in statement
        if (mentionedColumns.size() != queryStmt.getResultExprs().size()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_COUNT);
        }

        // Check if all columns mentioned is enough
        checkColumnCoverage(mentionedColumns, targetTable.getBaseSchema()) ;
        if (queryStmt instanceof SelectStmt && ((SelectStmt) queryStmt).getTableRefs().isEmpty()) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (selectStmt.getValueList() != null) {
                List<ArrayList<Expr>> rows = selectStmt.getValueList().getRows();
                for (int rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
                    analyzeRow(analyzer, targetColumns, rows.get(rowIdx), rowIdx + 1);
                }
                for (int i = 0; i < selectStmt.getResultExprs().size(); ++i) {
                    selectStmt.getResultExprs().set(i, selectStmt.getValueList().getFirstRow().get(i));
                    selectStmt.getBaseTblResultExprs().set(i, selectStmt.getValueList().getFirstRow().get(i));
                }
            } else {
                analyzeRow(analyzer, targetColumns, selectStmt.getResultExprs(), 1);
            }
            isStreaming = true;
        } else {
            for (int i = 0; i < targetColumns.size(); ++i) {
                Column column = targetColumns.get(i);
                if (column.getType().isHllType()) {
                    Expr expr = queryStmt.getResultExprs().get(i);
                    checkHllCompatibility(column, expr);
                }
            }
        }
    }

    private void analyzeRow(Analyzer analyzer, List<Column> targetColumns, ArrayList<Expr> row, int rowIdx)
            throws AnalysisException {
        // 1. check number of fields if equal with first row
        if (row.size() != targetColumns.size()) {
            throw new AnalysisException("Column count doesn't match value count at row " + rowIdx);
        }
        for (int i = 0; i < row.size(); ++i) {
            Expr expr = row.get(i);
            Column col = targetColumns.get(i);

            // TargeTable's hll column must be hll_hash's result
            if (col.getType().equals(Type.HLL)) {
                checkHllCompatibility(col, expr);
            }

            if (expr instanceof DefaultValueExpr) {
                if (targetColumns.get(i).getDefaultValue() == null) {
                    throw new AnalysisException("Column has no default value, column=" + targetColumns.get(i).getName());
                }
                expr = new StringLiteral(targetColumns.get(i).getDefaultValue());
            }

            expr.analyze(analyzer);

            row.set(i, checkTypeCompatibility(col, expr));
        }
    }

    private void analyzePlanHints(Analyzer analyzer) throws AnalysisException {
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
            } else if (STREAMING.equalsIgnoreCase(hint)) {
                isStreaming = true;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PLAN_HINT, hint);
            }
        }
    }
    private void checkHllCompatibility(Column col, Expr expr) throws AnalysisException {
        final String hllMismatchLog = "Column's type is HLL,"
                + " SelectList must contains HLL or hll_hash function's result, column=" + col.getName();
        if (expr instanceof SlotRef) {
            final SlotRef slot = (SlotRef) expr;
            if (!slot.getType().equals(Type.HLL)) {
                throw new AnalysisException(hllMismatchLog);
            }
        } else if (expr instanceof FunctionCallExpr) {
            final FunctionCallExpr functionExpr = (FunctionCallExpr) expr;
            if (!functionExpr.getFnName().getFunction().equalsIgnoreCase("hll_hash")) {
                throw new AnalysisException(hllMismatchLog);
            }
        } else {
            throw new AnalysisException(hllMismatchLog);
        }
    }

    private Expr checkTypeCompatibility(Column col, Expr expr) throws AnalysisException {
        if (col.getDataType().equals(expr.getType().getPrimitiveType())) {
            return expr;
        }
        return expr.castTo(col.getType());
    }

    public void prepareExpressions() throws UserException {
        List<Expr> selectList = Expr.cloneList(queryStmt.getBaseTblResultExprs());
        // check type compatibility
        int numCols = targetColumns.size();
        for (int i = 0; i < numCols; ++i) {
            Column col = targetColumns.get(i);
            Expr expr = checkTypeCompatibility(col, selectList.get(i));
            selectList.set(i, expr);
            exprByName.put(col.getName(), expr);
        }
        // reorder resultExprs in table column order
        for (Column col : targetTable.getBaseSchema()) {
            if (exprByName.containsKey(col.getName())) {
                resultExprs.add(exprByName.get(col.getName()));
            } else {
                if (col.getDefaultValue() == null) {
                    /*
                    The import stmt has been filtered in function checkColumnCoverage when
                        the default value of column is null and column is not nullable.
                    So the default value of column may simply be null when column is nullable
                     */
                    Preconditions.checkState(col.isAllowNull());
                    resultExprs.add(NullLiteral.create(col.getType()));
                }
                else {
                    resultExprs.add(checkTypeCompatibility(col, new StringLiteral(col.getDefaultValue())));
                }
            }
        }
    }

    public DataSink createDataSink() throws AnalysisException {
        if (dataSink != null) {
            return dataSink;
        }
        if (targetTable instanceof OlapTable) {
            String partitionNames = targetPartitions == null ? null : Joiner.on(",").join(targetPartitions);
            dataSink = new OlapTableSink((OlapTable) targetTable, olapTuple, partitionNames);
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
        } else {
            dataSink = DataSink.createDataSink(targetTable);
            dataPartition = DataPartition.UNPARTITIONED;
        }
        return dataSink;
    }

    public void finalize() throws UserException {
        if (targetTable instanceof OlapTable) {
            ((OlapTableSink) dataSink).finalize();
        }
    }

    public ArrayList<Expr> getResultExprs() {
        return resultExprs;
    }

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    @Override
    public void reset() {
        super.reset();
        queryStmt.reset();
        resultExprs.clear();
        exprByName.clear();
        dataSink = null;
        dataPartition = null;
        targetColumns.clear();
    }
}
