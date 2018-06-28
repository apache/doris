// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.BrokerTable;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MysqlTable;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PartitionType;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.planner.DataPartition;
import com.baidu.palo.planner.DataSink;
import com.baidu.palo.planner.DataSplitSink;
import com.baidu.palo.planner.ExportSink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// InsertStmt used to
public class InsertStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(InsertStmt.class);

    public static final String SHUFFLE_HINT = "SHUFFLE";
    public static final String NOSHUFFLE_HINT = "NOSHUFFLE";

    private final TableName tblName;
    private final Set<String> targetPartitions;
    private final List<String> targetColumnNames;
    private final QueryStmt queryStmt;
    private final List<String> planHints;
    private Boolean isRepartition;

    // set after parse all columns and expr in query statement
    // this result expr in the order of target table's columns
    private ArrayList<Expr> resultExprs = Lists.newArrayList();

    private Map<String, Expr> exprByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private Table targetTable;

    // we need a new TupleDesc for olap table.
    private TupleDescriptor olapTuple;

    private DataSink dataSink;
    private DataPartition dataPartition;

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
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), dbName, AccessPrivilege.READ_WRITE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getUser(), dbName);
        }

        dbs.put(dbName, db);
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);

        // Check privilege
        if (targetTable == null) {
            tblName.analyze(analyzer);
            analyzer.checkPrivilege(tblName.getDb(), AccessPrivilege.READ_WRITE);
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
        LOG.info("analyzer is ", analyzer.getDescTbl().debugString());
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

    private void checkColumnCoverage(Set<String> mentionedCols, List<Column> baseColumns, List<Expr> selectList)
            throws AnalysisException {
        // check if size of select item equal with columns mentioned in statement
        if (mentionedCols.size() != selectList.size()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_COUNT);
        }

        // check columns of target table
        for (Column col : baseColumns) {
            if (mentionedCols.contains(col.getName())) {
                continue;
            }
            if (col.getDefaultValue() == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COL_NOT_MENTIONED, col.getName());
            }
        }
    }

    public void analyzeSubquery(Analyzer analyzer) throws AnalysisException, InternalException {
        // parse query statement
        queryStmt.analyze(analyzer);
        List<Expr> selectList = Expr.cloneList(queryStmt.getBaseTblResultExprs());

        // Analyze columns mentioned in the statement.
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<Column> targetColumns = Lists.newArrayList();
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

        // Check if all columns mentioned is enough
        checkColumnCoverage(mentionedColumns, targetTable.getBaseSchema(), selectList);

        prepareExpressions(targetColumns, selectList, analyzer);
    }

    private void analyzePlanHints(Analyzer analyzer) throws AnalysisException {
        if (planHints == null) {
            return;
        }
        if (!planHints.isEmpty() && !targetTable.isPartitioned()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INSERT_HINT_NOT_SUPPORT);
        }
        for (String hint : planHints) {
            if (SHUFFLE_HINT.equalsIgnoreCase(hint)) {
                if (isRepartition != null && !isRepartition) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PLAN_HINT_CONFILT, hint);
                }
                isRepartition = Boolean.TRUE;
            } else if (NOSHUFFLE_HINT.equalsIgnoreCase(hint)) {
                if (isRepartition != null && isRepartition) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_PLAN_HINT_CONFILT, hint);
                }
                isRepartition = Boolean.FALSE;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PLAN_HINT, hint);
            }
        }
    }

    private Expr checkTypeCompatibility(Column col, Expr expr) throws AnalysisException {
        if (col.getDataType().equals(expr.getType())) {
            return expr;
        }
        return expr.castTo(col.getType());
    }

    private void prepareExpressions(List<Column> targetCols, List<Expr> selectList, Analyzer analyzer)
            throws AnalysisException {
        // check type compatibility
        int numCols = targetCols.size();
        for (int i = 0; i < numCols; ++i) {
            Column col = targetCols.get(i);
            Expr expr = checkTypeCompatibility(col, selectList.get(i));
            selectList.set(i, expr);
            exprByName.put(col.getName(), expr);
        }
        // reorder resultExprs in table column order
        for (Column col : targetTable.getBaseSchema()) {
            if (exprByName.containsKey(col.getName())) {
                resultExprs.add(exprByName.get(col.getName()));
            } else {
                resultExprs.add(checkTypeCompatibility(col, new StringLiteral(col.getDefaultValue())));
            }
        }
    }

    public DataSink createDataSink() throws AnalysisException {
        if (dataSink != null) {
            return dataSink;
        }
        if (targetTable instanceof OlapTable) {
            dataSink = new DataSplitSink((OlapTable) targetTable, olapTuple);
            dataPartition = dataSink.getOutputPartition();
        } else if (targetTable instanceof BrokerTable) {
            BrokerTable table = (BrokerTable) targetTable;
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
    }
}
