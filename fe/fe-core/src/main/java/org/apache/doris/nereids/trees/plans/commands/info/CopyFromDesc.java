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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.CopyFromParam;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StageAndPattern;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * copy from desc ==> copy from param
 */
public class CopyFromDesc {
    private static final Logger LOG = LogManager.getLogger(CopyFromParam.class);
    private static final String DOLLAR = "$";
    private StageAndPattern stageAndPattern;
    private List<NamedExpression> exprList;
    private Optional<Expression> fileFilterExpr;
    private List<String> fileColumns;
    private List<Expression> columnMappingList;
    private List<String> targetColumns;

    public CopyFromDesc(StageAndPattern stageAndPattern) {
        this.stageAndPattern = stageAndPattern;
    }

    public CopyFromDesc(StageAndPattern stageAndPattern, List<NamedExpression> exprList,
                        Optional<Expression> whereExpr) {
        this.stageAndPattern = stageAndPattern;
        this.exprList = exprList;
        this.fileFilterExpr = whereExpr;
    }

    public void setTargetColumns(List<String> targetColumns) {
        this.targetColumns = targetColumns;
    }

    public StageAndPattern getStageAndPattern() {
        return stageAndPattern;
    }

    public List<Expression> getColumnMappingList() {
        return columnMappingList;
    }

    public List<NamedExpression> getExprList() {
        return exprList;
    }

    public List<String> getFileColumns() {
        return fileColumns;
    }

    public Optional<Expression> getFileFilterExpr() {
        return fileFilterExpr;
    }

    public List<String> getTargetColumns() {
        return targetColumns;
    }

    /**
     * analyze
     */
    public void validate(String fullDbName, TableName tableName, boolean useDeleteSign, String fileType)
            throws AnalysisException {
        if (exprList == null && fileFilterExpr == null && !useDeleteSign) {
            return;
        }
        this.fileColumns = new ArrayList<>();
        this.columnMappingList = new ArrayList<>();

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(fullDbName);
        OlapTable olapTable = db.getOlapTableOrAnalysisException(tableName.getTbl());

        if (useDeleteSign && olapTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("copy.use_delete_sign property only support unique table");
        }

        // Analyze table columns mentioned in the statement.
        boolean addDeleteSign = false;
        if (targetColumns == null) {
            targetColumns = new ArrayList<>();
            for (Column col : olapTable.getBaseSchema(false)) {
                targetColumns.add(col.getName());
            }
            if (useDeleteSign) {
                targetColumns.add(getDeleteSignColumn(olapTable).getName());
                addDeleteSign = true;
            }
        } else {
            if (exprList == null || targetColumns.size() != exprList.size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_COUNT);
            }
            Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (String colName : targetColumns) {
                Column col = olapTable.getColumn(colName);
                if (col == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName, olapTable.getName());
                }
                if (!mentionedColumns.add(colName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_FIELD_SPECIFIED_TWICE, colName);
                }
            }
            if (useDeleteSign && !mentionedColumns.contains(Column.DELETE_SIGN)) {
                targetColumns.add(getDeleteSignColumn(olapTable).getName());
                addDeleteSign = true;
            }
        }

        if (!getFileColumnNames(addDeleteSign)) {
            if (fileType == null || fileType.equalsIgnoreCase("csv")) {
                int maxFileColumnId = getMaxFileColumnId();
                if (useDeleteSign && exprList == null && fileColumns.isEmpty()) {
                    maxFileColumnId = targetColumns.size() > maxFileColumnId ? targetColumns.size() : maxFileColumnId;
                }
                for (int i = 1; i <= maxFileColumnId; i++) {
                    fileColumns.add(DOLLAR + i);
                }
            } else {
                for (Column col : olapTable.getBaseSchema(false)) {
                    fileColumns.add(col.getName());
                }
                if (useDeleteSign) {
                    fileColumns.add(getDeleteSignColumn(olapTable).getName());
                }
            }
        }

        parseColumnNames(fileType, targetColumns);
        parseColumnNames(fileType, fileColumns);

        if (exprList != null) {
            if (!(exprList.size() == 1 && exprList.get(0) instanceof UnboundStar)) {
                if (targetColumns.size() != exprList.size()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_COUNT);
                }
                for (int i = 0; i < targetColumns.size(); i++) {
                    Expression expr = exprList.get(i);
                    EqualTo binaryPredicate = new EqualTo(new UnboundSlot(targetColumns.get(i)), expr);
                    columnMappingList.add(binaryPredicate);
                }
            }
        } else {
            for (int i = 0; i < targetColumns.size(); i++) {
                // If file column name equals target column name, don't need to add to
                // columnMapping List. Otherwise it will result in invalidation of strict
                // mode. Because if the src data is an expr, strict mode judgment will
                // not be performed.
                if (!fileColumns.get(i).equalsIgnoreCase(targetColumns.get(i))) {
                    EqualTo binaryPredicate = new EqualTo(new UnboundSlot(targetColumns.get(i)),
                            new UnboundSlot(fileColumns.get(i)));
                    columnMappingList.add(binaryPredicate);
                }
            }
        }
    }

    // expr use column name
    private boolean getFileColumnNames(boolean addDeleteSign) throws AnalysisException {
        if (exprList == null) {
            return false;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        //        Expr.collectList(exprList, SlotRef.class, slotRefs);
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SlotRef slotRef : slotRefs) {
            String columnName = slotRef.getColumnName();
            if (columnName.startsWith(DOLLAR)) {
                if (fileColumns.size() > 0) {
                    throw new AnalysisException("can not mix column name and dollar sign");
                }
                return false;
            }
            if (columnSet.add(columnName)) {
                fileColumns.add(columnName);
            }
        }
        if (addDeleteSign) {
            //            exprList.add(new SlotRef(null, Column.DELETE_SIGN));
            fileColumns.add(Column.DELETE_SIGN);
        }
        return true;
    }

    private Column getDeleteSignColumn(OlapTable olapTable) throws AnalysisException {
        for (Column column : olapTable.getFullSchema()) {
            if (column.isDeleteSignColumn()) {
                return column;
            }
        }
        throw new AnalysisException("Can not find DeleteSignColumn for unique table");
    }

    private int getMaxFileColumnId() throws AnalysisException {
        int maxId = 0;
        if (exprList != null) {
            int maxFileColumnId = getMaxFileFilterColumnId(
                    exprList.stream().map(expr -> (Expression) expr).collect(Collectors.toList()));
            maxId = maxId > maxFileColumnId ? maxId : maxFileColumnId;
        }
        if (fileFilterExpr != null) {
            int maxFileColumnId = getMaxFileFilterColumnId(Lists.newArrayList(fileFilterExpr.get()));
            maxId = maxId > maxFileColumnId ? maxId : maxFileColumnId;
        }
        return maxId;
    }

    private int getMaxFileFilterColumnId(List<Expression> exprList) throws AnalysisException {
        Set<Slot> slots = ExpressionUtils.getInputSlotSet(exprList);
        int maxId = 0;
        for (Slot slot : slots) {
            int fileColumnId = getFileColumnIdOfSlotRef((UnboundSlot) slot);
            maxId = fileColumnId < maxId ? maxId : fileColumnId;
        }
        return maxId;
    }

    private int getFileColumnIdOfSlotRef(UnboundSlot unboundSlot) throws AnalysisException {
        String columnName = unboundSlot.getName();
        try {
            if (!columnName.startsWith(DOLLAR)) {
                throw new AnalysisException("can not mix column name and dollar sign");
            }
            return Integer.parseInt(columnName.substring(1));
        } catch (NumberFormatException e) {
            throw new AnalysisException("column name: " + columnName + " can not parse to a number");
        }
    }

    private void parseColumnNames(String fileType, List<String> columns) {
        // In Be, parquet and orc column names are case-insensitive, but there is a bug for hidden columns,
        // so handle it temporary.
        if (columns != null && fileType != null && (fileType.equalsIgnoreCase("parquet") || fileType.equalsIgnoreCase(
                "orc"))) {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).equals(Column.DELETE_SIGN)) {
                    columns.set(i, Column.DELETE_SIGN.toLowerCase());
                }
            }
        }
    }
}
