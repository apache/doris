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
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.constants.S3Properties.Env;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * For TVF solution validation.
 */
public class S3LoadStmt extends NativeInsertStmt {

    private final DataDescription dataDescription;

    private boolean isFileFieldSpecified;

    public S3LoadStmt(LabelName label, List<DataDescription> dataDescList, BrokerDesc brokerDesc,
            Map<String, String> properties, String comments) throws DdlException {
        super(buildInsertTarget(dataDescList.get(0)),
                label.getLabelName(), /*insert into all columns by default*/null,
                buildInsertSource(dataDescList.get(0), brokerDesc), null);
        this.label = label;
        this.dataDescription = dataDescList.get(0);
        this.properties = properties;
        this.comments = comments;
    }

    // ------------------------------------ init helpers ------------------------------------

    private static InsertTarget buildInsertTarget(DataDescription dataDescription) {
        final TableName tableName = new TableName(null, null, dataDescription.getTableName());
        return new InsertTarget(tableName, dataDescription.getPartitionNames());
    }

    private static InsertSource buildInsertSource(DataDescription dataDescription, BrokerDesc brokerDesc)
            throws DdlException {
        final SelectList selectList = new SelectList();
        // use `select *` by default
        final SelectListItem item = new SelectListItem(SelectListItem.createStarItem(null));
        selectList.addItem(item);

        // build from
        final FromClause fromClause = new FromClause(
                Collections.singletonList(buildTvfRef(dataDescription, brokerDesc))
        );
        // build order by
        final TableName tableName = new TableName(null, null, dataDescription.getTableName());
        final OrderByElement orderByElement = new OrderByElement(
                new SlotRef(tableName, dataDescription.getSequenceCol()),
                true, null
        );

        // merge preceding filter and where expr
        final Expr whereExpr = dataDescription.getWhereExpr();
        final Expr precdingFilterExpr = dataDescription.getPrecdingFilterExpr();
        final Expr compoundPredicate = new CompoundPredicate(Operator.AND, precdingFilterExpr, whereExpr);

        final SelectStmt selectStmt = new SelectStmt(
                selectList, fromClause, compoundPredicate,
                null, null,
                Lists.newArrayList(orderByElement), LimitElement.NO_LIMIT
        );
        return new InsertSource(selectStmt);
    }

    private static TableRef buildTvfRef(DataDescription dataDescription, BrokerDesc brokerDesc) throws DdlException {
        final Map<String, String> params = Maps.newHashMap();

        final List<String> filePaths = dataDescription.getFilePaths();
        Preconditions.checkState(filePaths.size() == 1, "there should be only one file path");
        final String s3FilePath = filePaths.get(0);
        params.put(S3TableValuedFunction.S3_URI, s3FilePath);

        final Map<String, String> dataDescProp = dataDescription.getProperties();
        if (dataDescProp != null) {
            params.putAll(dataDescProp);
        }

        params.put(ExternalFileTableValuedFunction.FORMAT, dataDescription.getFileFormat());
        params.put(ExternalFileTableValuedFunction.COLUMN_SEPARATOR, dataDescription.getColumnSeparator());

        Preconditions.checkState(!brokerDesc.isMultiLoadBroker(), "do not support multi broker load currently");
        Preconditions.checkState(brokerDesc.getStorageType() == StorageType.S3, "only support S3 load");

        final Map<String, String> s3ResourceProp = brokerDesc.getProperties();
        S3Properties.convertToStdProperties(s3ResourceProp);
        s3ResourceProp.keySet().removeIf(Env.FS_KEYS::contains);
        params.putAll(s3ResourceProp);

        try {
            return new TableValuedFunctionRef(S3TableValuedFunction.NAME, null, params);
        } catch (AnalysisException e) {
            throw new DdlException("failed to create s3 tvf ref", e);
        }
    }

    // --------------------------------------------------------------------------------------

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        analyzeColumns(analyzer);
        super.analyze(analyzer);
    }

    // ------------------------------------ columns mapping ------------------------------------

    private void analyzeColumns(Analyzer analyzer) throws AnalysisException {
        final String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), analyzer);
        dataDescription.analyze(fullDbName);
        List<ImportColumnDesc> columnExprList = dataDescription.getParsedColumnExprList();
        rewriteColumns(columnExprList);
        filterColumns(columnExprList);
        if (isFileFieldSpecified) {
            resetTargetColumnNames(columnExprList);
            resetSelectList(columnExprList);
        }
    }

    /**
     * find and rewrite the derivative columns
     * e.g. (v1,v2=v1+1,v3=v2+1) --> (v1, v2=v1+1, v3=v1+1+1)
     */
    private void rewriteColumns(List<ImportColumnDesc> columnDescList) {
        Preconditions.checkNotNull(columnDescList, "columns should be not null");
        Map<String, Expr> derivativeColumns = new HashMap<>();
        columnDescList
                .stream()
                .filter(desc -> !desc.isColumn())
                .forEach(desc -> {
                    final Expr expr = desc.getExpr();
                    if (expr instanceof SlotRef) {
                        final String columnName = ((SlotRef) expr).getColumnName();
                        derivativeColumns.computeIfPresent(columnName, (n, e) -> {
                            desc.setExpr(e);
                            return e;
                        });
                    } else {
                        recursiveRewrite(expr, derivativeColumns);
                    }
                    derivativeColumns.put(desc.getColumnName(), expr);
                });
    }

    private void recursiveRewrite(Expr expr, Map<String, Expr> derivativeColumns) {
        final ArrayList<Expr> children = expr.getChildren();
        if (CollectionUtils.isEmpty(children)) {
            return;
        }
        for (int i = 0; i < children.size(); i++) {
            Expr child = expr.getChild(i);
            if (child instanceof SlotRef) {
                final String columnName = ((SlotRef) child).getColumnName();
                if (derivativeColumns.containsKey(columnName)) {
                    expr.setChild(i, derivativeColumns.get(columnName));
                }
                continue;
            }
            recursiveRewrite(child, derivativeColumns);
        }
    }

    private void filterColumns(List<ImportColumnDesc> columnExprList) throws AnalysisException {
        Preconditions.checkNotNull(targetTable, "target table should be not null");
        columnExprList.removeIf(
                Predicates.not(columnDesc ->
                        columnDesc.isColumn() || Objects.nonNull(targetTable.getColumn(columnDesc.getColumnName()))
                )
        );
        // if isFileFieldSpecified = false, `columnDesc with expr` must not exist
        isFileFieldSpecified = columnExprList.stream().anyMatch(ImportColumnDesc::isColumn);
        if (!isFileFieldSpecified) {
            // If user does not specify the file field names, generate it by using base schema of table.
            // So that the following process can be unified
            fillWithSchemaCols(columnExprList);
        }
        Map<String, Expr> columnExprMap = columnExprList.stream()
                .collect(Collectors.toMap(ImportColumnDesc::getColumnName, ImportColumnDesc::getExpr,
                        (expr1, expr2) -> expr1, () -> Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER)));
        checkUnspecifiedCols(columnExprList, columnExprMap);
        addSchemaChangeShadowCols(columnExprList, columnExprMap);
    }

    private void fillWithSchemaCols(List<ImportColumnDesc> columnDescList) {
        final List<Column> columns = targetTable.getBaseSchema(false);
        boolean hasSequenceCol = (targetTable instanceof OlapTable && ((OlapTable) targetTable).hasSequenceCol());
        for (Column column : columns) {
            if (hasSequenceCol && column.isSequenceColumn()) {
                // columnExprs has sequence column, don't need to generate the sequence column
                continue;
            }
            ImportColumnDesc columnDesc;
            if (StringUtils.equals(dataDescription.getFileFormat(), "json")) {
                columnDesc = new ImportColumnDesc(column.getName());
            } else {
                columnDesc = new ImportColumnDesc(column.getName().toLowerCase());
            }
            columnDescList.add(columnDesc);
        }
    }

    /**
     * unspecified columns must have default val
     */
    private void checkUnspecifiedCols(List<ImportColumnDesc> columnExprList, Map<String, Expr> columnExprMap)
            throws AnalysisException {

        final Optional<Column> colWithoutDefaultVal = targetTable.getBaseSchema()
                .stream()
                .filter(column -> !columnExprMap.containsKey(column.getName()))
                .filter(Predicates.not(column -> Objects.nonNull(column.getDefaultValue()) || column.isAllowNull()))
                .findFirst();
        if (colWithoutDefaultVal.isPresent()) {
            final String columnName = colWithoutDefaultVal.get().getName();
            throw new AnalysisException("Column has no default value. column: " + columnName);
        }
    }

    /**
     * When doing schema change, there may have some 'shadow' columns, with prefix '__doris_shadow_' in
     * their names. These columns are invisible to user, but we need to generate data for these columns.
     * So we add column mappings for these column.
     * eg1:
     * base schema is (A, B, C), and B is under schema change, so there will be a shadow column: '__doris_shadow_B'
     * So the final column mapping should looks like: (A, B, C, __doris_shadow_B = substitute(B));
     */
    private void addSchemaChangeShadowCols(List<ImportColumnDesc> columnExprList, Map<String, Expr> columnExprMap) {
        List<ImportColumnDesc> shadowColumnDescs = Lists.newArrayList();
        targetTable.getFullSchema()
                .stream()
                .filter(column -> !column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX))
                .forEach(column -> {
                    String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX);
                    if (columnExprMap.containsKey(originCol)) {
                        Expr mappingExpr = columnExprMap.get(originCol);
                        ImportColumnDesc importColumnDesc;
                        if (mappingExpr != null) {
                            /*
                             * eg:
                             * (A, C) SET (B = func(xx))
                             * ->
                             * (A, C) SET (B = func(xx), __doris_shadow_B = func(xx))
                             */
                            importColumnDesc = new ImportColumnDesc(column.getName(), mappingExpr);
                        } else {
                            /*
                             * eg:
                             * (A, B, C)
                             * ->
                             * (A, B, C) SET (__doris_shadow_B = B)
                             */
                            importColumnDesc = new ImportColumnDesc(column.getName(),
                                    new SlotRef(null, originCol));
                        }
                        shadowColumnDescs.add(importColumnDesc);
                    } else {
                        /*
                         * There is a case that if user does not specify the related origin column, eg:
                         * COLUMNS (A, C), and B is not specified, but B is being modified
                         * so there is a shadow column '__doris_shadow_B'.
                         * We can not just add a mapping function "__doris_shadow_B = substitute(B)",
                         * because Doris can not find column B.
                         * In this case, __doris_shadow_B can use its default value,
                         * so no need to add it to column mapping.
                         */
                        // do nothing
                    }
                });
        columnExprList.addAll(shadowColumnDescs);
    }

    private void resetTargetColumnNames(List<ImportColumnDesc> columnExprList) {
        final Set<String> schemaColNameSet = targetTable.getFullSchema()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toSet());

        targetColumnNames = columnExprList
                .stream()
                .map(ImportColumnDesc::getColumnName)
                .filter(schemaColNameSet::contains)
                .collect(Collectors.toList());
    }

    private void resetSelectList(List<ImportColumnDesc> columnExprList) {
        final SelectList selectList = new SelectList();
        columnExprList.forEach(desc -> {
            if (desc.isColumn()) {
                selectList.addItem(new SelectListItem(new SlotRef(null, desc.getColumnName()), null));
            } else {
                // use expr as select item and colName as alias
                selectList.addItem(new SelectListItem(desc.getExpr(), desc.getColumnName()));
            }
        });
        ((SelectStmt) getQueryStmt()).resetSelectList(selectList);
    }

    // -----------------------------------------------------------------------------------------
}
