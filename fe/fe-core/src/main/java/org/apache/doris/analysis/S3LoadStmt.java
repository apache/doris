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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.constants.S3Properties.Env;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * For TVF solution validation.
 */
public class S3LoadStmt extends NativeInsertStmt {

    private static final Logger LOG = LogManager.getLogger(S3LoadStmt.class);

    private final DataDescription dataDescription;

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
        final String sequenceCol = dataDescription.getSequenceCol();
        final ArrayList<OrderByElement> orderByElementList = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(sequenceCol)) {
            final OrderByElement orderByElement = new OrderByElement(
                    new SlotRef(null, sequenceCol),
                    true, null
            );
            orderByElementList.add(orderByElement);
        }


        // merge preceding filter and where expr
        final BoolLiteral trueLiteral = new BoolLiteral(true);
        final Expr whereExpr = Optional.ofNullable(dataDescription.getWhereExpr()).orElse(trueLiteral);
        final Expr precdingFilterExpr =
                Optional.ofNullable(dataDescription.getPrecdingFilterExpr()).orElse(trueLiteral);
        final Expr compoundPredicate = new CompoundPredicate(Operator.AND, precdingFilterExpr, whereExpr);

        final SelectStmt selectStmt = new SelectStmt(
                selectList, fromClause, compoundPredicate,
                null, null,
                orderByElementList, LimitElement.NO_LIMIT
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
    public void convertSemantic(Analyzer analyzer) throws UserException {
        label.analyze(analyzer);
        initTargetTable(analyzer);
        analyzeColumns(analyzer);
    }

    // ------------------------------------ columns mapping ------------------------------------

    private void analyzeColumns(Analyzer analyzer) throws AnalysisException {
        final String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), analyzer);
        dataDescription.analyze(fullDbName);
        // copy a list for analyzing
        List<ImportColumnDesc> columnExprList = Lists.newArrayList(dataDescription.getParsedColumnExprList());
        rewriteExpr(columnExprList);
        boolean isFileFieldSpecified = columnExprList.stream().anyMatch(ImportColumnDesc::isColumn);
        if (!isFileFieldSpecified) {
            return;
        }
        filterColumns(columnExprList);
        resetTargetColumnNames(columnExprList);
        resetSelectList(columnExprList);
    }

    /**
     * find and rewrite the derivative columns
     * e.g. (v1,v2=v1+1,v3=v2+1) --> (v1, v2=v1+1, v3=v1+1+1)
     */
    private void rewriteExpr(List<ImportColumnDesc> columnDescList) {
        Preconditions.checkNotNull(columnDescList, "columns should be not null");
        Preconditions.checkNotNull(targetTable, "target table is unset");
        LOG.info("original columnExpr:{}", columnDescList);
        Map<String, Expr> derivativeColumns = new HashMap<>();
        columnDescList
                .stream()
                .filter(desc -> !desc.isColumn())
                .forEach(desc -> {
                    final Expr expr = desc.getExpr();
                    if (expr instanceof SlotRef) {
                        final String columnName = ((SlotRef) expr).getColumnName();
                        if (derivativeColumns.containsKey(columnName)) {
                            desc.setExpr(derivativeColumns.get(columnName));
                        }
                    } else {
                        recursiveRewrite(expr, derivativeColumns);
                    }
                    derivativeColumns.put(desc.getColumnName(), expr);
                });
        // `tmp` columns with expr can be removed after expr rewritten
        columnDescList.removeIf(
                Predicates.not(columnDesc ->
                        columnDesc.isColumn() || Objects.nonNull(targetTable.getColumn(columnDesc.getColumnName()))
                )
        );
        LOG.info("rewrite result:{}", columnDescList);
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
        Preconditions.checkNotNull(targetTable, "target table is unset");
        // remove all `tmp` columns, which are not in target table
        columnExprList.removeIf(
                Predicates.and(ImportColumnDesc::isColumn,
                        columnDesc -> Objects.isNull(targetTable.getColumn(columnDesc.getColumnName())))
        );
        Map<String, Expr> columnExprMap = columnExprList.stream()
                // do not use Collector.toMap because ImportColumnDesc::getExpr may be null
                .collect(() -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER),
                        (map, desc) -> map.put(desc.getColumnName(), desc.getExpr()), TreeMap::putAll);
        checkUnspecifiedCols(columnExprMap);
        addSchemaChangeShadowCols(columnExprList, columnExprMap);
        LOG.info("filtered result:{}", columnExprList);
    }

    /**
     * unspecified columns must have default val
     */
    private void checkUnspecifiedCols(Map<String, Expr> columnExprMap)
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
                .filter(column -> column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX))
                .forEach(column -> {
                    String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX);
                    if (columnExprMap.containsKey(originCol)) {
                        Expr mappingExpr = columnExprMap.get(originCol);
                        ImportColumnDesc importColumnDesc = Optional.ofNullable(mappingExpr)
                                .map(
                                        /*
                                         * eg:
                                         * (A, C) SET (B = func(xx))
                                         * ->
                                         * (A, C) SET (B = func(xx), __doris_shadow_B = func(xx))
                                         */
                                        expr -> new ImportColumnDesc(column.getName(), expr)
                                )
                                .orElse(
                                        /*
                                         * eg:
                                         * (A, B, C)
                                         * ->
                                         * (A, B, C) SET (__doris_shadow_B = B)
                                         */
                                        new ImportColumnDesc(column.getName(), new SlotRef(null, originCol))
                                );
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
        targetColumnNames = columnExprList
                .stream()
                .map(ImportColumnDesc::getColumnName)
                .collect(Collectors.toList());
        LOG.info("target cols:{}", targetColumnNames);
    }

    private void resetSelectList(List<ImportColumnDesc> columnExprList) {
        LOG.info("select list:{}", columnExprList);
        final SelectList selectList = new SelectList();
        columnExprList.forEach(desc -> {
            if (desc.isColumn()) {
                selectList.addItem(new SelectListItem(new SlotRef(null, desc.getColumnName()), null));
            } else {
                selectList.addItem(new SelectListItem(desc.getExpr(), desc.getColumnName()));
            }
        });
        ((SelectStmt) getQueryStmt()).resetSelectList(selectList);
    }

    // -----------------------------------------------------------------------------------------
}
