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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * For TVF solution validation.
 */
public class S3LoadStmt extends NativeInsertStmt {

    private static final Logger LOG = LogManager.getLogger(S3LoadStmt.class);

    private static final String FORMAT_CSV = "csv";

    private static final String DEFAULT_FORMAT = FORMAT_CSV;

    private final DataDescription dataDescription;

    /**
     * we need some particular process
     */
    private final boolean isCsvFormat;

    /**
     * only used for loading from csv format tvf
     * with mapping from col name to csv-format-tvf-style col name (c1, c2, c3...)
     */
    private Map<String, String> selectColNameToCsvColName;

    public S3LoadStmt(LabelName label, List<DataDescription> dataDescList, BrokerDesc brokerDesc,
            Map<String, String> properties, String comments) throws DdlException {
        super(buildInsertTarget(dataDescList.get(0)),
                label.getLabelName(), /*insert into all columns by default*/null,
                buildInsertSource(dataDescList.get(0), brokerDesc), null);
        this.label = label;
        this.dataDescription = dataDescList.get(0);
        this.properties = properties;
        this.comments = comments;
        this.isCsvFormat = isCsvFormat(dataDescription.getFileFormat());
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

        final String format = Optional.ofNullable(dataDescription.getFileFormat()).orElse(DEFAULT_FORMAT);
        params.put(ExternalFileTableValuedFunction.FORMAT, format);
        if (isCsvFormat(format)) {
            final Separator separator = dataDescription.getColumnSeparatorObj();
            if (separator != null) {
                try {
                    separator.analyze();
                } catch (AnalysisException e) {
                    throw new DdlException("failed to create s3 tvf ref", e);
                }
                params.put(ExternalFileTableValuedFunction.COLUMN_SEPARATOR, dataDescription.getColumnSeparator());
            }
        }

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

    private static boolean isCsvFormat(String format) {
        return Strings.isNullOrEmpty(format) || StringUtils.equalsIgnoreCase(format, FORMAT_CSV);
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
        columnExprList = filterColumns(columnExprList);
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
        Map<String, Expr> derivativeColumns = Maps.newHashMap();
        columnDescList
                .stream()
                .filter(Predicates.not(ImportColumnDesc::isColumn))
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
        if (isCsvFormat) {
            // in tvf, csv format column names are like "c1, c2, c3", record for correctness of select list
            recordCsvColNames(columnDescList);
        }
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

    /**
     * record mapping from col name to csv-format-tvf-style col name
     *
     * @see selectColNameToCsvColName
     */
    private void recordCsvColNames(List<ImportColumnDesc> columnDescList) {
        AtomicInteger counter = new AtomicInteger(1);
        selectColNameToCsvColName = columnDescList.stream()
                .filter(ImportColumnDesc::isColumn)
                .collect(Collectors.toMap(
                        ImportColumnDesc::getColumnName,
                        name -> "c" + counter.getAndIncrement(),
                        (v1, v2) -> v1,
                        LinkedHashMap::new
                ));
        LOG.info("select column name to csv colum name:{}", selectColNameToCsvColName);
    }

    private List<ImportColumnDesc> filterColumns(List<ImportColumnDesc> columnExprList) throws AnalysisException {
        Preconditions.checkNotNull(targetTable, "target table is unset");
        // remove all `tmp` columns, which are not in target table
        columnExprList.removeIf(
                Predicates.and(ImportColumnDesc::isColumn,
                        columnDesc -> Objects.isNull(targetTable.getColumn(columnDesc.getColumnName())))
        );

        // to deal with the case like:
        // (k1, k2) SET(k1 = `upper(k1)`)
        columnExprList = Lists.newArrayList(columnExprList.stream()
                .collect(Collectors.toMap(
                        ImportColumnDesc::getColumnName,
                        Function.identity(),
                        (lhs, rhs) -> {
                            if (lhs.getExpr() != null && rhs.getExpr() == null) {
                                return lhs;
                            } else if (lhs.getExpr() == null && rhs.getExpr() != null) {
                                return rhs;
                            } else {
                                throw new IllegalArgumentException(
                                        String.format("column `%s` specified twice", lhs.getColumnName()));
                            }
                        }
                )).values());

        Map<String, Expr> columnExprMap = columnExprList.stream()
                // do not use Collector.toMap because ImportColumnDesc::getExpr may be null
                .collect(() -> Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER),
                        (map, desc) -> map.put(desc.getColumnName(), desc.getExpr()), TreeMap::putAll);
        checkUnspecifiedCols(columnExprMap);
        LOG.info("filtered result:{}", columnExprList);
        return columnExprList;
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

    private void resetTargetColumnNames(List<ImportColumnDesc> columnExprList) {
        targetColumnNames = columnExprList
                .stream()
                .map(ImportColumnDesc::getColumnName)
                .collect(Collectors.toList());
        LOG.info("target cols:{}", targetColumnNames);
    }

    private void resetSelectList(List<ImportColumnDesc> columnExprList) {
        if (isCsvFormat) {
            rewriteExprColNameToCsvStyle(columnExprList);
        }
        LOG.info("select list:{}", columnExprList);
        final SelectList selectList = new SelectList();
        columnExprList.forEach(desc -> {
            if (desc.isColumn()) {
                if (isCsvFormat) {
                    // use csv-style-column name and target column name as alias
                    final Expr slotRef = new SlotRef(null, selectColNameToCsvColName.get(desc.getColumnName()));
                    selectList.addItem(new SelectListItem(slotRef, desc.getColumnName()));
                } else {
                    selectList.addItem(new SelectListItem(new SlotRef(null, desc.getColumnName()), null));
                }
            } else {
                selectList.addItem(new SelectListItem(desc.getExpr(), desc.getColumnName()));
            }
        });
        ((SelectStmt) getQueryStmt()).resetSelectList(selectList);
    }

    private void rewriteExprColNameToCsvStyle(List<ImportColumnDesc> columnExprList) {
        Preconditions.checkNotNull(selectColNameToCsvColName,
                "SelectColName To CsvColName is not recorded");
        columnExprList
                .stream()
                .filter(Predicates.not(ImportColumnDesc::isColumn))
                .forEach(desc -> rewriteSlotRefInExpr(desc.getExpr()));

        // rewrite where predicate and order by elements
        final SelectStmt selectStmt = (SelectStmt) getQueryStmt();
        rewriteSlotRefInExpr(selectStmt.getWhereClause());
        selectStmt.getOrderByElements().forEach(orderByElement -> rewriteSlotRefInExpr(orderByElement.getExpr()));
    }

    private void rewriteSlotRefInExpr(Expr expr) {
        final Predicate<? super Expr> rewritePredicate = e -> e instanceof SlotRef
                && selectColNameToCsvColName.containsKey(((SlotRef) e).getColumnName());
        final Consumer<? super Expr> rewriteOperation = e -> {
            final SlotRef slotRef = (SlotRef) e;
            slotRef.setCol(selectColNameToCsvColName.get(slotRef.getColumnName()));
        };

        List<Expr> slotRefs = Lists.newArrayList();
        expr.collect(rewritePredicate, slotRefs);
        slotRefs.forEach(rewriteOperation);
    }

    // -----------------------------------------------------------------------------------------
}
