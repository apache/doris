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

import org.apache.doris.analysis.BulkLoadDataDesc;
import org.apache.doris.analysis.BulkStorageDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryStateException;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.HdfsTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * export table
 */
public class LoadCommand extends Command implements ForwardWithSync, Explainable {
    public static final Logger LOG = LogManager.getLogger(LoadCommand.class);
    private final String labelName;
    private final BulkStorageDesc bulkStorageDesc;
    private final List<BulkLoadDataDesc> sourceInfos;
    private final Map<String, String> properties;
    private final String comment;
    private final List<LogicalPlan> plans = new ArrayList<>();
    private Profile profile;

    /**
     * constructor of ExportCommand
     */
    public LoadCommand(String labelName, List<BulkLoadDataDesc> sourceInfos, BulkStorageDesc bulkStorageDesc,
                       Map<String, String> properties, String comment) {
        super(PlanType.LOAD_COMMAND);
        this.labelName = Objects.requireNonNull(labelName.trim(), "labelName should not null");
        this.sourceInfos = Objects.requireNonNull(sourceInfos, "sourceInfos should not null");
        this.properties = Objects.requireNonNull(properties, "properties should not null");
        this.bulkStorageDesc = Objects.requireNonNull(bulkStorageDesc, "bulkStorageDesc should not null");
        this.comment = Objects.requireNonNull(comment, "comment should not null");
    }

    /**
     * for test print
     * @param ctx context
     * @return parsed insert into plan
     */
    @VisibleForTesting
    public List<LogicalPlan> parseToInsertIntoPlan(ConnectContext ctx) throws AnalysisException {
        List<LogicalPlan> plans = new ArrayList<>();
        for (BulkLoadDataDesc dataDesc : sourceInfos) {
            ctx.getState().setNereids(true);
            plans.add(completeQueryPlan(ctx, dataDesc));
        }
        return plans;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        this.profile = new Profile("Query", ctx.getSessionVariable().enableProfile);
        // TODO: begin txn form multi insert sql
        profile.getSummaryProfile().setQueryBeginTime();
        for (BulkLoadDataDesc dataDesc : sourceInfos) {
            ctx.getState().setNereids(true);
            plans.add(new InsertIntoTableCommand(completeQueryPlan(ctx, dataDesc), Optional.of(labelName), false));
        }
        profile.getSummaryProfile().setQueryPlanFinishTime();
        executeInsertStmtPlan(ctx, executor, plans);
    }

    private LogicalPlan completeQueryPlan(ConnectContext ctx, BulkLoadDataDesc dataDesc)
            throws AnalysisException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("nereids load stmt before conversion: {}", dataDesc.toSql());
        }
        // build source columns, add select and insert node.
        List<String> sinkCols = new ArrayList<>();
        // contains the map of olap column to FileFieldNames, ColumnMappingList, ColumnsFromPath.
        Map<String, NamedExpression> originSinkToSourceMap = new LinkedHashMap<>();
        OlapTable olapTable = getOlapTable(ctx, dataDesc);
        buildSinkColumns(olapTable, dataDesc, sinkCols, originSinkToSourceMap);

        Map<String, String> properties = getTvfProperties(dataDesc, bulkStorageDesc);
        // mapping sink colum to mapping expression
        if (!dataDesc.getColumnMappingList().isEmpty()) {
            for (Expression mappingExpr : dataDesc.getColumnMappingList()) {
                if (!(mappingExpr instanceof EqualTo)) {
                    continue;
                }
                String mappingColumn = ((EqualTo) mappingExpr).left().toSql();
                if (originSinkToSourceMap.containsKey(mappingColumn)) {
                    Expression mappingExprAlias = ((EqualTo) mappingExpr).right();
                    originSinkToSourceMap.put(mappingColumn, new UnboundAlias(mappingExprAlias, mappingColumn));
                }
            }
        }

        for (String columnFromPath : dataDesc.getColumnsFromPath()) {
            sinkCols.add(columnFromPath);
            // columnFromPath will be parsed by BE, put columns as placeholder.
            originSinkToSourceMap.put(columnFromPath, new UnboundSlot(columnFromPath));
        }

        checkAndAddSequenceCol(olapTable, dataDesc, sinkCols, originSinkToSourceMap);
        // build source columns
        List<NamedExpression> selectLists = new ArrayList<>(originSinkToSourceMap.values());
        LogicalPlan tvfLogicalPlan = new LogicalProject<>(selectLists,                            // add select
                new LogicalFilter<>(buildTvfFilter(dataDesc, originSinkToSourceMap),              // add filter
                        new LogicalCheckPolicy<>(getUnboundTVFRelation(properties))));            // add relation
        // TODO: 1. get column schema in tvf metadata.
        // TODO: 2. rewrite pre filter and source projects on tvfLogicalPlan when use csv or text format.
        boolean isPartialUpdate = olapTable.getEnableUniqueKeyMergeOnWrite()
                && sinkCols.size() < olapTable.getColumns().size();
        return new UnboundOlapTableSink<>(dataDesc.getNameParts(), sinkCols, ImmutableList.of(),
                dataDesc.getPartitionNames(), isPartialUpdate, tvfLogicalPlan);
    }

    private static String getColumnValueFromPath(String columnFromPath, String columnName) {
        // TODO: get partition value from path ?
        return null;
    }

    private static void buildSinkColumns(OlapTable olapTable, BulkLoadDataDesc dataDesc, List<String> sinkCols,
                                         Map<String, NamedExpression> originSinkToSourceMap) {
        List<Column> fullSchema = olapTable.getFullSchema();
        if (dataDesc.getFileFieldNames().size() > fullSchema.size()) {
            throw new VerifyException("Source columns size should less than sink columns size");
        }
        checkDeleteOnConditions(dataDesc.getMergeType(), dataDesc.getDeleteCondition());
        for (int i = 0; i < dataDesc.getFileFieldNames().size(); i++) {
            String targetColumn = fullSchema.get(i).getName();
            String sourceColumn = dataDesc.getFileFieldNames().get(i);
            if (targetColumn.equalsIgnoreCase(Column.VERSION_COL)) {
                continue;
            } else if (olapTable.hasDeleteSign()
                    && targetColumn.equalsIgnoreCase(Column.DELETE_SIGN)
                    && dataDesc.getDeleteCondition() != null) {
                sinkCols.add(targetColumn);
                WhenClause deleteWhen = new WhenClause(dataDesc.getDeleteCondition(), new TinyIntLiteral((byte) 1));
                CaseWhen deleteCase = new CaseWhen(ImmutableList.of(deleteWhen), new TinyIntLiteral((byte) 0));
                originSinkToSourceMap.put(targetColumn, new UnboundAlias(deleteCase, targetColumn));
                continue;
            }
            sinkCols.add(targetColumn);
            originSinkToSourceMap.put(targetColumn, new UnboundSlot(sourceColumn));
        }
    }

    private static void checkDeleteOnConditions(LoadTask.MergeType mergeType, Expression deleteCondition) {
        if (mergeType != LoadTask.MergeType.MERGE && deleteCondition != null) {
            throw new IllegalArgumentException(BulkLoadDataDesc.EXPECT_MERGE_DELETE_ON);
        }
        if (mergeType == LoadTask.MergeType.MERGE && deleteCondition == null) {
            throw new IllegalArgumentException(BulkLoadDataDesc.EXPECT_DELETE_ON);
        }
    }

    private static void checkAndAddSequenceCol(OlapTable olapTable, BulkLoadDataDesc dataDesc,
                                               List<String> sinkCols,
                                               Map<String, NamedExpression> originSinkToSourceMap) {
        if (!dataDesc.hasSequenceCol() && !olapTable.hasSequenceCol()) {
            return;
        }
        // check olapTable schema and sequenceCol
        if (olapTable.hasSequenceCol() && !dataDesc.hasSequenceCol()) {
            throw new VerifyException("Table " + olapTable.getName()
                    + " has sequence column, need to specify the sequence column");
        }
        if (dataDesc.hasSequenceCol() && !olapTable.hasSequenceCol()) {
            throw new VerifyException("There is no sequence column in the table " + olapTable.getName());
        }
        String sequenceCol = dataDesc.getSequenceCol();
        // check source sequence column is in parsedColumnExprList or Table base schema
        boolean hasSourceSequenceCol = false;
        if (!originSinkToSourceMap.isEmpty()) {
            List<String> allCols = new ArrayList<>(dataDesc.getFileFieldNames());
            allCols.addAll(originSinkToSourceMap.keySet());
            for (String sinkCol : allCols) {
                if (sinkCol.equals(sequenceCol)) {
                    hasSourceSequenceCol = true;
                    break;
                }
            }
        } else {
            List<Column> columns = olapTable.getBaseSchema();
            for (Column column : columns) {
                if (column.getName().equals(sequenceCol)) {
                    hasSourceSequenceCol = true;
                    break;
                }
            }
        }
        if (!hasSourceSequenceCol) {
            throw new VerifyException("There is no sequence column " + sequenceCol + " in the " + olapTable.getName()
                    + " or the COLUMNS and SET clause");
        } else {
            sinkCols.add(Column.SEQUENCE_COL);
            originSinkToSourceMap.putIfAbsent(Column.SEQUENCE_COL, new UnboundSlot(sequenceCol));
        }
    }

    private UnboundTVFRelation getUnboundTVFRelation(Map<String, String> properties) {
        UnboundTVFRelation relation;
        if (bulkStorageDesc.getStorageType() == BulkStorageDesc.StorageType.S3) {
            relation = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                    S3TableValuedFunction.NAME, new Properties(properties));
        } else if (bulkStorageDesc.getStorageType() == BulkStorageDesc.StorageType.HDFS) {
            relation = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                    HdfsTableValuedFunction.NAME, new Properties(properties));
        } else {
            throw new UnsupportedOperationException("Unsupported load storage type: "
                    + bulkStorageDesc.getStorageType());
        }
        return relation;
    }

    private static Set<Expression> buildTvfFilter(BulkLoadDataDesc dataDesc,
                                                  Map<String, NamedExpression> originSinkToSourceMap) {
        Set<Expression> conjuncts = new HashSet<>();
        if (dataDesc.getWhereExpr() != null) {
            Set<Expression> whereParts = ExpressionUtils.extractConjunctionToSet(dataDesc.getWhereExpr());
            for (Expression conjunct : whereParts) {
                conjuncts.add(rewriteConjunct(conjunct, originSinkToSourceMap));
            }
        }
        if (dataDesc.getPrecedingFilterExpr() != null) {
            conjuncts.addAll(ExpressionUtils.extractConjunctionToSet(dataDesc.getPrecedingFilterExpr()));
        }
        return conjuncts;
    }

    private static Expression rewriteConjunct(Expression conjunct,
                                              Map<String, NamedExpression> originSinkToSourceMap) {
        return conjunct.rewriteUp(e -> {
            if (!(e instanceof UnboundSlot)) {
                return e;
            }
            UnboundSlot slot = (UnboundSlot) e;
            if (originSinkToSourceMap.containsKey(slot.getName())) {
                NamedExpression rewrittenSlot = originSinkToSourceMap.get(slot.getName());
                if (rewrittenSlot instanceof UnboundAlias) {
                    return rewrittenSlot.getArguments().get(0);
                }
                return originSinkToSourceMap.get(slot.getName());
            }
            return e;
        });
    }

    private static OlapTable getOlapTable(ConnectContext ctx, BulkLoadDataDesc dataDesc) throws AnalysisException {
        OlapTable targetTable;
        TableIf table = RelationUtil.getTable(dataDesc.getNameParts(), ctx.getEnv());
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("table must be olapTable in load command");
        }
        targetTable = ((OlapTable) table);
        return targetTable;
    }

    private static Map<String, String> getTvfProperties(BulkLoadDataDesc dataDesc, BulkStorageDesc bulkStorageDesc) {
        Map<String, String> tvfProperties = new HashMap<>(bulkStorageDesc.getProperties());
        String fileFormat = dataDesc.getFormatDesc().getFileFormat();
        if (StringUtils.isEmpty(fileFormat)) {
            fileFormat = "csv";
            dataDesc.getFormatDesc().getColumnSeparator().ifPresent(sep ->
                    tvfProperties.put(ExternalFileTableValuedFunction.COLUMN_SEPARATOR, sep.getSeparator()));
            dataDesc.getFormatDesc().getLineDelimiter().ifPresent(sep ->
                    tvfProperties.put(ExternalFileTableValuedFunction.LINE_DELIMITER, sep.getSeparator()));
        }
        // TODO: resolve and put ExternalFileTableValuedFunction params
        tvfProperties.put(ExternalFileTableValuedFunction.FORMAT, fileFormat);

        List<String> filePaths = dataDesc.getFilePaths();
        // TODO: support multi location by union
        String listFilePath = filePaths.get(0);
        if (bulkStorageDesc.getStorageType() == BulkStorageDesc.StorageType.S3) {
            S3Properties.convertToStdProperties(tvfProperties);
            tvfProperties.keySet().removeIf(S3Properties.Env.FS_KEYS::contains);
            // TODO: check file path by s3 fs list status
            tvfProperties.put(S3TableValuedFunction.S3_URI, listFilePath);
        }

        final Map<String, String> dataDescProps = dataDesc.getProperties();
        if (dataDescProps != null) {
            tvfProperties.putAll(dataDescProps);
        }
        List<String> columnsFromPath = dataDesc.getColumnsFromPath();
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            tvfProperties.put(ExternalFileTableValuedFunction.PATH_PARTITION_KEYS,
                    String.join(",", columnsFromPath));
        }
        return tvfProperties;
    }

    private void executeInsertStmtPlan(ConnectContext ctx, StmtExecutor executor, List<LogicalPlan> plans) {
        try {
            for (LogicalPlan logicalPlan : plans) {
                ((Command) logicalPlan).run(ctx, executor);
            }
        } catch (QueryStateException e) {
            ctx.setState(e.getQueryState());
            throw new NereidsException("Command process failed", new AnalysisException(e.getMessage(), e));
        } catch (UserException e) {
            // Return message to info client what happened.
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            throw new NereidsException("Command process failed", new AnalysisException(e.getMessage(), e));
        } catch (Exception e) {
            // Maybe our bug
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
            throw new NereidsException("Command process failed.", new AnalysisException(e.getMessage(), e));
        }
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return null;
        // return completeQueryPlan(dataDesc, logicalQuery);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadCommand(this, context);
    }
}
