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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.BulkLoadDataDesc;
import org.apache.doris.nereids.trees.plans.commands.info.BulkStorageDesc;
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
import org.apache.doris.tablefunction.HdfsTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * load OLAP table data from external bulk file
 */
public class LoadCommand extends Command implements ForwardWithSync {

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
        this.sourceInfos = Objects.requireNonNull(ImmutableList.copyOf(sourceInfos), "sourceInfos should not null");
        this.properties = Objects.requireNonNull(ImmutableMap.copyOf(properties), "properties should not null");
        this.bulkStorageDesc = Objects.requireNonNull(bulkStorageDesc, "bulkStorageDesc should not null");
        this.comment = Objects.requireNonNull(comment, "comment should not null");
    }

    /**
     * for test print
     *
     * @param ctx context
     * @return parsed insert into plan
     */
    @VisibleForTesting
    public List<LogicalPlan> parseToInsertIntoPlan(ConnectContext ctx) throws AnalysisException {
        List<LogicalPlan> plans = new ArrayList<>();
        for (BulkLoadDataDesc dataDesc : sourceInfos) {
            plans.add(completeQueryPlan(ctx, dataDesc));
        }
        return plans;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // TODO: begin txn form multi insert sql
        /* this.profile = new Profile("Query", ctx.getSessionVariable().enableProfile);
          profile.getSummaryProfile().setQueryBeginTime();
          for (BulkLoadDataDesc dataDesc : sourceInfos) {
               plans.add(new InsertIntoTableCommand(completeQueryPlan(ctx, dataDesc), Optional.of(labelName), false));
          }
          profile.getSummaryProfile().setQueryPlanFinishTime();
         * executeInsertStmtPlan(ctx, executor, plans);  */
        throw new AnalysisException("Fallback to legacy planner temporary.");
    }

    private LogicalPlan completeQueryPlan(ConnectContext ctx, BulkLoadDataDesc dataDesc)
            throws AnalysisException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("nereids load stmt before conversion: {}", dataDesc::toSql);
        }
        // 1. build source projects plan (select col1,col2... from tvf where prefilter)
        Map<String, String> tvfProperties = getTvfProperties(dataDesc, bulkStorageDesc);
        LogicalPlan tvfLogicalPlan = new LogicalCheckPolicy<>(getUnboundTVFRelation(tvfProperties));
        tvfLogicalPlan = buildTvfQueryPlan(dataDesc, tvfProperties, tvfLogicalPlan);

        if (!(tvfLogicalPlan instanceof LogicalProject)) {
            throw new AnalysisException("Fail to build TVF query, TVF query should be LogicalProject");
        }
        List<NamedExpression> tvfProjects = ((LogicalProject<?>) tvfLogicalPlan).getProjects();
        // tvfProjects may be '*' or 'col1,col2,...'
        if (tvfProjects.isEmpty()) {
            throw new AnalysisException("Fail to build TVF query, parsed TVF select list requires not null");
        }
        boolean scanAllTvfCol = (tvfProjects.get(0) instanceof UnboundStar);

        OlapTable olapTable = getOlapTable(ctx, dataDesc);
        List<Column> olapSchema = olapTable.getBaseSchema();
        // map column index to mapping expr
        Map<String, Expression> mappingExpressions = dataDesc.getColumnMappings();
        // 2. build sink where
        Set<Expression> conjuncts = new HashSet<>();
        if (dataDesc.getWhereExpr().isPresent()) {
            Set<Expression> whereParts = ExpressionUtils.extractConjunctionToSet(dataDesc.getWhereExpr().get());
            for (Expression wherePart : whereParts) {
                if (!(wherePart instanceof ComparisonPredicate)) {
                    throw new AnalysisException("WHERE clause must be comparison expression");
                }
                ComparisonPredicate comparison = ((ComparisonPredicate) wherePart);
                if (!(comparison.left() instanceof UnboundSlot)) {
                    throw new AnalysisException("Invalid predicate column " + comparison.left().toSql());
                }
                conjuncts.add(comparison.rewriteUp(e -> {
                    if (!(e instanceof UnboundSlot)) {
                        return e;
                    }
                    UnboundSlot slot = (UnboundSlot) e;
                    String colName = getUnquotedName(slot);
                    return mappingExpressions.getOrDefault(colName, e);
                }));
            }
        }

        if (dataDesc.getFileFieldNames().isEmpty() && isCsvType(tvfProperties) && !conjuncts.isEmpty()) {
            throw new AnalysisException("Required property 'csv_schema' for csv file, "
                    + "when no column list specified and use WHERE");
        }
        tvfLogicalPlan = new LogicalFilter<>(conjuncts, tvfLogicalPlan);

        // 3. build sink project
        List<String> sinkCols = new ArrayList<>();
        List<NamedExpression> selectLists = new ArrayList<>();
        List<String> olapColumns = olapSchema.stream().map(Column::getDisplayName).collect(Collectors.toList());
        if (!scanAllTvfCol) {
            int numSinkCol = Math.min(tvfProjects.size(), olapColumns.size());
            // if not scan all tvf column, try to treat each tvfColumn as olapColumn
            for (int i = 0; i < numSinkCol; i++) {
                UnboundSlot sourceCol = (UnboundSlot) tvfProjects.get(i);
                // check sourceCol is slot and check olapColumn beyond index.
                String olapColumn = olapColumns.get(i);
                fillSinkBySourceCols(mappingExpressions, olapColumn,
                        sourceCol, sinkCols, selectLists);
            }
            fillDeleteOnColumn(dataDesc, olapTable, sinkCols, selectLists, Column.DELETE_SIGN);
        } else {
            for (String olapColumn : olapColumns) {
                if (olapColumn.equalsIgnoreCase(Column.VERSION_COL)
                        || olapColumn.equalsIgnoreCase(Column.SEQUENCE_COL)) {
                    continue;
                }
                if (olapColumn.equalsIgnoreCase(Column.DELETE_SIGN)) {
                    fillDeleteOnColumn(dataDesc, olapTable, sinkCols, selectLists, olapColumn);
                    continue;
                }
                fillSinkBySourceCols(mappingExpressions, olapColumn, new UnboundSlot(olapColumn),
                        sinkCols, selectLists);
            }
        }
        if (sinkCols.isEmpty() && selectLists.isEmpty()) {
            // build 'insert into tgt_tbl select * from src_tbl'
            selectLists.add(new UnboundStar(new ArrayList<>()));
        }
        for (String columnFromPath : dataDesc.getColumnsFromPath()) {
            sinkCols.add(columnFromPath);
            // columnFromPath will be parsed by BE, put columns as placeholder.
            selectLists.add(new UnboundSlot(columnFromPath));
        }

        tvfLogicalPlan = new LogicalProject<>(selectLists, tvfLogicalPlan);
        checkAndAddSequenceCol(olapTable, dataDesc, sinkCols, selectLists);
        boolean isPartialUpdate = olapTable.getEnableUniqueKeyMergeOnWrite()
                && sinkCols.size() < olapTable.getColumns().size();
        return new UnboundOlapTableSink<>(dataDesc.getNameParts(), sinkCols, ImmutableList.of(),
                dataDesc.getPartitionNames(), isPartialUpdate, tvfLogicalPlan);
    }

    private static void fillDeleteOnColumn(BulkLoadDataDesc dataDesc, OlapTable olapTable,
                                           List<String> sinkCols,
                                           List<NamedExpression> selectLists,
                                           String olapColumn) throws AnalysisException {
        if (olapTable.hasDeleteSign() && dataDesc.getDeleteCondition().isPresent()) {
            checkDeleteOnConditions(dataDesc.getMergeType(), dataDesc.getDeleteCondition().get());
            Optional<If> deleteIf = createDeleteOnIfCall(olapTable, olapColumn, dataDesc);
            if (deleteIf.isPresent()) {
                sinkCols.add(olapColumn);
                selectLists.add(new UnboundAlias(deleteIf.get(), olapColumn));
            }
            sinkCols.add(olapColumn);
        }
    }

    /**
     * use to get unquoted column name
     * @return unquoted slot name
     */
    public static String getUnquotedName(NamedExpression slot) {
        if (slot instanceof UnboundAlias) {
            return slot.getName();
        } else if (slot instanceof UnboundSlot) {
            List<String> slotNameParts = ((UnboundSlot) slot).getNameParts();
            return slotNameParts.get(slotNameParts.size() - 1);
        }
        return slot.getName();
    }

    private static void fillSinkBySourceCols(Map<String, Expression> mappingExpressions,
                                             String olapColumn, UnboundSlot tvfColumn,
                                             List<String> sinkCols, List<NamedExpression> selectLists) {
        sinkCols.add(olapColumn);
        if (mappingExpressions.containsKey(olapColumn)) {
            selectLists.add(new UnboundAlias(mappingExpressions.get(olapColumn), olapColumn));
        } else {
            selectLists.add(new UnboundAlias(tvfColumn, olapColumn));
        }
    }

    private static boolean isCsvType(Map<String, String> tvfProperties) {
        return tvfProperties.get(FileFormatConstants.PROP_FORMAT).equalsIgnoreCase("csv");
    }

    /**
     * fill all column that need to be loaded to sinkCols.
     * fill the map with sink columns and generated source columns.
     * sink columns use for 'INSERT INTO'
     * generated source columns use for 'SELECT'
     *
     * @param dataDesc       dataDesc
     * @param tvfProperties  generated tvfProperties
     * @param tvfLogicalPlan source tvf relation
     */
    private static LogicalPlan buildTvfQueryPlan(BulkLoadDataDesc dataDesc,
                                                 Map<String, String> tvfProperties,
                                                 LogicalPlan tvfLogicalPlan) throws AnalysisException {
        // build tvf column filter
        if (dataDesc.getPrecedingFilterExpr().isPresent()) {
            Set<Expression> preConjuncts =
                    ExpressionUtils.extractConjunctionToSet(dataDesc.getPrecedingFilterExpr().get());
            if (!preConjuncts.isEmpty()) {
                tvfLogicalPlan = new LogicalFilter<>(preConjuncts, tvfLogicalPlan);
            }
        }

        Map<String, String> sourceProperties = dataDesc.getProperties();
        if (dataDesc.getFileFieldNames().isEmpty() && isCsvType(tvfProperties)) {
            String csvSchemaStr = sourceProperties.get(FileFormatConstants.PROP_CSV_SCHEMA);
            if (csvSchemaStr != null) {
                tvfProperties.put(FileFormatConstants.PROP_CSV_SCHEMA, csvSchemaStr);
                List<Column> csvSchema = new ArrayList<>();
                FileFormatUtils.parseCsvSchema(csvSchema, csvSchemaStr);
                List<NamedExpression> csvColumns = new ArrayList<>();
                for (Column csvColumn : csvSchema) {
                    csvColumns.add(new UnboundSlot(csvColumn.getName()));
                }
                if (!csvColumns.isEmpty()) {
                    for (String columnFromPath : dataDesc.getColumnsFromPath()) {
                        csvColumns.add(new UnboundSlot(columnFromPath));
                    }
                    return new LogicalProject<>(csvColumns, tvfLogicalPlan);
                }
                if (!dataDesc.getPrecedingFilterExpr().isPresent()) {
                    throw new AnalysisException("Required property 'csv_schema' for csv file, "
                            + "when no column list specified and use PRECEDING FILTER");
                }
            }
            return getStarProjectPlan(tvfLogicalPlan);
        }
        List<NamedExpression> dataDescColumns = new ArrayList<>();
        for (int i = 0; i < dataDesc.getFileFieldNames().size(); i++) {
            String sourceColumn = dataDesc.getFileFieldNames().get(i);
            dataDescColumns.add(new UnboundSlot(sourceColumn));
        }
        if (dataDescColumns.isEmpty()) {
            return getStarProjectPlan(tvfLogicalPlan);
        } else {
            return new LogicalProject<>(dataDescColumns, tvfLogicalPlan);
        }
    }

    private static LogicalProject<LogicalPlan> getStarProjectPlan(LogicalPlan logicalPlan) {
        return new LogicalProject<>(ImmutableList.of(new UnboundStar(new ArrayList<>())), logicalPlan);
    }

    private static Optional<If> createDeleteOnIfCall(OlapTable olapTable, String olapColName,
                                                     BulkLoadDataDesc dataDesc) throws AnalysisException {
        if (olapTable.hasDeleteSign()
                && dataDesc.getDeleteCondition().isPresent()) {
            if (!(dataDesc.getDeleteCondition().get() instanceof ComparisonPredicate)) {
                throw new AnalysisException("DELETE ON clause must be comparison expression.");
            }
            ComparisonPredicate deleteOn = (ComparisonPredicate) dataDesc.getDeleteCondition().get();
            Expression deleteOnCol = deleteOn.left();
            if (!(deleteOnCol instanceof UnboundSlot)) {
                throw new AnalysisException("DELETE ON column must be an undecorated OLAP column.");
            }
            if (!olapColName.equalsIgnoreCase(getUnquotedName((UnboundSlot) deleteOnCol))) {
                return Optional.empty();
            }
            If deleteIf = new If(deleteOn, new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0));
            return Optional.of(deleteIf);
        } else {
            return Optional.empty();
        }
    }

    private static void checkDeleteOnConditions(LoadTask.MergeType mergeType, Expression deleteCondition)
                throws AnalysisException {
        if (mergeType != LoadTask.MergeType.MERGE && deleteCondition != null) {
            throw new AnalysisException(BulkLoadDataDesc.EXPECT_MERGE_DELETE_ON);
        }
        if (mergeType == LoadTask.MergeType.MERGE && deleteCondition == null) {
            throw new AnalysisException(BulkLoadDataDesc.EXPECT_DELETE_ON);
        }
    }

    private static void checkAndAddSequenceCol(OlapTable olapTable, BulkLoadDataDesc dataDesc,
                                               List<String> sinkCols, List<NamedExpression> selectLists)
                throws AnalysisException {
        Optional<String> optSequenceCol = dataDesc.getSequenceCol();
        if (!optSequenceCol.isPresent() && !olapTable.hasSequenceCol()) {
            return;
        }
        // check olapTable schema and sequenceCol
        if (olapTable.hasSequenceCol() && !optSequenceCol.isPresent()) {
            throw new AnalysisException("Table " + olapTable.getName()
                    + " has sequence column, need to specify the sequence column");
        }
        if (optSequenceCol.isPresent() && !olapTable.hasSequenceCol()) {
            throw new AnalysisException("There is no sequence column in the table " + olapTable.getName());
        }
        String sequenceCol = dataDesc.getSequenceCol().get();
        // check source sequence column is in parsedColumnExprList or Table base schema
        boolean hasSourceSequenceCol = false;
        if (!sinkCols.isEmpty()) {
            List<String> allCols = new ArrayList<>(dataDesc.getFileFieldNames());
            allCols.addAll(sinkCols);
            for (String sinkCol : allCols) {
                if (sinkCol.equals(sequenceCol)) {
                    hasSourceSequenceCol = true;
                    break;
                }
            }
        }
        List<Column> columns = olapTable.getBaseSchema();
        for (Column column : columns) {
            if (column.getName().equals(sequenceCol)) {
                hasSourceSequenceCol = true;
                break;
            }
        }
        if (!hasSourceSequenceCol) {
            throw new AnalysisException("There is no sequence column " + sequenceCol + " in the " + olapTable.getName()
                    + " or the COLUMNS and SET clause");
        } else {
            sinkCols.add(Column.SEQUENCE_COL);
            selectLists.add(new UnboundAlias(new UnboundSlot(sequenceCol), Column.SEQUENCE_COL));
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
        String fileFormat = dataDesc.getFormatDesc().getFileFormat().orElse("csv");
        if ("csv".equalsIgnoreCase(fileFormat)) {
            dataDesc.getFormatDesc().getColumnSeparator().ifPresent(sep ->
                    tvfProperties.put(FileFormatConstants.PROP_COLUMN_SEPARATOR, sep.getSeparator()));
            dataDesc.getFormatDesc().getLineDelimiter().ifPresent(sep ->
                    tvfProperties.put(FileFormatConstants.PROP_LINE_DELIMITER, sep.getSeparator()));
        }
        // TODO: resolve and put ExternalFileTableValuedFunction params
        tvfProperties.put(FileFormatConstants.PROP_FORMAT, fileFormat);

        List<String> filePaths = dataDesc.getFilePaths();
        // TODO: support multi location by union
        String listFilePath = filePaths.get(0);
        if (bulkStorageDesc.getStorageType() == BulkStorageDesc.StorageType.S3) {
            S3Properties.convertToStdProperties(tvfProperties);
            tvfProperties.keySet().removeIf(S3Properties.Env.FS_KEYS::contains);
            // TODO: check file path by s3 fs list status
            tvfProperties.put(S3TableValuedFunction.PROP_URI, listFilePath);
        }

        final Map<String, String> dataDescProps = dataDesc.getProperties();
        if (dataDescProps != null) {
            tvfProperties.putAll(dataDescProps);
        }
        List<String> columnsFromPath = dataDesc.getColumnsFromPath();
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            tvfProperties.put(FileFormatConstants.PROP_PATH_PARTITION_KEYS,
                    String.join(",", columnsFromPath));
        }
        return tvfProperties;
    }

    private void executeInsertStmtPlan(ConnectContext ctx, StmtExecutor executor, List<InsertIntoTableCommand> plans) {
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
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadCommand(this, context);
    }
}
