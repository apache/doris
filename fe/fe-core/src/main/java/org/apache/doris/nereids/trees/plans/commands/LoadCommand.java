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
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryStateException;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * export table
 */
public class LoadCommand extends Command {
    public static final Logger LOG = LogManager.getLogger(LoadCommand.class);
    private final String labelName;
    private final BulkStorageDesc bulkStorageDesc;
    private final List<BulkLoadDataDesc> sourceInfos;
    private final Map<String, String> properties;
    private final String comment;
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

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        this.profile = new Profile("Query", ctx.getSessionVariable().enableProfile);
        // TODO: begin txn form multi insert sql
        List<LogicalPlan> plans = new ArrayList<>();
        profile.getSummaryProfile().setQueryBeginTime();
        for (BulkLoadDataDesc dataDesc : sourceInfos) {
            LOG.debug("nereids load stmt before conversion: {}", dataDesc.toSql());
            Map<String, String> props = getTvfProperties(dataDesc, bulkStorageDesc);
            String dataTvfSql = dataDesc.toInsertSql(bulkStorageDesc.getStorageType(), props);
            LOG.debug("nereids load stmt after conversion: {}", dataTvfSql);
            // it will visit InsertIntoCommand and call the run method.
            List<StatementBase> statements = new NereidsParser().parseSQL(dataTvfSql);
            StatementBase parsedStmt = statements.get(0);
            Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                    "Nereids only process LogicalPlanAdapter, but parsedStmt is "
                            + parsedStmt.getClass().getName());
            ctx.getState().setNereids(true);
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            plans.add(logicalPlan);
        }
        profile.getSummaryProfile().setQueryPlanFinishTime();
        executeInsertStmtPlan(ctx, executor, plans);
    }

    private Map<String, String> getTvfProperties(BulkLoadDataDesc dataDesc, BulkStorageDesc bulkStorageDesc) {
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

        List<String> filePath = dataDesc.getFilePaths();
        // TODO: support multi location by union
        String listFilePath = filePath.get(0);
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
        ctx.getSessionVariable().enableNereidsDML = true;
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
