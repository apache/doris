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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.ExportJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * EXPORT statement, export data to dirs by broker.
 *
 * syntax:
 *      EXPORT TABLE table_name [PARTITION (name1[, ...])]
 *          TO 'export_target_path'
 *          [PROPERTIES("key"="value")]
 *          BY BROKER 'broker_name' [( $broker_attrs)]
 */
public class ExportCommand extends Command implements ForwardWithSync {
    public static final String PARALLELISM = "parallelism";
    public static final String LABEL = "label";
    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_PARALLELISM = "1";
    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LABEL)
            .add(PARALLELISM)
            .add(LoadStmt.EXEC_MEM_LIMIT)
            .add(LoadStmt.TIMEOUT_PROPERTY)
            .add(LoadStmt.KEY_IN_PARAM_COLUMNS)
            .add(LoadStmt.TIMEOUT_PROPERTY)
            .add(OutFileClause.PROP_MAX_FILE_SIZE)
            .add(OutFileClause.PROP_DELETE_EXISTING_FILES)
            .add(PropertyAnalyzer.PROPERTIES_COLUMN_SEPARATOR)
            .add(PropertyAnalyzer.PROPERTIES_LINE_DELIMITER)
            .add("format")
            .build();

    private final List<String> nameParts;
    private final Optional<Expression> expr;
    private final String path;
    private final List<String> partitionsNames;
    private final Map<String, String> fileProperties;
    private final Optional<BrokerDesc> brokerDesc;

    /**
     * constructor of ExportCommand
     */
    public ExportCommand(List<String> nameParts, List<String> partitions, Optional<Expression> expr,
            String path, Map<String, String> fileProperties, Optional<BrokerDesc> brokerDesc) {
        super(PlanType.EXPORT_COMMAND);
        this.nameParts = ImmutableList.copyOf(Objects.requireNonNull(nameParts, "nameParts should not be null"));
        this.path = Objects.requireNonNull(path.trim(), "export path should not be null");
        this.partitionsNames = ImmutableList.copyOf(
                Objects.requireNonNull(partitions, "partitions should not be null"));
        this.fileProperties = ImmutableSortedMap.copyOf(
                Objects.requireNonNull(fileProperties, "fileProperties should not be null"),
                String.CASE_INSENSITIVE_ORDER);
        this.expr = expr;
        if (!brokerDesc.isPresent()) {
            this.brokerDesc = Optional.of(new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null));
        } else {
            this.brokerDesc = brokerDesc;
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // get tblName
        TableName tblName = getTableName(ctx);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, tblName.getDb(), tblName.getTbl(),
                PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "EXPORT",
                    ctx.getQualifiedUser(),
                    ctx.getRemoteIP(),
                    tblName.getDb() + ": " + tblName.getTbl());
        }

        // check phases
        checkAllParameters(ctx, tblName, fileProperties);

        ExportJob exportJob = generateExportJob(ctx, fileProperties, tblName);
        // register job
        ctx.getEnv().getExportMgr().addExportJobAndRegisterTask(exportJob);
    }

    private void checkAllParameters(ConnectContext ctx, TableName tblName, Map<String, String> fileProperties)
            throws UserException {
        checkPropertyKey(fileProperties);
        checkPartitions(ctx.getEnv(), tblName);
        checkBrokerDesc(ctx);
        checkFileProperties(fileProperties, tblName);
    }

    // check property key
    private void checkPropertyKey(Map<String, String> properties) throws AnalysisException {
        for (String key : properties.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("Invalid property key: [" + key + "]");
            }
        }
    }

    // check partitions specified by user are belonged to the table.
    private void checkPartitions(Env env, TableName tblName) throws AnalysisException, UserException {
        if (this.partitionsNames.isEmpty()) {
            return;
        }

        if (this.partitionsNames.size() > Config.maximum_number_of_export_partitions) {
            throw new AnalysisException("The partitions number of this export job is larger than the maximum number"
                    + " of partitions allowed by an export job");
        }

        Database db = env.getInternalCatalog().getDbOrAnalysisException(tblName.getDb());
        Table table = db.getTableOrAnalysisException(tblName.getTbl());
        table.readLock();
        try {
            // check table
            if (!table.isPartitioned()) {
                throw new AnalysisException("Table[" + tblName.getTbl() + "] is not partitioned.");
            }
            Table.TableType tblType = table.getType();
            switch (tblType) {
                case MYSQL:
                case ODBC:
                case JDBC:
                case OLAP:
                    break;
                case BROKER:
                case SCHEMA:
                case INLINE_VIEW:
                case VIEW:
                default:
                    throw new AnalysisException("Table[" + tblName.getTbl() + "] is "
                            + tblType + " type, do not support EXPORT.");
            }

            for (String partitionName : this.partitionsNames) {
                Partition partition = table.getPartition(partitionName);
                if (partition == null) {
                    throw new AnalysisException("Partition [" + partitionName + "] does not exist "
                            + "in Table[" + tblName.getTbl() + "]");
                }
            }
        } finally {
            table.readUnlock();
        }
    }

    private void checkBrokerDesc(ConnectContext ctx) throws UserException {
        // check path is valid
        StorageBackend.checkPath(this.path, this.brokerDesc.get().getStorageType());

        if (brokerDesc.get().getStorageType() == StorageBackend.StorageType.BROKER) {
            BrokerMgr brokerMgr = ctx.getEnv().getBrokerMgr();
            if (!brokerMgr.containsBroker(brokerDesc.get().getName())) {
                throw new AnalysisException("broker " + brokerDesc.get().getName() + " does not exist");
            }
            if (null == brokerMgr.getAnyBroker(brokerDesc.get().getName())) {
                throw new AnalysisException("failed to get alive broker");
            }
        }
    }

    private ExportJob generateExportJob(ConnectContext ctx, Map<String, String> fileProperties, TableName tblName)
            throws UserException {
        ExportJob exportJob = new ExportJob();
        // set export job
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(tblName.getDb());
        exportJob.setDbId(db.getId());
        exportJob.setTableName(tblName);
        exportJob.setExportTable(db.getTableOrDdlException(tblName.getTbl()));
        exportJob.setTableId(db.getTableOrDdlException(tblName.getTbl()).getId());

        // set partitions
        exportJob.setPartitionNames(this.partitionsNames);
        // set where expression
        exportJob.setWhereExpression(this.expr);
        // set path
        exportJob.setExportPath(this.path);

        // set column separator
        String columnSeparator = Separator.convertSeparator(fileProperties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_COLUMN_SEPARATOR, DEFAULT_COLUMN_SEPARATOR));
        exportJob.setColumnSeparator(columnSeparator);

        // set line delimiter
        String lineDelimiter = Separator.convertSeparator(fileProperties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_LINE_DELIMITER, DEFAULT_LINE_DELIMITER));
        exportJob.setLineDelimiter(lineDelimiter);

        // set format
        exportJob.setFormat(fileProperties.getOrDefault(LoadStmt.KEY_IN_PARAM_FORMAT_TYPE, "csv")
                .toLowerCase());

        // set parallelism
        int parallelism;
        try {
            parallelism = Integer.parseInt(fileProperties.getOrDefault(PARALLELISM, DEFAULT_PARALLELISM));
        } catch (NumberFormatException e) {
            throw new AnalysisException("The value of parallelism is invalid!");
        }
        exportJob.setParallelism(parallelism);

        // set label
        // if fileProperties contains LABEL, the label has been checked in check phases
        String defaultLabel = "export_" + UUID.randomUUID();
        exportJob.setLabel(fileProperties.getOrDefault(LABEL, defaultLabel));

        // set max_file_size
        exportJob.setMaxFileSize(fileProperties.getOrDefault(OutFileClause.PROP_MAX_FILE_SIZE, ""));
        // set delete_existing_files
        exportJob.setDeleteExistingFiles(fileProperties.getOrDefault(
                OutFileClause.PROP_DELETE_EXISTING_FILES, ""));

        // null means not specified
        // "" means user specified zero columns
        // if fileProperties contains KEY_IN_PARAM_COLUMNS, the columns have been checked in check phases
        String columns = fileProperties.getOrDefault(LoadStmt.KEY_IN_PARAM_COLUMNS, null);
        exportJob.setColumns(columns);
        if (columns != null) {
            Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();
            exportJob.setExportColumns(split.splitToList(columns.toLowerCase()));
        }

        // set broker desc
        exportJob.setBrokerDesc(this.brokerDesc.get());

        // set sessions
        exportJob.setQualifiedUser(ctx.getQualifiedUser());
        exportJob.setUserIdentity(ctx.getCurrentUserIdentity());

        Optional<SessionVariable> optionalSessionVariable = Optional.ofNullable(
                ConnectContext.get().getSessionVariable());
        exportJob.setSessionVariables(optionalSessionVariable.orElse(VariableMgr.getDefaultSessionVariable()));
        exportJob.setTimeoutSecond(optionalSessionVariable.orElse(VariableMgr.getDefaultSessionVariable())
                .getQueryTimeoutS());

        // exportJob generate outfile sql
        exportJob.generateOutfileLogicalPlans(this.nameParts);
        return exportJob;
    }

    private TableName getTableName(ConnectContext ctx) throws UserException {
        // get tblName
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, this.nameParts);
        TableName tblName = new TableName(qualifiedTableName.get(0), qualifiedTableName.get(1),
                qualifiedTableName.get(2));
        Util.prohibitExternalCatalog(tblName.getCtl(), this.getClass().getSimpleName());
        return tblName;
    }

    private void checkFileProperties(Map<String, String> fileProperties, TableName tblName) throws UserException {
        // check user specified columns
        if (fileProperties.containsKey(LoadStmt.KEY_IN_PARAM_COLUMNS)) {
            checkColumns(fileProperties.get(LoadStmt.KEY_IN_PARAM_COLUMNS), tblName);
        }

        // check user specified label
        if (fileProperties.containsKey(LABEL)) {
            FeNameFormat.checkLabel(fileProperties.get(LABEL));
        }
    }

    private void checkColumns(String columns, TableName tblName) throws AnalysisException, UserException {
        if (columns.isEmpty()) {
            throw new AnalysisException("columns can not be empty");
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(tblName.getDb());
        Table table = db.getTableOrDdlException(tblName.getTbl());
        List<String> tableColumns = table.getBaseSchema().stream().map(column -> column.getName())
                .collect(Collectors.toList());
        Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();

        List<String> columnsSpecified = split.splitToList(columns.toLowerCase());
        for (String columnName : columnsSpecified) {
            if (!tableColumns.contains(columnName)) {
                throw new AnalysisException("unknown column [" + columnName + "] in table [" + tblName.getTbl() + "]");
            }
        }
    }

    public Map<String, String> getFileProperties() {
        return this.fileProperties;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExportCommand(this, context);
    }
}
