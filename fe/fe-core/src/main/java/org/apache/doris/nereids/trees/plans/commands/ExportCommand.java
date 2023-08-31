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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.ExportJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
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
import com.google.common.collect.Maps;

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

    private List<String> nameParts;
    private String whereSql;
    private String path;
    private List<String> partitionsNameList;
    private Map<String, String> fileProperties;
    private BrokerDesc brokerDesc;

    // properties
    private TableName tblName;
    private String columnSeparator;
    private String lineDelimiter;
    private String columns;
    private String format;
    private String label;
    private Integer parallelism;
    private String maxFileSize;
    private String deleteExistingFiles;

    // session and user identity
    private SessionVariable sessionVariables;
    private String qualifiedUser;
    private UserIdentity userIdentity;

    // Export Job
    private ExportJob exportJob;

    /**
     * constructor of ExportCommand
     */
    public ExportCommand(List<String> nameParts, List<String> partitions, String whereSql, String path,
            Map<String, String> fileProperties, BrokerDesc brokerDesc) {
        super(PlanType.EXPORT_COMMAND);
        this.nameParts = ImmutableList.copyOf(Objects.requireNonNull(nameParts, "nameParts should not be null"));
        this.path = Objects.requireNonNull(path.trim(), "export path should not be null");
        this.partitionsNameList = partitions;
        this.whereSql = whereSql;
        this.fileProperties = fileProperties;
        this.brokerDesc = brokerDesc;

        this.columnSeparator = DEFAULT_COLUMN_SEPARATOR;
        this.lineDelimiter = DEFAULT_LINE_DELIMITER;

        Optional<SessionVariable> optionalSessionVariable = Optional.ofNullable(
                ConnectContext.get().getSessionVariable());
        this.sessionVariables = optionalSessionVariable.orElse(VariableMgr.getDefaultSessionVariable());
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        checkParameter(ctx);
        // register job
        ctx.getEnv().getExportMgr().addExportJobAndRegisterTask(exportJob);
    }

    private void checkParameter(ConnectContext ctx) throws UserException {
        // get tblName
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        this.tblName = new TableName(qualifiedTableName.get(0), qualifiedTableName.get(1), qualifiedTableName.get(2));
        Util.prohibitExternalCatalog(tblName.getCtl(), this.getClass().getSimpleName());

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, tblName.getDb(), tblName.getTbl(),
                PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "EXPORT",
                    ctx.getQualifiedUser(),
                    ctx.getRemoteIP(),
                    tblName.getDb() + ": " + tblName.getTbl());
        }
        qualifiedUser = ctx.getQualifiedUser();
        userIdentity = ctx.getCurrentUserIdentity();

        // check table && partitions whether exist
        checkPartitions(ctx.getEnv());

        // check broker whether exist
        if (brokerDesc == null) {
            brokerDesc = new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null);
        }

        // check path is valid
        path = StorageBackend.checkPath(path, brokerDesc.getStorageType());
        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            BrokerMgr brokerMgr = ctx.getEnv().getBrokerMgr();
            if (!brokerMgr.containsBroker(brokerDesc.getName())) {
                throw new AnalysisException("broker " + brokerDesc.getName() + " does not exist");
            }
            if (null == brokerMgr.getAnyBroker(brokerDesc.getName())) {
                throw new AnalysisException("failed to get alive broker");
            }
        }

        // check properties
        checkProperties(fileProperties);

        // create job and analyze job
        setJob();
        exportJob.analyze();
    }

    // check partitions specified by user are belonged to the table.
    private void checkPartitions(Env env) throws AnalysisException {
        if (this.partitionsNameList == null) {
            return;
        }

        if (this.partitionsNameList.size() > Config.maximum_number_of_export_partitions) {
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

            for (String partitionName : this.partitionsNameList) {
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

    private void checkProperties(Map<String, String> properties) throws UserException {
        for (String key : properties.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new UserException("Invalid property key: [" + key + "]");
            }
        }

        // convert key to lowercase
        Map<String, String> tmpMap = Maps.newHashMap();
        for (String key : properties.keySet()) {
            tmpMap.put(key.toLowerCase(), properties.get(key));
        }
        properties = tmpMap;

        this.columnSeparator = Separator.convertSeparator(PropertyAnalyzer.analyzeColumnSeparator(
                properties, DEFAULT_COLUMN_SEPARATOR));
        this.lineDelimiter = Separator.convertSeparator(PropertyAnalyzer.analyzeLineDelimiter(
                properties, DEFAULT_LINE_DELIMITER));
        // null means not specified
        // "" means user specified zero columns
        this.columns = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_COLUMNS, null);

        // check columns are exits
        if (columns != null) {
            checkColumns();
        }

        // format
        this.format = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_FORMAT_TYPE, "csv").toLowerCase();

        // parallelism
        String parallelismString = properties.getOrDefault(PARALLELISM, DEFAULT_PARALLELISM);
        try {
            this.parallelism = Integer.parseInt(parallelismString);
        } catch (NumberFormatException e) {
            throw new UserException("The value of parallelism is invalid!");
        }

        // max_file_size
        this.maxFileSize = properties.getOrDefault(OutFileClause.PROP_MAX_FILE_SIZE, "");
        this.deleteExistingFiles = properties.getOrDefault(OutFileClause.PROP_DELETE_EXISTING_FILES, "");

        // label
        if (properties.containsKey(LABEL)) {
            FeNameFormat.checkLabel(properties.get(LABEL));
            this.label = properties.get(LABEL);
        } else {
            // generate a random label
            this.label = "export_" + UUID.randomUUID();
        }
    }

    private void checkColumns() throws DdlException {
        if (this.columns.isEmpty()) {
            throw new DdlException("columns can not be empty");
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(this.tblName.getDb());
        Table table = db.getTableOrDdlException(this.tblName.getTbl());
        List<String> tableColumns = table.getBaseSchema().stream().map(column -> column.getName())
                .collect(Collectors.toList());
        Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();

        List<String> columnsSpecified = split.splitToList(this.columns.toLowerCase());
        for (String columnName : columnsSpecified) {
            if (!tableColumns.contains(columnName)) {
                throw new DdlException("unknown column [" + columnName + "] in table [" + this.tblName.getTbl() + "]");
            }
        }
    }

    private void setJob() throws UserException {
        exportJob = new ExportJob();

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(this.tblName.getDb());
        exportJob.setDbId(db.getId());
        exportJob.setTableName(this.tblName);
        exportJob.setExportTable(db.getTableOrDdlException(this.tblName.getTbl()));
        exportJob.setTableId(db.getTableOrDdlException(this.tblName.getTbl()).getId());

        // set partitions
        exportJob.setPartitionNames(this.partitionsNameList);

        // set where sql
        exportJob.setWhereSql(this.whereSql);

        // set path
        exportJob.setExportPath(this.path);

        // set properties
        exportJob.setLabel(this.label);
        exportJob.setColumnSeparator(this.columnSeparator);
        exportJob.setLineDelimiter(this.lineDelimiter);
        exportJob.setFormat(this.format);
        exportJob.setColumns(this.columns);
        exportJob.setParallelism(this.parallelism);
        exportJob.setMaxFileSize(this.maxFileSize);
        exportJob.setDeleteExistingFiles(this.deleteExistingFiles);

        if (columns != null) {
            Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();
            exportJob.setExportColumns(split.splitToList(this.columns.toLowerCase()));
        }

        // set broker desc
        exportJob.setBrokerDesc(this.brokerDesc);

        // set sessions
        exportJob.setQualifiedUser(this.qualifiedUser);
        exportJob.setUserIdentity(this.userIdentity);
        exportJob.setSessionVariables(this.sessionVariables);
        exportJob.setTimeoutSecond(this.sessionVariables.getQueryTimeoutS());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExportCommand(this, context);
    }
}
