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

import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.ExportJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

// EXPORT statement, export data to dirs by broker.
//
// syntax:
//      EXPORT TABLE table_name [PARTITION (name1[, ...])]
//          TO 'export_target_path'
//          [PROPERTIES("key"="value")]
//          WITH BROKER 'broker_name' [( $broker_attrs)]
@Getter
public class ExportStmt extends StatementBase {
    public static final String PARALLELISM = "parallelism";
    public static final String LABEL = "label";
    public static final String DATA_CONSISTENCY = "data_consistency";
    public static final String COMPRESS_TYPE = "compress_type";

    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_PARALLELISM = "1";
    private static final Integer DEFAULT_TIMEOUT = 7200;

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LABEL)
            .add(PARALLELISM)
            .add(DATA_CONSISTENCY)
            .add(LoadStmt.KEY_IN_PARAM_COLUMNS)
            .add(OutFileClause.PROP_MAX_FILE_SIZE)
            .add(OutFileClause.PROP_DELETE_EXISTING_FILES)
            .add(PropertyAnalyzer.PROPERTIES_COLUMN_SEPARATOR)
            .add(PropertyAnalyzer.PROPERTIES_LINE_DELIMITER)
            .add(PropertyAnalyzer.PROPERTIES_TIMEOUT)
            .add("format")
            .add(COMPRESS_TYPE)
            .build();

    private TableName tblName;
    private List<String> partitionStringNames;
    private Expr whereExpr;
    private String path;
    private BrokerDesc brokerDesc;
    private Map<String, String> properties = Maps.newHashMap();
    private String columnSeparator;
    private String lineDelimiter;
    private String columns;

    private TableRef tableRef;

    private String format;

    private String label;

    private Integer parallelism;

    private Integer timeout;

    private String maxFileSize;
    private String deleteExistingFiles;
    private String withBom;
    private String dataConsistency = ExportJob.CONSISTENT_PARTITION;
    private String compressionType;
    private SessionVariable sessionVariables;

    private String qualifiedUser;

    private UserIdentity userIdentity;

    private ExportJob exportJob;

    public ExportStmt(TableRef tableRef, Expr whereExpr, String path,
                      Map<String, String> properties, BrokerDesc brokerDesc) {
        this.tableRef = tableRef;
        this.whereExpr = whereExpr;
        this.path = path.trim();
        if (properties != null) {
            this.properties = properties;
        }
        this.brokerDesc = brokerDesc;
        this.columnSeparator = DEFAULT_COLUMN_SEPARATOR;
        this.lineDelimiter = DEFAULT_LINE_DELIMITER;
        this.timeout = DEFAULT_TIMEOUT;

        // ConnectionContext may not exist when in replay thread
        if (ConnectContext.get() != null) {
            this.sessionVariables = VariableMgr.cloneSessionVariable(ConnectContext.get().getSessionVariable());
        } else {
            this.sessionVariables = VariableMgr.cloneSessionVariable(VariableMgr.getDefaultSessionVariable());
        }
    }

    @Override
    public boolean needAuditEncryption() {
        return brokerDesc != null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!Config.enable_outfile_to_local && Objects.requireNonNull(path)
                .startsWith(OutFileClause.LOCAL_FILE_PREFIX)) {
            throw new AnalysisException("`enable_outfile_to_local` = false, exporting file to local fs is disabled.");
        }

        tableRef = analyzer.resolveTableRef(tableRef);
        Preconditions.checkNotNull(tableRef);
        tableRef.analyze(analyzer);

        // disallow external catalog
        tblName = tableRef.getName();
        Util.prohibitExternalCatalog(tblName.getCtl(), this.getClass().getSimpleName());

        // get partitions name
        Optional<PartitionNames> optionalPartitionNames = Optional.ofNullable(tableRef.getPartitionNames());
        if (optionalPartitionNames.isPresent()) {
            if (optionalPartitionNames.get().isTemp()) {
                throw new AnalysisException("Do not support exporting temporary partitions");
            }
            partitionStringNames = optionalPartitionNames.get().getPartitionNames();
        } else {
            partitionStringNames = ImmutableList.of();
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), tblName.getCtl(),
                tblName.getDb(), tblName.getTbl(),
                PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "EXPORT",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tblName.getDb() + ": " + tblName.getTbl());
        }
        qualifiedUser = ConnectContext.get().getQualifiedUser();
        userIdentity = ConnectContext.get().getCurrentUserIdentity();

        // check table && partitions whether exist
        checkPartitions(analyzer.getEnv());

        // check broker whether exist
        if (brokerDesc == null) {
            brokerDesc = new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null);
        }

        // check path is valid
        StorageBackend.checkPath(path, brokerDesc.getStorageType(), null);
        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            BrokerMgr brokerMgr = analyzer.getEnv().getBrokerMgr();
            if (!brokerMgr.containsBroker(brokerDesc.getName())) {
                throw new AnalysisException("broker " + brokerDesc.getName() + " does not exist");
            }
            if (null == brokerMgr.getAnyBroker(brokerDesc.getName())) {
                throw new AnalysisException("failed to get alive broker");
            }
        }

        // check properties
        checkProperties(properties);

        // create job and analyze job
        setJob();
        exportJob.generateOutfileStatement();
    }

    private void setJob() throws UserException {
        exportJob = new ExportJob(Env.getCurrentEnv().getNextId());

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(this.tblName.getDb());
        exportJob.setDbId(db.getId());
        exportJob.setTableName(this.tblName);
        exportJob.setExportTable(db.getTableOrDdlException(this.tblName.getTbl()));
        exportJob.setTableId(db.getTableOrDdlException(this.tblName.getTbl()).getId());
        exportJob.setTableRef(this.tableRef);

        // set partitions
        exportJob.setPartitionNames(this.partitionStringNames);

        // set where expr
        exportJob.setWhereExpr(this.whereExpr);

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
        exportJob.setWithBom(this.withBom);
        exportJob.setDataConsistency(this.dataConsistency);
        exportJob.setCompressType(this.compressionType);

        if (columns != null) {
            Splitter split = Splitter.on(',').trimResults().omitEmptyStrings();
            exportJob.setExportColumns(split.splitToList(this.columns.toLowerCase()));
        }

        // set broker desc
        exportJob.setBrokerDesc(this.brokerDesc);

        // set sessions
        exportJob.setQualifiedUser(this.qualifiedUser);
        exportJob.setUserIdentity(this.userIdentity);
        SessionVariable clonedSessionVariable = VariableMgr.cloneSessionVariable(Optional.ofNullable(
                ConnectContext.get().getSessionVariable()).orElse(VariableMgr.getDefaultSessionVariable()));
        exportJob.setSessionVariables(clonedSessionVariable);
        exportJob.setTimeoutSecond(this.timeout);

        exportJob.setOrigStmt(this.getOrigStmt());
    }

    // check partitions specified by user are belonged to the table.
    private void checkPartitions(Env env) throws AnalysisException {
        if (partitionStringNames.isEmpty()) {
            return;
        }

        if (partitionStringNames.size() > Config.maximum_number_of_export_partitions) {
            throw new AnalysisException("The partitions number of this export job is larger than the maximum number"
                    + " of partitions allowed by an export job");
        }

        Database db = env.getInternalCatalog().getDbOrAnalysisException(tblName.getDb());
        Table table = db.getTableOrAnalysisException(tblName.getTbl());
        table.readLock();
        try {
            // check table
            if (!table.isPartitionedTable()) {
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

            for (String partitionName : partitionStringNames) {
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
                properties, ExportStmt.DEFAULT_COLUMN_SEPARATOR));
        this.lineDelimiter = Separator.convertSeparator(PropertyAnalyzer.analyzeLineDelimiter(
                properties, ExportStmt.DEFAULT_LINE_DELIMITER));
        // null means not specified
        // "" means user specified zero columns
        this.columns = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_COLUMNS, null);

        // format
        this.format = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_FORMAT_TYPE, "csv").toLowerCase();

        // parallelism
        String parallelismString = properties.getOrDefault(PARALLELISM, DEFAULT_PARALLELISM);
        try {
            this.parallelism = Integer.parseInt(parallelismString);
        } catch (NumberFormatException e) {
            throw new UserException("The value of parallelism is invalid!");
        }

        // timeout
        String timeoutString = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_TIMEOUT,
                String.valueOf(DEFAULT_TIMEOUT));
        try {
            this.timeout = Integer.parseInt(timeoutString);
        } catch (NumberFormatException e) {
            throw new UserException("The value of timeout is invalid!");
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

        // with bom
        this.withBom = properties.getOrDefault(OutFileClause.PROP_WITH_BOM, "false");

        // data consistency
        if (properties.containsKey(DATA_CONSISTENCY)) {
            String dataConsistencyStr = properties.get(DATA_CONSISTENCY);
            if (ExportJob.CONSISTENT_NONE.equalsIgnoreCase(dataConsistencyStr)) {
                this.dataConsistency = ExportJob.CONSISTENT_NONE;
            } else if (ExportJob.CONSISTENT_PARTITION.equalsIgnoreCase(dataConsistencyStr)) {
                this.dataConsistency = ExportJob.CONSISTENT_PARTITION;
            } else {
                throw new AnalysisException("The value of data_consistency is invalid, please use `"
                        + ExportJob.CONSISTENT_PARTITION + "`/`" + ExportJob.CONSISTENT_NONE + "`");
            }
        }

        // compress_type
        this.compressionType = properties.getOrDefault(COMPRESS_TYPE, "");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPORT TABLE ");
        if (tblName == null) {
            sb.append("non-exist");
        } else {
            sb.append(tblName.toSql());
        }
        if (partitionStringNames != null && !partitionStringNames.isEmpty()) {
            sb.append(" PARTITION (");
            Joiner.on(", ").appendTo(sb, partitionStringNames);
            sb.append(")");
        }
        sb.append("\n");

        sb.append(" TO ").append("'");
        sb.append(path);
        sb.append("'");

        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }

        if (brokerDesc != null) {
            sb.append("\n WITH BROKER '").append(brokerDesc.getName()).append("' (");
            sb.append(new PrintableMap<String, String>(brokerDesc.getProperties(), "=", true, false, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
