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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.URI;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

// EXPORT statement, export data to dirs by broker.
//
// syntax:
//      EXPORT TABLE tablename [PARTITION (name1[, ...])]
//          TO 'export_target_path'
//          [PROPERTIES("key"="value")]
//          BY BROKER 'broker_name' [( $broker_attrs)]
public class ExportStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(ExportStmt.class);
    public static final String PARALLELISM = "parallelism";
    public static final String LABEL = "label";

    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_COLUMNS = "";
    private static final String DEFAULT_PARALLELISM = "1";
    private static final Integer DEFAULT_TIMEOUT = 7200;

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LABEL)
            .add(PARALLELISM)
            .add(LoadStmt.EXEC_MEM_LIMIT)
            .add(LoadStmt.TIMEOUT_PROPERTY)
            .add(LoadStmt.KEY_IN_PARAM_COLUMNS)
            .add(OutFileClause.PROP_MAX_FILE_SIZE)
            .add(OutFileClause.PROP_DELETE_EXISTING_FILES)
            .add(PropertyAnalyzer.PROPERTIES_COLUMN_SEPARATOR)
            .add(PropertyAnalyzer.PROPERTIES_LINE_DELIMITER)
            .add("format")
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
    private SessionVariable sessionVariables;

    private String qualifiedUser;

    private UserIdentity userIdentity;

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
        this.columns = DEFAULT_COLUMNS;
        if (ConnectContext.get() != null) {
            this.sessionVariables = ConnectContext.get().getSessionVariable();
        } else {
            this.sessionVariables = VariableMgr.getDefaultSessionVariable();
        }
    }

    public String getColumns() {
        return columns;
    }

    public TableName getTblName() {
        return tblName;
    }

    public List<String> getPartitions() {
        return partitionStringNames;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public String getPath() {
        return path;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getLineDelimiter() {
        return this.lineDelimiter;
    }

    public TableRef getTableRef() {
        return this.tableRef;
    }

    public String getFormat() {
        return format;
    }

    public String getLabel() {
        return label;
    }

    public SessionVariable getSessionVariables() {
        return sessionVariables;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public UserIdentity getUserIdentity() {
        return this.userIdentity;
    }

    @Override
    public boolean needAuditEncryption() {
        if (brokerDesc != null) {
            return true;
        }
        return false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        tableRef = analyzer.resolveTableRef(tableRef);
        Preconditions.checkNotNull(tableRef);
        tableRef.analyze(analyzer);

        this.tblName = tableRef.getName();
        // disallow external catalog
        Util.prohibitExternalCatalog(tblName.getCtl(), this.getClass().getSimpleName());

        PartitionNames partitionNames = tableRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support exporting temporary partitions");
            }
            partitionStringNames = partitionNames.getPartitionNames();
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
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
        checkTable(analyzer.getEnv());

        // check broker whether exist
        if (brokerDesc == null) {
            brokerDesc = new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null);
        }

        // check path is valid
        path = checkPath(path, brokerDesc.getStorageType());
        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            if (!analyzer.getEnv().getBrokerMgr().containsBroker(brokerDesc.getName())) {
                throw new AnalysisException("broker " + brokerDesc.getName() + " does not exist");
            }

            FsBroker broker = analyzer.getEnv().getBrokerMgr().getAnyBroker(brokerDesc.getName());
            if (broker == null) {
                throw new AnalysisException("failed to get alive broker");
            }
        }

        // check properties
        checkProperties(properties);
    }

    private void checkTable(Env env) throws AnalysisException {
        Database db = env.getInternalCatalog().getDbOrAnalysisException(tblName.getDb());
        Table table = db.getTableOrAnalysisException(tblName.getTbl());
        table.readLock();
        try {
            if (partitionStringNames == null) {
                return;
            }
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
                            + tblType.toString() + " type, do not support EXPORT.");
            }

            for (String partitionName : partitionStringNames) {
                Partition partition = table.getPartition(partitionName);
                if (partition == null) {
                    throw new AnalysisException("Partition [" + partitionName + "] does not exist");
                }
            }
        } finally {
            table.readUnlock();
        }
    }

    public static String checkPath(String path, StorageBackend.StorageType type) throws AnalysisException {
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException("No dest path specified.");
        }

        URI uri = URI.create(path);
        String schema = uri.getScheme();
        if (type == StorageBackend.StorageType.BROKER) {
            if (schema == null || (!schema.equalsIgnoreCase("bos")
                    && !schema.equalsIgnoreCase("afs")
                    && !schema.equalsIgnoreCase("hdfs")
                    && !schema.equalsIgnoreCase("viewfs")
                    && !schema.equalsIgnoreCase("ofs")
                    && !schema.equalsIgnoreCase("obs")
                    && !schema.equalsIgnoreCase("oss")
                    && !schema.equalsIgnoreCase("s3a")
                    && !schema.equalsIgnoreCase("cosn")
                    && !schema.equalsIgnoreCase("gfs")
                    && !schema.equalsIgnoreCase("jfs")
                    && !schema.equalsIgnoreCase("gs"))) {
                throw new AnalysisException("Invalid broker path. please use valid 'hdfs://', 'viewfs://', 'afs://',"
                        + " 'bos://', 'ofs://', 'obs://', 'oss://', 's3a://', 'cosn://', 'gfs://', 'gs://'"
                        + " or 'jfs://' path.");
            }
        } else if (type == StorageBackend.StorageType.S3) {
            if (schema == null || !schema.equalsIgnoreCase("s3")) {
                throw new AnalysisException("Invalid export path. please use valid 's3://' path.");
            }
        } else if (type == StorageBackend.StorageType.HDFS) {
            if (schema == null || (!schema.equalsIgnoreCase("hdfs") && !schema.equalsIgnoreCase("viewfs"))) {
                throw new AnalysisException("Invalid export path. please use valid 'HDFS://' or 'viewfs://' path.");
            }
        } else if (type == StorageBackend.StorageType.LOCAL) {
            if (schema != null && !schema.equalsIgnoreCase("file")) {
                throw new AnalysisException(
                        "Invalid export path. please use valid '" + OutFileClause.LOCAL_FILE_PREFIX + "' path.");
            }
        }
        return path;
    }

    private void checkProperties(Map<String, String> properties) throws UserException {
        for (String key : properties.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new DdlException("Invalid property key: '" + key + "'");
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
        this.columns = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_COLUMNS, DEFAULT_COLUMNS);

        // format
        this.format = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_FORMAT_TYPE, "csv").toLowerCase();

        // parallelism
        String parallelismString = properties.getOrDefault(PARALLELISM, DEFAULT_PARALLELISM);
        parallelism = Integer.parseInt(parallelismString);

        // max_file_size
        this.maxFileSize = properties.getOrDefault(OutFileClause.PROP_MAX_FILE_SIZE, "");
        this.deleteExistingFiles = properties.getOrDefault(OutFileClause.PROP_DELETE_EXISTING_FILES, "");

        // timeout
        String timeoutString = properties.getOrDefault(LoadStmt.TIMEOUT_PROPERTY,
                String.valueOf(DEFAULT_TIMEOUT));
        try {
            this.timeout = Integer.parseInt(timeoutString);
        } catch (NumberFormatException e) {
            throw new UserException("The value of timeout is invalid!");
        }

        if (properties.containsKey(LABEL)) {
            FeNameFormat.checkLabel(properties.get(LABEL));
        } else {
            // generate a random label
            String label = "export_" + UUID.randomUUID();
            properties.put(LABEL, label);
        }
        label = properties.get(LABEL);
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

    public String getMaxFileSize() {
        return maxFileSize;
    }

    public String getDeleteExistingFiles() {
        return deleteExistingFiles;
    }

    public Integer getParallelNum() {
        return parallelism;
    }

    public Integer getTimeout() {
        return timeout;
    }
}
