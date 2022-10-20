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
import org.apache.doris.common.Config;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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

    public static final String TABLET_NUMBER_PER_TASK_PROP = "tablet_num_per_task";
    public static final String LABEL = "label";

    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_COLUMNS = "";
    private TableName tblName;
    private List<String> partitions;
    private Expr whereExpr;
    private String path;
    private BrokerDesc brokerDesc;
    private Map<String, String> properties = Maps.newHashMap();
    private String columnSeparator;
    private String lineDelimiter;
    private String columns;

    private TableRef tableRef;

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
        this.columns = DEFAULT_COLUMNS;
    }

    public String getColumns() {
        return columns;
    }

    public TableName getTblName() {
        return tblName;
    }

    public List<String> getPartitions() {
        return partitions;
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

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getLineDelimiter() {
        return this.lineDelimiter;
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
            partitions = partitionNames.getPartitionNames();
        }

        // check auth
        if (!Env.getCurrentEnv().getAuth().checkTblPriv(ConnectContext.get(),
                                                                tblName.getDb(), tblName.getTbl(),
                                                                PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "EXPORT",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tblName.getDb() + ": " + tblName.getTbl());
        }

        // check table && partitions whether exist
        checkTable(analyzer.getEnv());

        // check broker whether exist
        if (brokerDesc == null) {
            brokerDesc = new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null);
        }

        // where expr will be checked in export job

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
            if (partitions == null) {
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

            for (String partitionName : partitions) {
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
                    && !schema.equalsIgnoreCase("ofs")
                    && !schema.equalsIgnoreCase("obs"))) {
                throw new AnalysisException("Invalid export path. please use valid 'HDFS://', 'AFS://' , 'BOS://', "
                    + "or 'ofs://' or 'obs://' path.");
            }
        } else if (type == StorageBackend.StorageType.S3) {
            if (schema == null || !schema.equalsIgnoreCase("s3")) {
                throw new AnalysisException("Invalid export path. please use valid 's3://' path.");
            }
        } else if (type == StorageBackend.StorageType.HDFS) {
            if (schema == null || !schema.equalsIgnoreCase("hdfs")) {
                throw new AnalysisException("Invalid export path. please use valid 'HDFS://' path.");
            }
        } else if (type == StorageBackend.StorageType.LOCAL) {
            if (schema != null && !schema.equalsIgnoreCase("file")) {
                throw new AnalysisException(
                        "Invalid export path. please use valid '" + OutFileClause.LOCAL_FILE_PREFIX + "' path.");
            }
            path = path.substring(OutFileClause.LOCAL_FILE_PREFIX.length() - 1);
        }
        return path;
    }

    private void checkProperties(Map<String, String> properties) throws UserException {
        this.columnSeparator = Separator.convertSeparator(PropertyAnalyzer.analyzeColumnSeparator(
                properties, ExportStmt.DEFAULT_COLUMN_SEPARATOR));
        this.lineDelimiter = Separator.convertSeparator(PropertyAnalyzer.analyzeLineDelimiter(
                properties, ExportStmt.DEFAULT_LINE_DELIMITER));
        this.columns = properties.get(LoadStmt.KEY_IN_PARAM_COLUMNS);
        // exec_mem_limit
        if (properties.containsKey(LoadStmt.EXEC_MEM_LIMIT)) {
            try {
                Long.parseLong(properties.get(LoadStmt.EXEC_MEM_LIMIT));
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid exec_mem_limit value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(LoadStmt.EXEC_MEM_LIMIT,
                           String.valueOf(ConnectContext.get().getSessionVariable().getMaxExecMemByte()));
        }
        // timeout
        if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            try {
                Long.parseLong(properties.get(LoadStmt.TIMEOUT_PROPERTY));
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid timeout value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(LoadStmt.TIMEOUT_PROPERTY, String.valueOf(Config.export_task_default_timeout_second));
        }

        // tablet num per task
        if (properties.containsKey(TABLET_NUMBER_PER_TASK_PROP)) {
            try {
                Long.parseLong(properties.get(TABLET_NUMBER_PER_TASK_PROP));
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid tablet num per task value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(TABLET_NUMBER_PER_TASK_PROP, String.valueOf(Config.export_tablet_num_per_task));
        }

        if (properties.containsKey(LABEL)) {
            FeNameFormat.checkLabel(properties.get(LABEL));
        } else {
            // generate a random label
            String label = "export_" + UUID.randomUUID().toString();
            properties.put(LABEL, label);
        }
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
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(" PARTITION (");
            Joiner.on(", ").appendTo(sb, partitions);
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
