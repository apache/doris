// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.BrokerMgr;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.util.PrintableMap;
import com.baidu.palo.common.util.PropertyAnalyzer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

// EXPORT statement, export data to dirs by broker.
//
// syntax:
//      EXPORT TABLE tablename [PARTITION (name1[, ...])]
//          TO 'export_target_path'
//          [PROPERTIES("key"="value")]
//          BY BROKER 'broker_name' [( $broker_attrs)]
public class ExportStmt extends StatementBase {
    private final static Logger LOG = LogManager.getLogger(ExportStmt.class);

    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";

    private TableName tblName;
    private List<String> partitions;
    private final String path;
    private final BrokerDesc brokerDesc;
    private final Map<String, String> properties;
    private String columnSeparator;
    private String lineDelimiter;

    private TableRef tableRef;

    private String user;

    public ExportStmt(TableRef tableRef, String path,
                      Map<String, String> properties, BrokerDesc brokerDesc) {
        this.tableRef = tableRef;
        this.path = path.trim();
        this.properties = properties;
        this.brokerDesc = brokerDesc;
        this.columnSeparator = DEFAULT_COLUMN_SEPARATOR;
        this.lineDelimiter = DEFAULT_LINE_DELIMITER;

        this.user = null;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public TableName getTblName() {
        return tblName;
    }

    public List<String> getPartitions() {
        return partitions;
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

    public String getUser() {
        return user;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getLineDelimiter() {
        return this.lineDelimiter;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);

        tableRef = analyzer.resolveTableRef(tableRef);
        Preconditions.checkNotNull(tableRef);
        tableRef.analyze(analyzer);

        this.tblName = tableRef.getName();
        this.partitions = tableRef.getPartitions();

        // check auth
        user = analyzer.getUser();
        if (!analyzer.getCatalog().getUserMgr().checkAccess(user, tblName.getDb(), AccessPrivilege.READ_ONLY)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, user, tblName.getDb());
        }

        // check table && partitions whether exist
        checkTable(analyzer.getCatalog());

        // check path is valid
        checkPath(path);

        // check broker whether exist
        if (brokerDesc == null) {
            throw new AnalysisException("broker is not provided");
        }
        BrokerMgr.BrokerAddress address = analyzer.getCatalog().getBrokerMgr().getAnyBroker(brokerDesc.getName());
        if (address == null) {
            throw new AnalysisException("broker is not exist");
        }

        // check properties
        checkProperties(properties);
    }

    private void checkTable(Catalog catalog) throws AnalysisException {
        Database db = catalog.getDb(tblName.getDb());
        if (db == null) {
            throw new AnalysisException("Db does not exist. name: " + tblName.getDb());
        }

        db.readLock();
        try {
            Table table = db.getTable(tblName.getTbl());
            if (table == null) {
                throw new AnalysisException("Table[" + tblName.getTbl() + "] does not exist");
            }

            if (partitions == null) {
                return;
            }
            if (!table.isPartitioned()) {
                throw new AnalysisException("Table[" + tblName.getTbl() + "] is not partitioned.");
            }
            Table.TableType tblType = table.getType();
            switch (tblType) {
                case MYSQL:
                case OLAP:
                case BROKER:
                case KUDU:
                    break;
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
            db.readUnlock();
        }

        return;
    }

    private static void checkPath(String path) throws AnalysisException {
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException("No dest path specified.");
        }

        String upperCasePath = path.toUpperCase();
        if (upperCasePath.startsWith("BOS://")
                || upperCasePath.startsWith("HDFS://")) {
            return;
        }

        throw new AnalysisException("Invalid export path. please use valid 'HDFS://' or 'BOS://' path.");
    }

    private void checkProperties(Map<String, String> properties) throws AnalysisException {
        this.columnSeparator = PropertyAnalyzer.analyzeColumnSeparator(
                properties, ExportStmt.DEFAULT_COLUMN_SEPARATOR);
        this.lineDelimiter = PropertyAnalyzer.analyzeLineDelimiter(properties, ExportStmt.DEFAULT_LINE_DELIMITER);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPORT TABLE ").append(tblName.toSql());
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
            sb.append(new PrintableMap<String, String>(brokerDesc.getProperties(), "=", true, false));
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
