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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.load.ExportJob;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.List;
import java.util.Map;

// EXPORT statement, export data to dirs by broker.
//
// syntax:
//      EXPORT TABLE table_name [PARTITION (name1[, ...])]
//          TO 'export_target_path'
//          [PROPERTIES("key"="value")]
//          WITH BROKER 'broker_name' [( $broker_attrs)]
@Getter
public class ExportStmt extends StatementBase implements NotFallbackInParser {
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
    public void analyze() throws UserException {

    }

    @Override
    public String toSql() {
        return "";
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXPORT;
    }
}
