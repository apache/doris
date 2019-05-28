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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcNode;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * ShowAlterStmt: used to show process state of alter statement.
 * Syntax:
 *      SHOW ALTER [COLUMN | ROLLUP] [FROM dbName]
 */
public class ShowAlterStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowAlterStmt.class);

    public static enum AlterType {
        COLUMN, ROLLUP
    }

    private AlterType type;
    private String dbName;

    private ProcNodeInterface node;

    public AlterType getType() {
        return type;
    }

    public String getDbName() {
        return dbName;
    }

    public ProcNodeInterface getNode() {
        return this.node;
    }

    public ShowAlterStmt(AlterType type, String dbName) {
        this.type = type;
        this.dbName = dbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        Preconditions.checkNotNull(type);

        // check auth when get job info

        handleShowAlterTable(analyzer);
    }

    private void handleShowAlterTable(Analyzer analyzer) throws AnalysisException, UserException {
        final String dbNameWithoutPrefix = ClusterNamespace.getNameFromFullName(dbName);
        Database db = analyzer.getCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbNameWithoutPrefix);
        }

        // build proc path
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        if (type == AlterType.COLUMN) {
            sb.append("/schema_change");
        } else if (type == AlterType.ROLLUP) {
            sb.append("/rollup");
        } else {
            throw new UserException("SHOW " + type.name() + " does not implement yet");
        }

        LOG.debug("process SHOW PROC '{}';", sb.toString());
        // create show proc stmt
        // '/jobs/db_name/rollup|schema_change/
        node = ProcService.getInstance().open(sb.toString());
        if (node == null) {
            throw new AnalysisException("Failed to show alter table");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ALTER ").append(type.name()).append(" ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ImmutableList<String> titleNames = null;
        if (type == AlterType.ROLLUP) {
            titleNames = RollupProcDir.TITLE_NAMES;
        } else if (type == AlterType.COLUMN) {
            titleNames = SchemaChangeProcNode.TITLE_NAMES;
        }

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }

        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
