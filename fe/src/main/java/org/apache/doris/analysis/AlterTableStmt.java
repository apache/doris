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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

// Alter table statement.
public class AlterTableStmt extends DdlStmt implements Writable {
    private TableName tbl;
    private List<AlterClause> ops;

    public AlterTableStmt() {
        // for persist
        tbl = new TableName();
        ops = Lists.newArrayList();
    }

    public TableName getTbl() {
        return tbl;
    }

    public List<AlterClause> getOps() {
        return ops;
    }

    public AlterTableStmt(TableName tbl, List<AlterClause> ops) {
        this.tbl = tbl;
        this.ops = ops;
    }

    public void setTableName(String newTableName) {
        tbl = new TableName(tbl.getDb(), newTableName);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(analyzer);
        if (ops == null || ops.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        for (AlterClause op : ops) {
            op.analyze(analyzer);
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tbl.getDb(), tbl.getTbl(),
                                                                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tbl.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(tbl.toSql()).append(" ");
        int idx = 0;
        for (AlterClause op : ops) {
            if (idx != 0) {
                sb.append(", \n");
            }
            sb.append(op.toSql());
            idx++;
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tbl.write(out);
        int count = ops.size();
        out.writeInt(count);
        for (AlterClause alterClause : ops) {
            alterClause.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tbl.readFields(in);
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            AlterClause alterClause = AlterClause.read(in);
            ops.add(alterClause);
        }
    }
}
