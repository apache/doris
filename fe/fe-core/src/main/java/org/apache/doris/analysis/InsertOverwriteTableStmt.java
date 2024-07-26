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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class InsertOverwriteTableStmt extends DdlStmt {

    private final InsertTarget target;

    @Getter
    private final String label;

    @Getter
    private final  List<String> cols;

    private final InsertSource source;

    @Getter
    private final List<String> hints;

    public InsertOverwriteTableStmt(InsertTarget target, String label, List<String> cols, InsertSource source,
            List<String> hints) {
        this.target = target;
        this.label = label;
        this.cols = cols;
        this.source = source;
        this.hints = hints;
    }

    public String getDb() {
        return target.getTblName().getDb();
    }

    public String getTbl() {
        return target.getTblName().getTbl();
    }

    public String getCtl() {
        return target.getTblName().getCtl();
    }

    public QueryStmt getQueryStmt() {
        return source.getQueryStmt();
    }

    public List<String> getPartitionNames() {
        if (target.getPartitionNames() == null) {
            return new ArrayList<>();
        }
        return target.getPartitionNames().getPartitionNames();
    }

    /*
     * auto detect which partitions to replace. enable by partition(*) grammer
     */
    public boolean isAutoDetectPartition() {
        return target.getPartitionNames().isStar();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        target.getTblName().analyze(analyzer);
        InternalDatabaseUtil.checkDatabase(getDb(), ConnectContext.get());
        TableIf tableIf = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(getCtl())
                .getDbOrAnalysisException(getDb()).getTableOrAnalysisException(getTbl());
        if (tableIf instanceof MTMV && !MTMVUtil.allowModifyMTMVData(ConnectContext.get())) {
            throw new DdlException("Not allowed to perform current operation on async materialized view");
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), target.getTblName().getCtl(), getDb(), getTbl(),
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    getDb() + ": " + getTbl());
        }
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }

}
