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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * show create mtmv info
 */
public class ShowCreateMTMVInfo {
    private final TableNameInfo mvName;

    public ShowCreateMTMVInfo(TableNameInfo mvName) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
    }

    /**
     * analyze resume info
     *
     * @param ctx ConnectContext
     */
    public void analyze(ConnectContext ctx) {
        mvName.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), mvName.getCtl(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.SHOW)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SHOW",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    mvName.getDb() + ": " + mvName.getTbl());
            throw new AnalysisException(message);
        }
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(mvName.getDb());
            db.getTableOrMetaException(mvName.getTbl(), TableType.MATERIALIZED_VIEW);
        } catch (MetaNotFoundException | DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    /**
     * run show create materialized view
     *
     * @param executor executor
     * @throws DdlException DdlException
     * @throws IOException IOException
     */
    public void run(StmtExecutor executor) throws DdlException, IOException {
        List<List<String>> rows = Lists.newArrayList();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(mvName.getDb());
        MTMV mtmv = (MTMV) db.getTableOrDdlException(mvName.getTbl());
        mtmv.readLock();
        try {
            String mtmvDdl = Env.getMTMVDdl(mtmv);
            rows.add(Lists.newArrayList(mtmv.getName(), mtmvDdl));
            executor.handleShowCreateMTMVStmt(rows);
        } finally {
            mtmv.readUnlock();
        }
    }

    /**
     * getMvName
     *
     * @return TableNameInfo
     */
    public TableNameInfo getMvName() {
        return mvName;
    }

    @Override
    public String toString() {
        return "ShowCreateMTMVInfo{"
                + "mvName=" + mvName
                + '}';
    }
}
