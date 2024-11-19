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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.AuthProcDir;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * show grants command
 */
public class ShowGrantsCommand extends ShowCommand {
    public static final Logger LOG = LogManager.getLogger(ShowGrantsCommand.class);
    private static final ShowResultSetMetaData META_DATA;
    private final boolean isAll;
    private UserIdentity userIdent; // if not given will update with self.

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : AuthProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(100)));
        }
        META_DATA = builder.build();
    }

    /**
     * constructor
     */
    public ShowGrantsCommand(UserIdentity userIdent, boolean isAll) {
        super(PlanType.SHOW_GRANTS_COMMAND);
        this.userIdent = userIdent;
        this.isAll = isAll;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (userIdent != null) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze();
        } else {
            if (!isAll) {
                // self
                userIdent = ConnectContext.get().getCurrentUserIdentity();
            }
        }
        Preconditions.checkState(isAll || userIdent != null);
        UserIdentity self = ConnectContext.get().getCurrentUserIdentity();

        // if show all grants, or show other user's grants, need global GRANT priv.
        if (isAll || !self.equals(userIdent)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
        if (userIdent != null && !Env.getCurrentEnv().getAccessManager().getAuth().doesUserExist(userIdent)) {
            throw new AnalysisException(String.format("User: %s does not exist", userIdent));
        }
        List<List<String>> infos = Env.getCurrentEnv().getAuth().getAuthInfo(userIdent);
        return new ShowResultSet(META_DATA, infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowGrantsCommand(this, context);
    }

}
