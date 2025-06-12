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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * show create user command
 */
public class ShowCreateUserCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowCreateUserCommand.class);
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Create User", ScalarType.createVarchar(512)))
                    .build();
    private UserIdentity userIdent;
    private boolean showAllUser;

    /**
     * constructor for show create user
     */
    public ShowCreateUserCommand(UserIdentity userIdent, boolean showAllUser) {
        super(PlanType.SHOW_CREATE_USER_COMMAND);
        this.userIdent = userIdent;
        this.showAllUser = showAllUser;
    }

    private ShowResultSet handleShowCreateUser(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (userIdent != null) {
            if (showAllUser) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze();
        } else {
            if (!showAllUser) {
                // self
                userIdent = ConnectContext.get().getCurrentUserIdentity();
            }
        }
        Preconditions.checkState(showAllUser || userIdent != null);
        UserIdentity self = ConnectContext.get().getCurrentUserIdentity();

        // if show all grants, or show other user's grants, need global GRANT priv.
        if (showAllUser || !self.equals(userIdent)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
        if (userIdent != null && !Env.getCurrentEnv().getAccessManager().getAuth().doesUserExist(userIdent)) {
            throw new AnalysisException(String.format("User: %s does not exist", userIdent));
        }
        Set<String> users = Env.getCurrentEnv().getAuth().getAllUsers();
        List<List<String>> infos = Env.getCurrentEnv().getAuth().getAllUserInfo();
        for (String user : users) {
            List<User> alist = Env.getCurrentEnv().getAuth().getUserManager().getUserByName(user);
            for (User a : alist) {
                LOG.info("now user is : " + user + "; info is : " + a.toString());
            }
        }

        // order by UserIdentity
        infos.sort(Comparator.comparing(list -> list.isEmpty() ? "" : list.get(0)));
        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowCreateUser(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateUserCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}
