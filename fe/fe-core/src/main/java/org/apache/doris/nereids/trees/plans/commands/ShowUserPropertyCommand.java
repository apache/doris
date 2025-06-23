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

import org.apache.doris.analysis.SetUserPropertyVar;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.proc.UserPropertyProcNode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the command for SHOW ALL/USER PROPERTIES COMMAND.
 */
public class ShowUserPropertyCommand extends ShowCommand {
    private String user;
    private String pattern;
    private boolean isAll;

    public ShowUserPropertyCommand(String user, String pattern, boolean isAll) {
        super(PlanType.SHOW_USER_PROPERTY_COMMAND);
        this.user = user;
        this.pattern = pattern;
        this.isAll = isAll;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        boolean needCheckAuth = true;
        if (!Strings.isNullOrEmpty(user)) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
        } else {
            if (!isAll) {
                // self
                user = ctx.getQualifiedUser();
                // user can see itself's property, no need to check privs
                needCheckAuth = false;
            }
        }

        if (needCheckAuth) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }

        pattern = Strings.emptyToNull(pattern);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowUserPropertyCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return new ShowResultSet(getMetaData(), getRows());
    }

    private List<List<String>> getRows() throws AnalysisException {
        return isAll ? getRowsForAllUser() : getRowsForOneUser();
    }

    private List<List<String>> getRowsForOneUser() throws AnalysisException {
        List<List<String>> rows = Env.getCurrentEnv().getAuth().getUserProperties(user);

        if (pattern == null) {
            return rows;
        }

        List<List<String>> result = Lists.newArrayList();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(pattern,
                CaseSensibility.USER.getCaseSensibility());
        for (List<String> row : rows) {
            String key = row.get(0).split("\\" + SetUserPropertyVar.DOT_SEPARATOR)[0];
            if (matcher.match(key)) {
                result.add(row);
            }
        }

        return result;
    }

    private List<List<String>> getRowsForAllUser() throws AnalysisException {
        Set<String> allUser = Env.getCurrentEnv().getAuth().getAllUser();
        List<List<String>> result = Lists.newArrayListWithCapacity(allUser.size());

        for (String user : allUser) {
            List<String> row = Lists.newArrayListWithCapacity(2);
            row.add(user);
            row.add(GsonUtils.GSON.toJson(getRowsForUser(user)));
            result.add(row);
        }
        return result;
    }

    private Map<String, String> getRowsForUser(String user) throws AnalysisException {
        Map<String, String> result = Maps.newHashMap();
        List<List<String>> userProperties = Env.getCurrentEnv().getAuth()
                .getUserProperties(user);
        PatternMatcher matcher = null;
        if (pattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(pattern,
                    CaseSensibility.USER.getCaseSensibility());
        }

        for (List<String> row : userProperties) {
            String key = row.get(0).split("\\" + SetUserPropertyVar.DOT_SEPARATOR)[0];
            if (matcher == null || matcher.match(key)) {
                result.put(row.get(0), row.get(1));
            }
        }
        return result;
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : isAll ? UserPropertyProcNode.ALL_USER_TITLE_NAMES : UserPropertyProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
