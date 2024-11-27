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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.UserPropertyProcNode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

// Show Property Stmt
//  syntax:
//      SHOW [ALL] PROPERTY [FOR user] [LIKE key pattern]
public class ShowUserPropertyStmt extends ShowStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowUserPropertyStmt.class);

    private String user;
    private String pattern;
    private boolean isAll;

    public ShowUserPropertyStmt(String user, String pattern, boolean isAll) {
        this.user = user;
        this.pattern = pattern;
        this.isAll = isAll;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        boolean needCheckAuth = true;
        if (!Strings.isNullOrEmpty(user)) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
        } else {
            if (!isAll) {
                // self
                user = analyzer.getQualifiedUser();
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

    public List<List<String>> getRows() throws AnalysisException {
        return isAll ? getRowsForAllUser() : getRowsForOneUser();
    }

    public List<List<String>> getRowsForOneUser() throws AnalysisException {
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

    public List<List<String>> getRowsForAllUser() throws AnalysisException {
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

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : isAll ? UserPropertyProcNode.ALL_USER_TITLE_NAMES : UserPropertyProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ");
        if (isAll) {
            sb.append("ALL PROPERTIES");
        } else {
            sb.append("PROPERTY FOR '");
            sb.append(user);
            sb.append("'");
        }
        if (pattern != null) {
            sb.append(" LIKE '");
            sb.append(pattern);
            sb.append("'");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
