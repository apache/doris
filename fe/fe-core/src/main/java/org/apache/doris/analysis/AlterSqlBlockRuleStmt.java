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

import org.apache.doris.block.SqlBlockRule;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AlterSqlBlockRuleStmt extends DdlStmt {

    private final String ruleName;

    private String user;

    private String sql;

    private Boolean enable;

    private final Map<String, String> properties;

    public AlterSqlBlockRuleStmt(String ruleName, Map<String, String> properties) {
        this.ruleName = ruleName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check properties
        checkProperties(properties);
    }

    private void checkProperties(Map<String, String> properties) throws UserException {
        this.user = properties.get(CreateSqlBlockRuleStmt.USER_PROPERTY);
        // if not default, need check whether user exist
        if (StringUtils.isNotEmpty(user) &&  !SqlBlockRule.DEFAULT_USER.equals(user)) {
            List<String> allUserIdents = Catalog.getCurrentCatalog().getAuth().getAllUserIdents(false).stream().map(UserIdentity::getUser).collect(Collectors.toList());
            if (!allUserIdents.contains(user)) {
                throw new AnalysisException(user + " is not exist");
            }
        }
        this.sql = properties.get(CreateSqlBlockRuleStmt.SQL_PROPERTY);
        String enableStr = properties.get(CreateSqlBlockRuleStmt.ENABLE_PROPERTY);
        this.enable = StringUtils.isNotEmpty(enableStr) ? Boolean.parseBoolean(enableStr) : null;
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getUser() {
        return user;
    }

    public String getSql() {
        return sql;
    }

    public Boolean getEnable() {
        return enable;
    }
}
