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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/*
 Create sqlBlockRule statement

 syntax:
      CREATE SQL_BLOCK_RULE LOAD NAME PROPERTIES
      (
          user = default,
          sql = select * from a,
          enable = true
      )
*/
public class CreateSqlBlockRuleStmt extends DdlStmt {

    public static final String NAME_PROPERTY = "name";

    public static final String USER_PROPERTY = "user";

    public static final String SQL_PROPERTY = "sql";

    public static final String ENABLE_PROPERTY = "enable";

    private final String ruleName;

    // default stands for all users
    private String user;

    private String sql;

    // whether to use the rule
    private boolean enable;

    private final Map<String, String> properties;

    private static final String NAME_TYPE = "SQL BLOCK RULE NAME";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(NAME_PROPERTY)
            .add(USER_PROPERTY)
            .add(SQL_PROPERTY)
            .add(ENABLE_PROPERTY)
            .build();

    public CreateSqlBlockRuleStmt(String ruleName, Map<String, String> properties) {
        this.ruleName = ruleName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // check name
        FeNameFormat.checkCommonName(NAME_TYPE, ruleName);
        // check properties
        checkProperties(properties);
    }

    private void checkProperties(Map<String, String> properties) throws UserException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }
        this.user = properties.get(USER_PROPERTY);
        // if not default, need check whether user exist
        if (!SqlBlockRule.DEFAULT_USER.equals(user)) {
            List<String> allUserIdents = Catalog.getCurrentCatalog().getAuth().getAllUserIdents(false).stream().map(UserIdentity::getUser).collect(Collectors.toList());
            if (!allUserIdents.contains(user)) {
                throw new AnalysisException(user + " is not exist");
            }
        }
        this.sql = properties.get(SQL_PROPERTY);
        this.enable = Util.getBooleanPropertyOrDefault(properties.get(ENABLE_PROPERTY), true, ENABLE_PROPERTY + " should be a boolean");
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

    public boolean isEnable() {
        return enable;
    }
}
