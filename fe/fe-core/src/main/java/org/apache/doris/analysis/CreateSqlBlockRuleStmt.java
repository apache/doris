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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;

/*
 Create sqlBlockRule statement

 syntax:
      CREATE SQL_BLOCK_RULE `rule_name` PROPERTIES
      (
          sql = select * from a,
          global = false
          enable = true
      )
*/
public class CreateSqlBlockRuleStmt extends DdlStmt {

    public static final String SQL_PROPERTY = "sql";

    public static final String SQL_HASH_PROPERTY = "sqlHash";

    public static final String GLOBAL_PROPERTY = "global";

    public static final String ENABLE_PROPERTY = "enable";

    private final String ruleName;

    private String sql;

    private String sqlHash;

    // whether effective global, default is false
    private boolean global;

    // whether to use the rule, default is true
    private boolean enable;

    private final Map<String, String> properties;

    private static final String NAME_TYPE = "SQL BLOCK RULE NAME";

    public static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(SQL_PROPERTY)
            .add(SQL_HASH_PROPERTY)
            .add(GLOBAL_PROPERTY)
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
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check properties
        CreateSqlBlockRuleStmt.checkCommonProperties(properties);
        setProperties(properties);
    }

    private void setProperties(Map<String, String> properties) throws UserException {
        this.sql = properties.get(SQL_PROPERTY);
        this.sqlHash = properties.get(SQL_HASH_PROPERTY);
        if ((StringUtils.isNotEmpty(sql) && StringUtils.isNotEmpty(sqlHash)) || (StringUtils.isEmpty(sql) && StringUtils.isEmpty(sqlHash))) {
            throw new AnalysisException("Only one sql or sqlHash can be configured");
        }

        this.global = Util.getBooleanPropertyOrDefault(properties.get(GLOBAL_PROPERTY), false, GLOBAL_PROPERTY + " should be a boolean");
        this.enable = Util.getBooleanPropertyOrDefault(properties.get(ENABLE_PROPERTY), true, ENABLE_PROPERTY + " should be a boolean");
    }

    public static void checkCommonProperties(Map<String, String> properties) throws UserException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Not set properties");
        }
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getSql() {
        return sql;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public boolean isGlobal() {
        return global;
    }

    public boolean isEnable() {
        return enable;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SQL_BLOCK_RULE ")
                .append(ruleName)
                .append(" \nPROPERTIES(\n")
                .append(new PrintableMap<>(properties, " = ", true, true, true))
                .append(")");
        return sb.toString();
    }
}
