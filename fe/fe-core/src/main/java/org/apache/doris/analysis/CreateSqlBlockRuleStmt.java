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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

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
@Getter
public class CreateSqlBlockRuleStmt extends DdlStmt {

    public static final String SQL_PROPERTY = "sql";

    public static final String SQL_HASH_PROPERTY = "sqlHash";

    public static final String SCANNED_PARTITION_NUM = "partition_num";

    public static final String SCANNED_TABLET_NUM = "tablet_num";

    public static final String SCANNED_CARDINALITY = "cardinality";

    public static final String GLOBAL_PROPERTY = "global";

    public static final String ENABLE_PROPERTY = "enable";

    public static final String STRING_NOT_SET = SqlBlockUtil.STRING_DEFAULT;

    private final String ruleName;

    private String sql;

    private String sqlHash;

    private Long partitionNum;

    private Long tabletNum;

    private Long cardinality;

    // whether effective global, default is false
    private boolean global;

    // whether to use the rule, default is true
    private boolean enable;

    private boolean ifNotExists;

    private final Map<String, String> properties;

    private static final String NAME_TYPE = "SQL BLOCK RULE NAME";

    public static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>().add(SQL_PROPERTY)
            .add(SQL_HASH_PROPERTY).add(GLOBAL_PROPERTY).add(ENABLE_PROPERTY).add(SCANNED_PARTITION_NUM)
            .add(SCANNED_TABLET_NUM).add(SCANNED_CARDINALITY).build();

    public CreateSqlBlockRuleStmt(String ruleName, Map<String, String> properties) {
        this.ifNotExists = false;
        this.ruleName = ruleName;
        this.properties = properties;
    }

    public CreateSqlBlockRuleStmt(boolean ifNotExists, String ruleName, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.ruleName = ruleName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // check name
        FeNameFormat.checkCommonName(NAME_TYPE, ruleName);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check properties
        CreateSqlBlockRuleStmt.checkCommonProperties(properties);
        setProperties(properties);

        // avoid a rule block any ddl for itself
        if (StringUtils.isNotEmpty(sql) && Pattern.compile(sql).matcher(this.ruleName).find()) {
            throw new AnalysisException("sql of SQL_BLOCK_RULE should not match its name");
        }
    }

    private void setProperties(Map<String, String> properties) throws UserException {
        this.sql = properties.getOrDefault(SQL_PROPERTY, STRING_NOT_SET);
        this.sqlHash = properties.getOrDefault(SQL_HASH_PROPERTY, STRING_NOT_SET);
        String partitionNumString = properties.get(SCANNED_PARTITION_NUM);
        String tabletNumString = properties.get(SCANNED_TABLET_NUM);
        String cardinalityString = properties.get(SCANNED_CARDINALITY);

        SqlBlockUtil.checkSqlAndSqlHashSetBoth(sql, sqlHash);
        SqlBlockUtil.checkPropertiesValidate(sql, sqlHash, partitionNumString, tabletNumString, cardinalityString);

        this.partitionNum = Util.getLongPropertyOrDefault(partitionNumString, 0L, null,
                SCANNED_PARTITION_NUM + " should be a long");
        this.tabletNum = Util.getLongPropertyOrDefault(tabletNumString, 0L, null,
                SCANNED_TABLET_NUM + " should be a long");
        this.cardinality = Util.getLongPropertyOrDefault(cardinalityString, 0L, null,
                SCANNED_CARDINALITY + " should be a long");

        this.global = Util.getBooleanPropertyOrDefault(properties.get(GLOBAL_PROPERTY), false,
                GLOBAL_PROPERTY + " should be a boolean");
        this.enable = Util.getBooleanPropertyOrDefault(properties.get(ENABLE_PROPERTY), true,
                ENABLE_PROPERTY + " should be a boolean");
    }

    public static void checkCommonProperties(Map<String, String> properties) throws UserException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Not set properties");
        }
        Optional<String> optional = properties.keySet().stream().filter(entity -> !PROPERTIES_SET.contains(entity))
                .findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SQL_BLOCK_RULE ").append(ruleName).append(" \nPROPERTIES(\n")
                .append(new PrintableMap<>(properties, " = ", true, true, true)).append(")");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
