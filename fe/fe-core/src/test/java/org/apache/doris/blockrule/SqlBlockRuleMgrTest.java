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

package org.apache.doris.blockrule;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import mockit.Mocked;

public class SqlBlockRuleMgrTest {

    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MetricRepo.init();
    }


    @Test(expected = AnalysisException.class)
    public void testRegexMatchSql() throws AnalysisException {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", "default", ".* join .*", null, true);
        Pattern sqlPattern = Pattern.compile(sqlRule.getSql());
        SqlBlockRuleMgr.matchSql(sqlRule, sql, sqlPattern);
    }

    @Test(expected = AnalysisException.class)
    public void testHashMatchSql() throws AnalysisException {
        String sql = "select * from test_table1 tt1 join test_table2 tt2 on tt1.testId=tt2.testId limit 5";
        String hashSql = DigestUtils.md5Hex(sql);
        System.out.println(hashSql);
        SqlBlockRule sqlRule = new SqlBlockRule("test_rule1", "default", null, hashSql, true);
        SqlBlockRuleMgr.matchSql(sqlRule, sql, null);
    }

    @Test
    public void testNormalCreate(@Mocked ConnectContext connectContext, @Mocked Catalog catalog) throws UserException {
        SqlBlockRuleMgr sqlBlockRuleMgr = new SqlBlockRuleMgr();
        Map<String, String> properties = new HashMap<>();
        properties.put(CreateSqlBlockRuleStmt.SQL_PROPERTY, "select \\* from test_table");
        properties.put(CreateSqlBlockRuleStmt.USER_PROPERTY, SqlBlockRule.DEFAULT_USER);
        properties.put(CreateSqlBlockRuleStmt.ENABLE_PROPERTY, "true");
        CreateSqlBlockRuleStmt stmt = new CreateSqlBlockRuleStmt("test_rule", properties);
        stmt.analyze(analyzer);
        sqlBlockRuleMgr.createSqlBlockRule(stmt);
    }
}
