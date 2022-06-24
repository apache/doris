// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

import java.util.List;

public class SsbTest extends TestWithFeService {
    private static final NereidsParser PARSER = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("ssb");
        connectContext.setDatabase("default_cluster:ssb");
        List<String> tableList = SsbText.getAllCreateTableSql();
        for (String sql : tableList) {
            createTable(sql);
        }
    }

    @Test
    public void testMultiJoinSqlPlan() throws Exception {
        List<String> sqlList = SsbText.getAllMultiJoinSql();
        for (String sql : sqlList) {
            plan(sql);
        }
    }

    @Test
    // TODO: Subsequent implementation of weekofyear parser
    public void testSingleSqlPlan() throws Exception {
        List<String> sqlList = SsbText.getSelectFlatSql();
        for (String sql : sqlList) {
            plan(sql);
        }
    }

    //TODO: Complete phase of supplemental plan
    private void plan(String sql) throws Exception {
        LogicalPlan parsed = PARSER.parseSingle(sql);
        System.out.println(parsed.toString());
        //LogicalPlan analyze = AnalyzeTest.TestAnalyzer.analyze(parsed, connectContext);
    }
}
