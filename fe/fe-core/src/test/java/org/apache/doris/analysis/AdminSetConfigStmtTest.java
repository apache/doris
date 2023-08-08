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
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.VariableAnnotation;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AdminSetConfigStmtTest extends TestWithFeService {
    @Test
    public void testNormal() throws Exception {
        String stmt = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) parseAndAnalyzeStmt(stmt);
        Env.getCurrentEnv().setConfig(adminSetConfigStmt);
    }

    @Test
    public void testUnknownConfig() throws Exception {
        String stmt = "admin set frontend config(\"unknown_config\" = \"unknown\");";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) parseAndAnalyzeStmt(stmt);
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> Env.getCurrentEnv().setConfig(adminSetConfigStmt));
        Assertions.assertEquals("errCode = 2, detailMessage = Config 'unknown_config' does not exist",
                exception.getMessage());
    }

    @Test
    public void testEmptyConfig() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> parseAndAnalyzeStmt("admin set frontend config;"));
        Assertions.assertEquals("errCode = 2, detailMessage = config parameter size is not equal to 1",
                exception.getMessage());
    }

    @Test
    public void testExperimentalConfig() throws Exception {
        // 1. set without experimental
        boolean enableMtmv = Config.enable_mtmv;
        String stmt = "admin set frontend config('enable_mtmv' = '" + String.valueOf(!enableMtmv) + "');";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) parseAndAnalyzeStmt(stmt);
        Env.getCurrentEnv().setConfig(adminSetConfigStmt);
        Assert.assertNotEquals(enableMtmv, Config.enable_mtmv);

        // 2. set with experimental
        enableMtmv = Config.enable_mtmv;
        stmt = "admin set frontend config('experimental_enable_mtmv' = '" + String.valueOf(!enableMtmv) + "');";
        adminSetConfigStmt = (AdminSetConfigStmt) parseAndAnalyzeStmt(stmt);
        Env.getCurrentEnv().setConfig(adminSetConfigStmt);
        Assert.assertNotEquals(enableMtmv, Config.enable_mtmv);

        // 3. show config
        int num = ConfigBase.getConfigNumByVariableAnnotation(VariableAnnotation.EXPERIMENTAL);
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern("%experimental%",
                CaseSensibility.CONFIG.getCaseSensibility());
        List<List<String>> results = ConfigBase.getConfigInfo(matcher);
        Assert.assertEquals(num, results.size());

        num = ConfigBase.getConfigNumByVariableAnnotation(VariableAnnotation.DEPRECATED);
        matcher = PatternMatcherWrapper.createMysqlPattern("%deprecated%",
                CaseSensibility.CONFIG.getCaseSensibility());
        results = ConfigBase.getConfigInfo(matcher);
        Assert.assertEquals(num, results.size());
    }
}

