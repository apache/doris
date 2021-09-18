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
import org.apache.doris.common.DdlException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class AdminSetConfigStmtTest {
    private static String runningDir = "fe/mocked/AdminSetConfigStmtTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        String stmt = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) UtFrameUtils.parseAndAnalyzeStmt(stmt, connectContext);
        Catalog.getCurrentCatalog().setConfig(adminSetConfigStmt);
    }

    @Test
    public void testUnknownConfig() throws Exception {
        String stmt = "admin set frontend config(\"unknown_config\" = \"unknown\");";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) UtFrameUtils.parseAndAnalyzeStmt(stmt, connectContext);
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Config 'unknown_config' does not exist or is not mutable");
        Catalog.getCurrentCatalog().setConfig(adminSetConfigStmt);
    }

    @Test
    public void testEmptyConfig() throws Exception {
        String stmt = "admin set frontend config;";
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = config parameter size is not equal to 1");
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) UtFrameUtils.parseAndAnalyzeStmt(stmt, connectContext);
    }
}

