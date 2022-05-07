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
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AdminSetConfigStmtTest extends TestWithFeService {
    @Test
    public void testNormal() throws Exception {
        String stmt = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) parseAndAnalyzeStmt(stmt);
        Catalog.getCurrentCatalog().setConfig(adminSetConfigStmt);
    }

    @Test
    public void testUnknownConfig() throws Exception {
        String stmt = "admin set frontend config(\"unknown_config\" = \"unknown\");";
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) parseAndAnalyzeStmt(stmt);
        DdlException exception = assertThrows(DdlException.class,
            () -> Catalog.getCurrentCatalog().setConfig(adminSetConfigStmt));
        assertEquals("errCode = 2, detailMessage = Config 'unknown_config' does not exist",
            exception.getMessage());
    }

    @Test
    public void testEmptyConfig() {
        AnalysisException exception =
            assertThrows(AnalysisException.class,
                () -> parseAndAnalyzeStmt("admin set frontend config;"));
        assertEquals("errCode = 2, detailMessage = config parameter size is not equal to 1",
            exception.getMessage());
    }
}
