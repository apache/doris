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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MTMVPrivilegeValidationTest extends TestWithFeService {

    private static final String DB_NAME = "mtmv_priv_db";
    private static final String TBL_NAME = "mtmv_priv_tbl";
    private static final String USER_NO_PRIV = "mtmv_priv_user_none";
    private static final String USER_ALTER_PRIV = "mtmv_priv_user_alter";
    private static final String USER_CREATE_PRIV = "mtmv_priv_user_create";
    private static final String QUALIFIED_TBL = "internal." + DB_NAME + "." + TBL_NAME;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse(DB_NAME);
        createTable("CREATE TABLE IF NOT EXISTS " + DB_NAME + "." + TBL_NAME + " ("
                + "    id INT,"
                + "    val INT"
                + ") DUPLICATE KEY(id)"
                + " DISTRIBUTED BY HASH(id) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '1')");

        addUser(USER_NO_PRIV, true);
        addUser(USER_ALTER_PRIV, true);
        addUser(USER_CREATE_PRIV, true);
        grantPriv("GRANT ALTER_PRIV ON " + QUALIFIED_TBL + " TO '" + USER_ALTER_PRIV + "'@'%'");
        grantPriv("GRANT CREATE_PRIV ON " + QUALIFIED_TBL + " TO '" + USER_CREATE_PRIV + "'@'%'");
    }

    private TableNameInfo targetTable() {
        return new TableNameInfo("internal", DB_NAME, TBL_NAME);
    }

    @Test
    public void testPauseMTMVDeniedWhenNoPriv() throws Exception {
        useUser(USER_NO_PRIV);
        PauseMTMVInfo info = new PauseMTMVInfo(targetTable());
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertTrue(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testResumeMTMVDeniedWhenNoPriv() throws Exception {
        useUser(USER_NO_PRIV);
        ResumeMTMVInfo info = new ResumeMTMVInfo(targetTable());
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertTrue(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testCancelMTMVTaskDeniedWhenNoPriv() throws Exception {
        useUser(USER_NO_PRIV);
        CancelMTMVTaskInfo info = new CancelMTMVTaskInfo(targetTable(), 1L);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertTrue(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testRefreshMTMVDeniedWhenNoPriv() throws Exception {
        useUser(USER_NO_PRIV);
        RefreshMTMVInfo info = new RefreshMTMVInfo(targetTable(), ImmutableList.of(),
                RefreshMTMVInfo.RefreshMode.AUTO);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertTrue(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testPauseMTMVPassesWithAlterPriv() throws Exception {
        useUser(USER_ALTER_PRIV);
        PauseMTMVInfo info = new PauseMTMVInfo(targetTable());
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertFalse(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testResumeMTMVPassesWithAlterPriv() throws Exception {
        useUser(USER_ALTER_PRIV);
        ResumeMTMVInfo info = new ResumeMTMVInfo(targetTable());
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertFalse(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testCancelMTMVTaskPassesWithAlterPriv() throws Exception {
        useUser(USER_ALTER_PRIV);
        CancelMTMVTaskInfo info = new CancelMTMVTaskInfo(targetTable(), 1L);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertFalse(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testRefreshMTMVPassesWithAlterPriv() throws Exception {
        useUser(USER_ALTER_PRIV);
        RefreshMTMVInfo info = new RefreshMTMVInfo(targetTable(), ImmutableList.of(),
                RefreshMTMVInfo.RefreshMode.AUTO);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertFalse(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }

    @Test
    public void testPauseMTMVPassesWithCreatePriv() throws Exception {
        useUser(USER_CREATE_PRIV);
        PauseMTMVInfo info = new PauseMTMVInfo(targetTable());
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> info.analyze(connectContext));
        Assertions.assertFalse(e.getMessage().contains("ALTER command denied"), e.getMessage());
    }
}
