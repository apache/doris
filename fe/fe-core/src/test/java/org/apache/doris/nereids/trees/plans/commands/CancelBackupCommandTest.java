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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Tests for CancelBackupCommand label matching functionality.
 */
public class CancelBackupCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessControllerManager;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    private final String dbName = "test_db";

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    public void testValidateNormal() {
        Mockito.when(accessControllerManager.checkDbPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(dbName), Mockito.eq(PrivPredicate.LOAD))).thenReturn(true);

        CancelBackupCommand command = new CancelBackupCommand(dbName, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testMatchesLabel_ExactMatch() {
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "backup1", false);

        Assertions.assertTrue(cmd.matchesLabel("backup1"));
        Assertions.assertFalse(cmd.matchesLabel("backup2"));
        Assertions.assertFalse(cmd.matchesLabel("backup11"));
        Assertions.assertFalse(cmd.matchesLabel("xbackup1"));
    }

    @Test
    public void testMatchesLabel_LikeWithPercent() {
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "test_%", true);

        Assertions.assertTrue(cmd.matchesLabel("test_backup1"));
        Assertions.assertTrue(cmd.matchesLabel("test_restore1"));
        Assertions.assertTrue(cmd.matchesLabel("test_"));
        Assertions.assertFalse(cmd.matchesLabel("backup1"));
        Assertions.assertFalse(cmd.matchesLabel("tes_backup"));
    }

    @Test
    public void testMatchesLabel_LikeWithUnderscore() {
        // LIKE with underscore (single char wildcard): backup_00_
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "backup_00_", true);

        Assertions.assertTrue(cmd.matchesLabel("backup_001"));
        Assertions.assertTrue(cmd.matchesLabel("backup_002"));
        Assertions.assertTrue(cmd.matchesLabel("backup_00x"));
        Assertions.assertFalse(cmd.matchesLabel("backup_0011"));  // too long
        Assertions.assertFalse(cmd.matchesLabel("backup_00"));    // too short
    }

    @Test
    public void testMatchesLabel_LikeComplexPattern() {
        // Pattern: %_2023%
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "%_2023%", true);

        Assertions.assertTrue(cmd.matchesLabel("backup_20230101"));
        Assertions.assertTrue(cmd.matchesLabel("test_2023_backup"));
        Assertions.assertTrue(cmd.matchesLabel("x_2023x"));
        Assertions.assertFalse(cmd.matchesLabel("backup_2024"));
        Assertions.assertFalse(cmd.matchesLabel("2023"));  // underscore before 2023 is required
    }

    @Test
    public void testMatchesLabel_Null() {
        // label = null means match all
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, null, false);

        Assertions.assertTrue(cmd.matchesLabel("backup1"));
        Assertions.assertTrue(cmd.matchesLabel("backup2"));
        Assertions.assertTrue(cmd.matchesLabel("any_label"));
        Assertions.assertTrue(cmd.matchesLabel(""));
    }

    @Test
    public void testMatchesLabel_EmptyString() {
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "", false);

        Assertions.assertTrue(cmd.matchesLabel(""));
        Assertions.assertFalse(cmd.matchesLabel("backup1"));
    }

    @Test
    public void testMatchesLabel_LikePercentOnly() {
        // LIKE % matches everything
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "%", true);

        Assertions.assertTrue(cmd.matchesLabel("backup1"));
        Assertions.assertTrue(cmd.matchesLabel("any_label"));
        Assertions.assertTrue(cmd.matchesLabel(""));
    }

    @Test
    public void testMatchesLabel_LikeStartsWith() {
        // LIKE test%
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "test%", true);

        Assertions.assertTrue(cmd.matchesLabel("test"));
        Assertions.assertTrue(cmd.matchesLabel("test_backup"));
        Assertions.assertTrue(cmd.matchesLabel("testing"));
        Assertions.assertFalse(cmd.matchesLabel("xtest"));
        Assertions.assertFalse(cmd.matchesLabel("backup_test"));
    }

    @Test
    public void testMatchesLabel_LikeEndsWith() {
        // LIKE %_backup
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "%_backup", true);

        Assertions.assertTrue(cmd.matchesLabel("test_backup"));
        Assertions.assertTrue(cmd.matchesLabel("prod_backup"));
        Assertions.assertTrue(cmd.matchesLabel("x_backup"));
        Assertions.assertFalse(cmd.matchesLabel("backup"));       // no underscore
        Assertions.assertFalse(cmd.matchesLabel("backup_test"));  // not ending with _backup
    }

    @Test
    public void testBackwardCompatibility() {
        // Old constructor should work
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false);

        Assertions.assertNull(cmd.getLabel());
        Assertions.assertFalse(cmd.isLike());
        Assertions.assertTrue(cmd.matchesLabel("any_label"));  // Should match all
    }

    @Test
    public void testRestore() {
        // Test restore command
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", true, "restore1", false);

        Assertions.assertTrue(cmd.isRestore());
        Assertions.assertTrue(cmd.matchesLabel("restore1"));
        Assertions.assertFalse(cmd.matchesLabel("restore2"));
    }

    @Test
    public void testMatchesLabel_RegexSpecialCharsInExact() {
        CancelBackupCommand cmd = new CancelBackupCommand("test_db", false, "test.backup", false);

        Assertions.assertTrue(cmd.matchesLabel("test.backup"));
        Assertions.assertFalse(cmd.matchesLabel("testabackup"));
    }

    @Test
    public void testMatchesLabel_RegexSpecialCharsInLike() {

        CancelBackupCommand dotCmd = new CancelBackupCommand("test_db", false, "test.backup%", true);
        Assertions.assertTrue(dotCmd.matchesLabel("test.backup_001"));
        Assertions.assertFalse(dotCmd.matchesLabel("testabackup_001"));

        CancelBackupCommand bracketCmd = new CancelBackupCommand("test_db", false, "test[1-9]%", true);
        Assertions.assertTrue(bracketCmd.matchesLabel("test[1-9]_backup"));
        Assertions.assertFalse(bracketCmd.matchesLabel("test1_backup"));

        CancelBackupCommand parenCmd = new CancelBackupCommand("test_db", false, "test(a)%", true);
        Assertions.assertTrue(parenCmd.matchesLabel("test(a)_backup"));
        Assertions.assertFalse(parenCmd.matchesLabel("testa_backup"));
    }
}
