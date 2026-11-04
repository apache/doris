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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies that CREATE / ALTER RESOURCE statements have their credential properties masked
 * before they reach the audit log, via {@link NeedAuditEncryption#geneEncryptionSQL(String)}.
 */
public class RepositoryAuditEncryptionTest {

    private String encrypt(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof NeedAuditEncryption,
                "command should be NeedAuditEncryption: " + plan.getClass().getName());
        NeedAuditEncryption cmd = (NeedAuditEncryption) plan;
        Assertions.assertTrue(cmd.needAuditEncryption());
        return cmd.geneEncryptionSQL(sql);
    }

    @Test
    public void testCreateResourceMasksAiApiKey() {
        String sql = "CREATE EXTERNAL RESOURCE \"ai_resource\" PROPERTIES ("
                + "\"type\" = \"ai\", "
                + "\"ai.api_key\" = \"sk-test\", "
                + "\"ai.endpoint\" = \"https://api.test\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("sk-test"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
        Assertions.assertTrue(masked.contains("https://api.test"), masked);
    }

    @Test
    public void testAlterResourceMasksAiApiKey() {
        String sql = "ALTER RESOURCE \"ai_resource\" PROPERTIES ("
                + "\"ai.api_key\" = \"sk-test\", "
                + "\"ai.endpoint\" = \"https://api.test\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("sk-test"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
        Assertions.assertTrue(masked.contains("https://api.test"), masked);
    }

    @Test
    public void testCreateJobWithUpdateDoesNotThrowClassCastException() {
        // CREATE JOB ... DO <update> parses the DML as UpdateContext, not InsertTableContext.
        // geneEncryptionSQL must not fail with ClassCastException on such statements.
        String sql = "CREATE JOB job1 ON SCHEDULE AT CURRENT_TIMESTAMP DO UPDATE t SET type = 2 WHERE type = 1";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof NeedAuditEncryption,
                "command should be NeedAuditEncryption: " + plan.getClass().getName());
        NeedAuditEncryption cmd = (NeedAuditEncryption) plan;
        Assertions.assertDoesNotThrow(() -> cmd.geneEncryptionSQL(sql));
    }
}
