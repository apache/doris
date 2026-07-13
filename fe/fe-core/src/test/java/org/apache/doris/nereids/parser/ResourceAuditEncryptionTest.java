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

public class ResourceAuditEncryptionTest {

    private String encrypt(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof NeedAuditEncryption,
                "command should be NeedAuditEncryption: " + plan.getClass().getName());
        NeedAuditEncryption cmd = (NeedAuditEncryption) plan;
        Assertions.assertTrue(cmd.needAuditEncryption());
        return cmd.geneEncryptionSQL(sql);
    }

    @Test
    public void testCreateAiResourceMasksApiKey() {
        String sql = "CREATE RESOURCE IF NOT EXISTS `ai_resource` PROPERTIES ("
                + "'type' = 'ai', "
                + "'ai.provider_type' = 'deepseek', "
                + "'ai.endpoint' = 'https://api.deepseek.com/chat/completions', "
                + "'ai.model_name' = 'deepseek-chat', "
                + "'ai.api_key' = 'sk-secret')";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("sk-secret"), "api key must be masked: " + masked);
        Assertions.assertTrue(masked.contains("*XXX"), "expected mask token: " + masked);
        Assertions.assertTrue(masked.contains("https://api.deepseek.com/chat/completions"), masked);
        Assertions.assertTrue(masked.contains("deepseek-chat"), masked);
    }

    @Test
    public void testAlterAiResourceMasksApiKey() {
        String sql = "ALTER RESOURCE `ai_resource` PROPERTIES ("
                + "'ai.api_key' = 'new-secret', "
                + "'ai.endpoint' = 'https://api.deepseek.com/chat/completions')";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("new-secret"), "api key must be masked: " + masked);
        Assertions.assertTrue(masked.contains("*XXX"), "expected mask token: " + masked);
        Assertions.assertTrue(masked.contains("https://api.deepseek.com/chat/completions"), masked);
    }
}
