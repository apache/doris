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

package org.apache.doris.authentication.rolemapping;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.Principal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class IntegrationPropertyRoleMappingEvaluatorTest {

    @Test
    @DisplayName("UT-RME-001: Inline rules grant union of matched roles")
    void testEvaluate_UnionOfMatchedInlineRules() throws AuthenticationException {
        RoleMappingEvaluator evaluator = new IntegrationPropertyRoleMappingEvaluator();
        AuthenticationIntegration integration = integration(Map.of(
                "role_mapping.rule.groups.condition", "has_group(\"oncall\")",
                "role_mapping.rule.groups.roles", "logs_reader,logs_writer",
                "role_mapping.rule.scope.condition", "has_scope(\"session:role:reader\")",
                "role_mapping.rule.scope.roles", "session_reader",
                "role_mapping.rule.entitlements.condition", "has_attr_value(\"entitlements\", \"reports:view\")",
                "role_mapping.rule.entitlements.roles", "reports_reader",
                "role_mapping.rule.default.condition", "true",
                "role_mapping.rule.default.roles", "default_readonly"));
        Principal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("mapped_oidc")
                .externalGroups(Set.of("oncall"))
                .multiValueAttributes(Map.of(
                        "scope", Set.of("session:role:reader"),
                        "entitlements", Set.of("reports:view")))
                .build();

        Assertions.assertEquals(
                Set.of("logs_reader", "logs_writer", "session_reader", "reports_reader", "default_readonly"),
                evaluator.evaluate(integration, principal));
    }

    @Test
    @DisplayName("UT-RME-002: Zero matching rules return empty roles")
    void testEvaluate_ZeroMatchReturnsEmptySet() throws AuthenticationException {
        RoleMappingEvaluator evaluator = new IntegrationPropertyRoleMappingEvaluator();
        AuthenticationIntegration integration = integration(Map.of(
                "role_mapping.rule.finance.condition", "has_group(\"finance\")",
                "role_mapping.rule.finance.roles", "finance_reader"));
        Principal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("mapped_oidc")
                .externalGroups(Set.of("engineering"))
                .build();

        Assertions.assertTrue(evaluator.evaluate(integration, principal).isEmpty());
    }

    private static AuthenticationIntegration integration(Map<String, String> properties) {
        return AuthenticationIntegration.builder()
                .name("mapped_oidc")
                .type("oidc")
                .properties(new HashMap<>(properties))
                .build();
    }
}
