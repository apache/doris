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

import org.apache.doris.authentication.BasicPrincipal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

class UnifiedRoleMappingCelEngineTest {

    @Test
    void shouldUnionRolesAcrossMatchedRules() {
        UnifiedRoleMappingCelEngine engine = new UnifiedRoleMappingCelEngine(List.of(
                UnifiedRoleMappingCelEngine.Rule.of(
                        "has_group(\"team-a\")",
                        Set.of("team_a_reader")),
                UnifiedRoleMappingCelEngine.Rule.of(
                        "has_group(\"oncall\") && has_scope(\"logs:write\")",
                        Set.of("oncall_logs_writer")),
                UnifiedRoleMappingCelEngine.Rule.of(
                        "has_scope(\"session:role:analyst\")",
                        Set.of("analyst", "dashboard_readonly")),
                UnifiedRoleMappingCelEngine.Rule.of(
                        "attr(\"environment\") == \"prod\"",
                        Set.of("prod_observer"))));

        UnifiedRoleMappingCelEngine.EvaluationContext context = UnifiedRoleMappingCelEngine.EvaluationContext.builder()
                .name("alice")
                .externalPrincipal("oidc:alice")
                .externalGroups(Set.of("team-a", "oncall"))
                .attributes(Map.of("environment", "prod"))
                .multiValueAttributes(Map.of(
                        "scope", Set.of("logs:write"),
                        "scp", Set.of("session:role:analyst")))
                .build();

        Assertions.assertEquals(
                Set.of("team_a_reader", "oncall_logs_writer", "analyst", "dashboard_readonly", "prod_observer"),
                engine.evaluate(context));
    }

    @Test
    void shouldSupportRoleScopeAndAttributeHelpers() {
        UnifiedRoleMappingCelEngine engine = new UnifiedRoleMappingCelEngine(List.of(
                UnifiedRoleMappingCelEngine.Rule.of(
                        "has_role(\"incident-admin\") && has_any_scope(\"logs:write\", \"metrics:write\") "
                                + "&& has_attr_value(\"permissions\", \"ticket:approve\")",
                        Set.of("incident_admin"))));

        UnifiedRoleMappingCelEngine.EvaluationContext context = UnifiedRoleMappingCelEngine.EvaluationContext.builder()
                .name("svc-alerting")
                .externalPrincipal("client:alerting")
                .servicePrincipal(true)
                .multiValueAttributes(Map.of(
                        "roles", Set.of("incident-admin"),
                        "scp", Set.of("logs:write"),
                        "permissions", Set.of("ticket:approve")))
                .build();

        Assertions.assertEquals(Set.of("incident_admin"), engine.evaluate(context));
    }

    @Test
    void shouldSupportNamePrincipalAndAnyHelpers() {
        UnifiedRoleMappingCelEngine engine = new UnifiedRoleMappingCelEngine(List.of(
                UnifiedRoleMappingCelEngine.Rule.of(
                        "name() == \"dave\" && external_principal() == \"dave@EXAMPLE.COM\" "
                                + "&& has_any_group(\"ops\", \"security\") "
                                + "&& has_any_role(\"viewer\", \"operator\") "
                                + "&& has_any_attr_value(\"permissions\", \"job:run\", \"job:stop\")",
                        Set.of("ops_operator"))));

        UnifiedRoleMappingCelEngine.EvaluationContext context = UnifiedRoleMappingCelEngine.EvaluationContext.builder()
                .name("dave")
                .externalPrincipal("dave@EXAMPLE.COM")
                .externalGroups(Set.of("ops"))
                .multiValueAttributes(Map.of(
                        "roles", Set.of("operator"),
                        "permissions", Set.of("job:stop")))
                .build();

        Assertions.assertEquals(Set.of("ops_operator"), engine.evaluate(context));
    }

    @Test
    void shouldBranchOnServicePrincipal() {
        UnifiedRoleMappingCelEngine engine = new UnifiedRoleMappingCelEngine(List.of(
                UnifiedRoleMappingCelEngine.Rule.of(
                        "is_service_principal() && has_scope(\"ingest:write\")",
                        Set.of("ingest_writer")),
                UnifiedRoleMappingCelEngine.Rule.of(
                        "!is_service_principal() && has_scope(\"session:role:reader\")",
                        Set.of("dashboard_readonly"))));

        UnifiedRoleMappingCelEngine.EvaluationContext machineContext = UnifiedRoleMappingCelEngine.EvaluationContext
                .builder()
                .name("svc-ingest")
                .servicePrincipal(true)
                .multiValueAttributes(Map.of("scope", Set.of("ingest:write")))
                .build();

        UnifiedRoleMappingCelEngine.EvaluationContext userContext = UnifiedRoleMappingCelEngine.EvaluationContext
                .builder()
                .name("bob")
                .servicePrincipal(false)
                .multiValueAttributes(Map.of("scope", Set.of("session:role:reader")))
                .build();

        Assertions.assertEquals(Set.of("ingest_writer"), engine.evaluate(machineContext));
        Assertions.assertEquals(Set.of("dashboard_readonly"), engine.evaluate(userContext));
    }

    @Test
    void shouldReturnEmptyStringForMissingAttributes() {
        UnifiedRoleMappingCelEngine engine = new UnifiedRoleMappingCelEngine(List.of(
                UnifiedRoleMappingCelEngine.Rule.of(
                        "attr(\"department\") == \"\" && !has_group(\"finance\")",
                        Set.of("fallback_role"))));

        UnifiedRoleMappingCelEngine.EvaluationContext context = UnifiedRoleMappingCelEngine.EvaluationContext.builder()
                .name("carol")
                .build();

        Assertions.assertEquals(Set.of("fallback_role"), engine.evaluate(context));
    }

    @Test
    void shouldRejectInvalidExpressionsAtConstructionTime() {
        IllegalArgumentException error = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new UnifiedRoleMappingCelEngine(List.of(
                        UnifiedRoleMappingCelEngine.Rule.of(
                                "unknown_helper(\"x\")",
                                Set.of("broken")))));

        Assertions.assertTrue(error.getMessage().contains("ERROR"));
    }

    @Test
    void shouldBuildEvaluationContextFromPrincipal() {
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("oidc_demo")
                .externalPrincipal("oidc:alice")
                .externalGroups(Set.of("team-a", "oncall"))
                .attributes(Map.of("environment", "prod"))
                .build();

        UnifiedRoleMappingCelEngine.EvaluationContext context = UnifiedRoleMappingCelEngine.fromPrincipal(
                principal,
                Map.of("scope", Set.of("logs:write")));

        Assertions.assertEquals("alice", context.getName());
        Assertions.assertEquals("oidc:alice", context.getExternalPrincipal());
        Assertions.assertEquals(Set.of("team-a", "oncall"), context.getExternalGroups());
        Assertions.assertEquals("prod", context.getAttributes().get("environment"));
        Assertions.assertEquals(Set.of("logs:write"), context.getMultiValueAttributes().get("scope"));
        Assertions.assertFalse(context.isServicePrincipal());
    }
}
