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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Set;

public class RangerHiveAccessControllerTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("user1", "%");

    /**
     * A Ranger policy may be written against a role, and which roles a Doris account holds is the engine's to
     * know: the source has no user directory of its own to look them up in.
     */
    @Test
    public void testRequestCarriesTheRolesTheEngineKnows() {
        Set<String> roles = ImmutableSet.of("analyst");
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Mockito.when(context.rolesOf(SUBJECT)).thenReturn(roles);

        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                RangerAccessRequestImpl request = controller.createRequest(SUBJECT);

                Assert.assertEquals("user1", request.getUser());
                Assert.assertEquals(roles, request.getUserRoles());
                Assert.assertEquals("%", request.getClientIPAddress());
            } finally {
                controller.close();
            }
        }
    }

    /**
     * Only the checks the engine asks by name map onto a Hive access type. Anything else - a requirement put
     * together for one statement, for instance - maps to one no Hive policy grants, rather than to a
     * neighbouring access type that some policy might.
     */
    @Test
    public void testAccessTypeIsRecognisedByWhatIsAsked() {
        Assert.assertEquals(HiveAccessType.USE,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.VISIBILITY));
        Assert.assertEquals(HiveAccessType.SELECT,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.SELECT));
        Assert.assertEquals(HiveAccessType.UPDATE,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.LOAD));
        Assert.assertEquals(HiveAccessType.ALTER,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.ALTER));
        Assert.assertEquals(HiveAccessType.CREATE,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.CREATE));
        Assert.assertEquals(HiveAccessType.DROP,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.DROP));
        Assert.assertEquals(HiveAccessType.ALL,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.ADMINISTRATION));
        Assert.assertEquals(HiveAccessType.ALL,
                RangerHiveAccessController.accessTypeOf(AccessRequirements.ANY_PRIVILEGE));

        Assert.assertEquals(HiveAccessType.NONE, RangerHiveAccessController.accessTypeOf(
                AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT)));
        Assert.assertEquals(HiveAccessType.NONE,
                RangerHiveAccessController.accessTypeOf(AccessRequirement.of(AccessAction.USAGE)));
    }

    /**
     * Which row-filter and masking policies a Hive service returns depends on the access type the lookup
     * asks with, and that string reaches a Ranger server nobody rebuilds when Doris changes.
     *
     * <p>Pinned lower case, because that is the only spelling a Hive policy can carry: Ranger's stock Hive
     * service definition declares its access types as {@code select}, {@code update}, ... and its
     * {@code rowFilterDef} declares {@code select}, so the Ranger UI cannot write anything else. Doris used
     * to ask with a single hard-coded {@code SELECT} shared with the Doris service type - right there, where
     * the definition really is upper case, and matching nothing here. A row filter or column mask written
     * against a Hive service therefore never reached the query, silently. The privilege checks above already
     * lower case the access type they ask with; this makes the data-policy lookup agree with them and with
     * the deployed service definition.
     */
    @Test
    public void testRowFilterLookupAsksWithTheReadAccessType() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class,
                        RangerHiveAccessControllerTest::answerRowFilterLookups);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                controller.getRowFilters(SUBJECT, AuthorizedResource.table("ctl", "db", "tbl"),
                        AccessContext.NONE);

                ArgumentCaptor<RangerAccessRequest> asked = ArgumentCaptor.forClass(RangerAccessRequest.class);
                Mockito.verify(plugin.constructed().get(0))
                        .evalRowFilterPolicies(asked.capture(), Mockito.any());
                Assert.assertEquals("select", asked.getValue().getAccessType());
            } finally {
                controller.close();
            }
        }
    }

    /**
     * A Ranger plugin with a policy engine that cannot answer - not yet initialized, or being cleaned up -
     * hands back null, and that is not "this table has no row filter".
     *
     * <p>Reading it as an empty policy set is a silent unfiltered, unmasked read of a table Ranger governs: the
     * planner would go on to plan the query with no filter and no mask, the SQL cache would record that, and no
     * line would be logged anywhere. The privilege paths of this same class already read the same null as "no
     * answer" and refuse, which is what these two now agree with.
     */
    @Test
    public void testAPolicyEngineWithNoAnswerRefusesInsteadOfSayingNoPolicy() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                AuthorizedResource.Table table = AuthorizedResource.table("ctl", "db", "tbl");

                Assert.assertThrows(IllegalStateException.class,
                        () -> controller.getRowFilters(SUBJECT, table, AccessContext.NONE));
                Assert.assertThrows(IllegalStateException.class,
                        () -> controller.getDataMasks(SUBJECT, table, ImmutableSet.of("col1"),
                                AccessContext.NONE));
            } finally {
                controller.close();
            }
        }
    }

    /**
     * A closed controller refuses everything, data policies included.
     *
     * <p>The manager hands a controller out without holding a lock and closes it outside every lock, so an
     * {@code ALTER CATALOG ... SET PROPERTIES} can close this one while a query that is still planning holds
     * it. Answering "no row filter, no column mask" there is what made that query read the table whole and in
     * the clear, and the refusal it should have got is fail-closed the same way the privilege check is.
     */
    @Test
    public void testAClosedControllerRefusesDataPoliciesToo() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class,
                        RangerHiveAccessControllerTest::answerRowFilterLookups);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            AuthorizedResource.Table table = AuthorizedResource.table("ctl", "db", "tbl");
            Assert.assertTrue("a plugin answering normally must return no filter, not refuse",
                    controller.getRowFilters(SUBJECT, table, AccessContext.NONE).isEmpty());

            controller.close();

            IllegalStateException refused = Assert.assertThrows(IllegalStateException.class,
                    () -> controller.getRowFilters(SUBJECT, table, AccessContext.NONE));
            Assert.assertTrue(refused.getMessage(),
                    refused.getMessage().contains(RangerHiveAccessController.NAME));
            Assert.assertThrows(IllegalStateException.class,
                    () -> controller.getDataMasks(SUBJECT, table, ImmutableSet.of("col1"), AccessContext.NONE));
            AccessDeniedException denied = Assert.assertThrows(AccessDeniedException.class,
                    () -> controller.checkPrivilege(SUBJECT, table, AccessRequirements.SELECT,
                            AccessContext.NONE));
            Assert.assertTrue(denied.getMessage(), denied.getMessage().contains("has been closed"));
        }
    }

    /** A policy engine that answers, with no row filter to report - the ordinary case for most tables. */
    private static void answerRowFilterLookups(RangerHivePlugin plugin,
            MockedConstruction.Context ignored) {
        Mockito.when(plugin.evalRowFilterPolicies(Mockito.any(), Mockito.any())).thenAnswer(
                invocation -> new RangerAccessResult(RangerPolicy.POLICY_TYPE_ROWFILTER, "hive", null,
                        invocation.getArgument(0)));
    }
}
