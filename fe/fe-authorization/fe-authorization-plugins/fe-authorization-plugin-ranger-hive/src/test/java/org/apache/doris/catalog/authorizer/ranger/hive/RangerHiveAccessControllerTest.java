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
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

public class RangerHiveAccessControllerTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("user1", "%");

    /**
     * A Ranger policy may be written against a role, and which roles a Doris account holds is the engine's to
     * know: the source has no user directory of its own to look them up in.
     *
     * <p>The client address is here too, because it is the other half of what a Ranger policy can be
     * conditioned on and the two come from different places. The address belongs to the connection and
     * arrives in the {@link AccessContext}; {@code AuthorizedSubject.getHost()} is the host <em>pattern</em>
     * of the account - {@code '%'} for an ordinary {@code CREATE USER} - and matching an IP condition
     * against that matches nothing anybody wrote.
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
                RangerAccessRequestImpl request = controller.createRequest(SUBJECT, connectedFrom("10.0.0.7"));

                Assert.assertEquals("user1", request.getUser());
                Assert.assertEquals(roles, request.getUserRoles());
                Assert.assertEquals("the account's host pattern was sent to Ranger as the client's address,"
                                + " so a policy conditioned on an IP matches on a pattern instead",
                        "10.0.0.7", request.getClientIPAddress());

                // No connection behind the check - a background job, a replay - and the pattern is what this
                // source has always sent, so a policy written against it keeps working.
                Assert.assertEquals("%",
                        controller.createRequest(SUBJECT, AccessContext.NONE).getClientIPAddress());
            } finally {
                controller.close();
            }
        }
    }

    private static AccessContext connectedFrom(String clientIp) {
        return new AccessContext() {
            @Override
            public Optional<String> getClientIp() {
                return Optional.of(clientIp);
            }
        };
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

    /**
     * A database and a table are each put to the policy engine as one request, naming the Hive resource the
     * question is about and carrying the access type the requirement maps to.
     *
     * <p>The access type is a string that reaches a Ranger server nobody rebuilds when Doris changes, and it
     * has to be the spelling the stock Hive service definition declares - lower case. Nothing else pinned it
     * on the privilege path: spelling it {@code SELECT} instead left every test here green while matching no
     * Hive policy at all, which is the shape of the bug this release fixes on the data-policy path.
     */
    @Test
    public void testDatabaseAndTableAreAskedAboutOneResourceAtATime() throws AccessDeniedException {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class,
                        (built, ignored) -> Mockito.when(built.isAccessAllowed(
                                        Mockito.any(RangerAccessRequest.class), Mockito.any()))
                                .thenAnswer(RangerHiveAccessControllerTest::allow));
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                controller.checkPrivilege(SUBJECT, AuthorizedResource.database("ctl", "db"),
                        AccessRequirements.SELECT, AccessContext.NONE);
                controller.checkPrivilege(SUBJECT, AuthorizedResource.table("ctl", "db", "tbl"),
                        AccessRequirements.LOAD, AccessContext.NONE);

                ArgumentCaptor<RangerAccessRequest> asked = ArgumentCaptor.forClass(RangerAccessRequest.class);
                Mockito.verify(plugin.constructed().get(0), Mockito.times(2))
                        .isAccessAllowed(asked.capture(), Mockito.any());

                RangerAccessRequest database = asked.getAllValues().get(0);
                Assert.assertEquals("select", database.getAccessType());
                Assert.assertEquals("db", database.getResource().getValue("database"));
                Assert.assertNull(database.getResource().getValue("table"));

                RangerAccessRequest table = asked.getAllValues().get(1);
                Assert.assertEquals("update", table.getAccessType());
                Assert.assertEquals("db", table.getResource().getValue("database"));
                Assert.assertEquals("tbl", table.getResource().getValue("table"));
            } finally {
                controller.close();
            }
        }
    }

    /**
     * The columns of one table go to the policy engine as one batch of requests, one per column, each naming
     * its own column and carrying the access type the requirement maps to - and a column the engine denies
     * refuses the whole check.
     */
    @Test
    public void testColumnsAreAskedAboutAsOneBatchAndADeniedColumnRefusesTheCheck()
            throws AccessDeniedException {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class,
                        (built, ignored) -> Mockito.when(built.isAccessAllowed(
                                        Mockito.anyCollection(), Mockito.any()))
                                .thenAnswer(RangerHiveAccessControllerTest::allowEveryColumnBut));
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                controller.checkPrivilege(SUBJECT,
                        AuthorizedResource.columns("ctl", "db", "tbl", ImmutableSet.of("col1", "col2")),
                        AccessRequirements.SELECT, AccessContext.NONE);

                ArgumentCaptor<Collection<RangerAccessRequest>> asked =
                        ArgumentCaptor.forClass(Collection.class);
                Mockito.verify(plugin.constructed().get(0)).isAccessAllowed(asked.capture(), Mockito.any());
                Assert.assertEquals("one request per column, in one call", 2, asked.getValue().size());
                Set<String> columnsAsked = new LinkedHashSet<>();
                for (RangerAccessRequest request : asked.getValue()) {
                    Assert.assertEquals("select", request.getAccessType());
                    Assert.assertEquals("db", request.getResource().getValue("database"));
                    Assert.assertEquals("tbl", request.getResource().getValue("table"));
                    columnsAsked.add((String) request.getResource().getValue("column"));
                }
                Assert.assertEquals(ImmutableSet.of("col1", "col2"), columnsAsked);

                AccessDeniedException denied = Assert.assertThrows(AccessDeniedException.class,
                        () -> controller.checkPrivilege(SUBJECT, AuthorizedResource.columns("ctl", "db", "tbl",
                                ImmutableSet.of("col1", "secret")), AccessRequirements.SELECT,
                                AccessContext.NONE));
                Assert.assertEquals(RangerHiveAccessController.NAME, denied.getDeniedBy().orElse(null));
            } finally {
                controller.close();
            }
        }
    }

    /**
     * A batch the policy engine has no answer for is refused, not read as "every column is allowed".
     *
     * <p>{@code RangerBasePlugin.isAccessAllowed(Collection, ...)} answers null for the same reasons the
     * single-request form does - policies not downloaded yet, or being cleaned up - and this was the one
     * Ranger entry point still reading that as an answer. What the caller got was a NullPointerException out
     * of a loop with nothing to say about which source could not answer, or why.
     */
    @Test
    public void testABatchWithNoAnswerIsRefusedRatherThanRead() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class,
                        (built, ignored) -> Mockito.when(built.isAccessAllowed(
                                Mockito.anyCollection(), Mockito.any())).thenReturn(null));
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                AccessDeniedException denied = Assert.assertThrows(AccessDeniedException.class,
                        () -> controller.checkPrivilege(SUBJECT, AuthorizedResource.columns("ctl", "db", "tbl",
                                ImmutableSet.of("col1")), AccessRequirements.SELECT, AccessContext.NONE));
                Assert.assertTrue(denied.getMessage(),
                        denied.getMessage().contains(RangerHiveAccessController.NAME));
            } finally {
                controller.close();
            }
        }
    }

    /**
     * Closing a binding cancels its audit flush task, and drains what it buffered before stopping the plugin.
     *
     * <p>Four things here can each be removed without any other case noticing, and each has a cost of its own:
     * never scheduling the task stops auditing entirely; never cancelling it leaks one periodic task per
     * binding closed, against a handler nothing will read again; not draining on the way out loses up to
     * twenty seconds of audit events on every {@code DROP CATALOG}; and draining after {@code cleanup()}
     * drains through a plugin already torn down.
     */
    @Test
    public void testClosingABindingCancelsItsFlusherAndDrainsWhatItBuffered() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            ScheduledFuture<?> scheduled = controller.logFlushFuture;
            Assert.assertNotNull("no audit flush task was scheduled, so this binding audits nothing",
                    scheduled);

            controller.close();

            Assert.assertTrue("the audit flush task outlived the binding that scheduled it",
                    scheduled.isCancelled() || scheduled.isDone());
            Assert.assertNull(controller.logFlushFuture);

            RangerHiveAuditHandler handler = audit.constructed().get(0);
            RangerHivePlugin hive = plugin.constructed().get(0);
            InOrder order = Mockito.inOrder(handler, hive);
            order.verify(handler).flushAudit();
            order.verify(hive).cleanup();
        }
    }

    /** A policy engine that answers, with no row filter to report - the ordinary case for most tables. */
    private static void answerRowFilterLookups(RangerHivePlugin plugin,
            MockedConstruction.Context ignored) {
        Mockito.when(plugin.evalRowFilterPolicies(Mockito.any(), Mockito.any())).thenAnswer(
                invocation -> new RangerAccessResult(RangerPolicy.POLICY_TYPE_ROWFILTER, "hive", null,
                        invocation.getArgument(0)));
    }

    /** Grants whatever is asked, so that what reaches the policy engine is what the case is about. */
    private static RangerAccessResult allow(InvocationOnMock invocation) {
        RangerAccessRequest request = invocation.getArgument(0);
        RangerAccessResult result = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, "hive", null,
                request);
        result.setIsAllowed(true);
        return result;
    }

    /** Grants every column but the one named "secret", so that the refusal has a column to name. */
    private static Collection<RangerAccessResult> allowEveryColumnBut(InvocationOnMock invocation) {
        Collection<RangerAccessRequest> requests = invocation.getArgument(0);
        List<RangerAccessResult> results = new ArrayList<>();
        for (RangerAccessRequest request : requests) {
            RangerAccessResult result = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, "hive", null,
                    request);
            result.setIsAllowed(!"secret".equals(request.getResource().getValue("column")));
            results.add(result);
        }
        return results;
    }
}
