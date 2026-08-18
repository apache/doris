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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.common.AuthorizationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A controller written against the older per-scope interface, asked the way an authorization source is asked.
 *
 * <p>The translation this covers is a lookup table with no logic in it, which is exactly why it is worth
 * testing: every resource kind has to reach the one method that used to answer for it, and a wire crossed
 * between two of them - a storage vault asked about as if it were a resource - produces a plausible answer
 * from the wrong policy set, silently. So each kind is checked against the method it must land on, and each
 * is checked to be refused when that method says no.
 */
public class LegacyAccessControllerPluginTest {

    private static final UserIdentity USER = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
    private static final AuthorizedSubject SUBJECT = AccessTranslation.subjectOf(USER);
    private static final AccessRequirement SELECT = AccessTranslation.requirementOf(PrivPredicate.SELECT);

    private final CatalogAccessController controller = Mockito.mock(CatalogAccessController.class);
    // What the instance-scope source answers, which the adapter grants on; false unless a case says so.
    private boolean grantedAtGlobalScope;
    private final LegacyAccessControllerPlugin plugin = new LegacyAccessControllerPlugin("legacy", controller,
            (subject, requirement, context) -> grantedAtGlobalScope);

    private boolean allows(AuthorizedResource resource) {
        try {
            plugin.checkPrivilege(SUBJECT, resource, SELECT, AccessContext.NONE);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    @Test
    public void testGlobalGoesToTheGlobalCheck() {
        Mockito.when(controller.checkGlobalPriv(USER, PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.global()));
    }

    @Test
    public void testCatalogGoesToTheCatalogCheck() {
        Mockito.when(controller.checkCtlPriv(USER, "ctl", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.catalog("ctl")));
        Assert.assertFalse(allows(AuthorizedResource.catalog("other")));
    }

    @Test
    public void testDatabaseGoesToTheDatabaseCheck() {
        Mockito.when(controller.checkDbPriv(USER, "ctl", "db", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.database("ctl", "db")));
        Assert.assertFalse(allows(AuthorizedResource.database("ctl", "other")));
    }

    @Test
    public void testTableGoesToTheTableCheck() {
        Mockito.when(controller.checkTblPriv(USER, "ctl", "db", "tbl", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.table("ctl", "db", "tbl")));
        Assert.assertFalse(allows(AuthorizedResource.table("ctl", "db", "other")));
    }

    /**
     * The three system-wide names are the ones a crossed wire would hide best: they are all "a name and a
     * privilege", so asking about a vault through the resource check would look like it worked and would
     * consult grants nobody made about the vault.
     */
    @Test
    public void testEachSystemWideNameGoesToItsOwnCheck() {
        Mockito.when(controller.checkResourcePriv(USER, "name", PrivPredicate.SELECT)).thenReturn(true);
        Mockito.when(controller.checkWorkloadGroupPriv(USER, "name", PrivPredicate.SELECT)).thenReturn(false);
        Mockito.when(controller.checkStorageVaultPriv(USER, "name", PrivPredicate.SELECT)).thenReturn(false);

        Assert.assertTrue(allows(AuthorizedResource.resource("name")));
        Assert.assertFalse(allows(AuthorizedResource.workloadGroup("name")));
        Assert.assertFalse(allows(AuthorizedResource.storageVault("name")));

        Mockito.verify(controller).checkResourcePriv(USER, "name", PrivPredicate.SELECT);
        Mockito.verify(controller).checkWorkloadGroupPriv(USER, "name", PrivPredicate.SELECT);
        Mockito.verify(controller).checkStorageVaultPriv(USER, "name", PrivPredicate.SELECT);
    }

    /** Which cloud object it is travels along, because the privileges live in different tables per type. */
    @Test
    public void testCloudObjectsCarryTheirTypeThrough() {
        Mockito.when(controller.checkCloudPriv(USER, "cg", PrivPredicate.SELECT, ResourceTypeEnum.CLUSTER))
                .thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.cloud(ResourceKind.CLOUD_COMPUTE_GROUP, "cg")));
        Assert.assertFalse(allows(AuthorizedResource.cloud(ResourceKind.CLOUD_STAGE, "cg")));
    }

    @Test
    public void testColumnsGoToTheColumnCheck() throws Exception {
        Set<String> columns = ImmutableSet.of("col1", "col2");

        plugin.checkPrivilege(SUBJECT, AuthorizedResource.columns("ctl", "db", "tbl", columns), SELECT,
                AccessContext.NONE);

        Mockito.verify(controller).checkColsPriv(USER, "ctl", "db", "tbl", columns, PrivPredicate.SELECT);
    }

    /**
     * The refused column is named in the message; that name is the answer and has to arrive intact - and in
     * the bare form the controller wrote it, because the engine puts it back into an
     * {@link AuthorizationException} on the way out and that class renders its own error code in front.
     * Carrying the rendered form across would print that prefix twice on every denied column query.
     */
    @Test
    public void testAColumnRefusalKeepsTheMessageThatNamesTheColumn() throws Exception {
        AuthorizationException fromTheController = new AuthorizationException("no privilege on [col2]");
        Mockito.doThrow(fromTheController).when(controller).checkColsPriv(
                Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.any());

        AccessDeniedException denied = Assert.assertThrows(AccessDeniedException.class,
                () -> plugin.checkPrivilege(SUBJECT,
                        AuthorizedResource.columns("ctl", "db", "tbl", ImmutableSet.of("col1", "col2")),
                        SELECT, AccessContext.NONE));

        Assert.assertEquals("no privilege on [col2]", denied.getMessage());
        Assert.assertEquals("legacy", denied.getDeniedBy().orElse(null));
        // Carried as the bare wording, not as rendered: the engine wraps this in an AuthorizationException
        // again on the way out, and that class puts its own error code in front of whatever it is given. The
        // count is the claim - carrying the rendered form renders the code twice and the user reads both.
        Assert.assertEquals("the error code is rendered more than once in what the user reads: "
                        + new AuthorizationException(denied.getMessage()).getMessage(),
                1, StringUtils.countMatches(
                        new AuthorizationException(denied.getMessage()).getMessage(), "detailMessage"));
    }

    /**
     * The batch mask question is asked of a controller that only answers per column, so it asks per column -
     * and reports only the columns that came back masked, an absent entry being how "not masked" is said.
     */
    @Test
    public void testDataMasksAreCollectedOneColumnAtATime() {
        DataMaskSpec spec = new DataMaskSpec("policy", "CONCAT(LEFT(col1,1),'***')");
        Mockito.when(controller.evalDataMaskPolicy(USER, "ctl", "db", "tbl", "col1"))
                .thenReturn(Optional.of(spec));
        Mockito.when(controller.evalDataMaskPolicy(USER, "ctl", "db", "tbl", "col2"))
                .thenReturn(Optional.empty());

        Map<String, DataMaskSpec> masks = plugin.getDataMasks(SUBJECT,
                AuthorizedResource.table("ctl", "db", "tbl"),
                new LinkedHashSet<>(ImmutableSet.of("col1", "col2")), AccessContext.NONE);

        Assert.assertEquals(ImmutableSet.of("col1"), masks.keySet());
        Assert.assertSame(spec, masks.get("col1"));
    }

    /**
     * The grant the older interface was handed and the current one is not.
     *
     * <p>Its scoped methods came in pairs - {@code checkDbPriv(boolean hasGlobal, ...)} in front of
     * {@code checkDbPriv(...)} - and the engine computed {@code hasGlobal} from whoever governed instance
     * scope, so a caller holding the privilege globally was granted without the controller being asked at
     * all. Those default methods are gone. A third-party controller upgraded across that release refuses
     * nothing it used to refuse only if the adapter reproduces the exemption, which is what this pins: the
     * controller here says no to every scope, and every scope inside a catalog is allowed anyway.
     */
    @Test
    public void testAGlobalGrantIsHonouredWithoutAskingTheController() throws Exception {
        grantedAtGlobalScope = true;

        Assert.assertTrue(allows(AuthorizedResource.catalog("ctl")));
        Assert.assertTrue(allows(AuthorizedResource.database("ctl", "db")));
        Assert.assertTrue(allows(AuthorizedResource.table("ctl", "db", "tbl")));
        plugin.checkPrivilege(SUBJECT, AuthorizedResource.columns("ctl", "db", "tbl",
                ImmutableSet.of("col1")), SELECT, AccessContext.NONE);

        Mockito.verify(controller, Mockito.never()).checkCtlPriv(Mockito.any(), Mockito.anyString(),
                Mockito.any());
        Mockito.verify(controller, Mockito.never()).checkDbPriv(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any());
        Mockito.verify(controller, Mockito.never()).checkTblPriv(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.verify(controller, Mockito.never()).checkColsPriv(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    /**
     * And it stops there. A global grant is not a global override: the global question itself is what the
     * exemption is made of, so it goes to the controller, and the system-wide names never had a
     * {@code hasGlobal} form to begin with.
     */
    @Test
    public void testAGlobalGrantDoesNotAnswerForGlobalOrSystemWideNames() {
        grantedAtGlobalScope = true;
        Mockito.when(controller.checkGlobalPriv(USER, PrivPredicate.SELECT)).thenReturn(false);

        Assert.assertFalse(allows(AuthorizedResource.global()));
        Assert.assertFalse(allows(AuthorizedResource.resource("name")));
        Assert.assertFalse(allows(AuthorizedResource.workloadGroup("name")));
        Assert.assertFalse(allows(AuthorizedResource.storageVault("name")));
        // The cloud kinds too, all four of them: they are system-wide names as well, and none of them had a
        // hasGlobal form either - checkCloudPriv came in one shape only.
        for (ResourceKind kind : ImmutableList.of(ResourceKind.CLOUD_GENERAL, ResourceKind.CLOUD_COMPUTE_GROUP,
                ResourceKind.CLOUD_STAGE, ResourceKind.CLOUD_STORAGE_VAULT)) {
            Assert.assertFalse(kind.name(), allows(AuthorizedResource.cloud(kind, "name")));
        }

        Mockito.verify(controller).checkGlobalPriv(USER, PrivPredicate.SELECT);
    }

    /**
     * The global-scope question this adapter asks on the controller's behalf is about the same statement the
     * controller was asked about, so it carries that check's circumstances rather than reading the thread.
     *
     * <p>Whoever governs instance scope may be a source that decides from more than the subject - the client
     * address, say - and a check can arrive before its connection is on the thread, where the thread holds
     * another request's or none at all.
     */
    @Test
    public void testTheGlobalScopeQuestionCarriesTheContextOfTheCheck() {
        AtomicReference<AccessContext> seenByTheAuthority = new AtomicReference<>();
        LegacyAccessControllerPlugin adapter = new LegacyAccessControllerPlugin("legacy", controller,
                (subject, requirement, context) -> {
                    seenByTheAuthority.set(context);
                    return false;
                });
        AccessContext given = new AccessContext() {
            @Override
            public Optional<String> getClientIp() {
                return Optional.of("10.0.0.7");
            }
        };

        Assert.assertThrows(AccessDeniedException.class, () -> adapter.checkPrivilege(SUBJECT,
                AuthorizedResource.table("ctl", "db", "tbl"), SELECT, given));

        Assert.assertSame("the question was asked about circumstances nobody stated",
                given, seenByTheAuthority.get());
    }

    /**
     * A controller built against the data policy types this release deleted is refused where its answer
     * crosses back, with a message naming the source and what has to be done about it.
     *
     * <p>Both signatures erase, so such a controller loads, answers every privilege check, and runs its data
     * policy method to completion; what it hands back is a {@code RowPolicy} or a {@code DataMaskPolicy}. The
     * row filter one used to be found several frames later, by a Nereids rule, as a bare
     * {@code ClassCastException} with nothing in it about which source produced the object or why - and the
     * one thing an operator has to be told there is "recompile your access controller".
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testAnAnswerOfTheOldPayloadTypeIsRefusedWithTheRecompileMessage() {
        List whateverTheOldTypeWas = ImmutableList.of(new Object());
        Mockito.when(controller.evalRowFilterPolicies(USER, "ctl", "db", "tbl"))
                .thenReturn(whateverTheOldTypeWas);
        Mockito.when(controller.evalDataMaskPolicy(USER, "ctl", "db", "tbl", "col1"))
                .thenReturn((Optional) Optional.of(new Object()));

        AuthorizedResource.Table table = AuthorizedResource.table("ctl", "db", "tbl");
        IllegalStateException refusedFilter = Assert.assertThrows(IllegalStateException.class,
                () -> plugin.getRowFilters(SUBJECT, table, AccessContext.NONE));
        Assert.assertTrue(refusedFilter.getMessage(), refusedFilter.getMessage().contains("legacy"));
        Assert.assertTrue(refusedFilter.getMessage(), refusedFilter.getMessage().contains("recompiled"));

        IllegalStateException refusedMask = Assert.assertThrows(IllegalStateException.class,
                () -> plugin.getDataMasks(SUBJECT, table, ImmutableSet.of("col1"), AccessContext.NONE));
        Assert.assertTrue(refusedMask.getMessage(), refusedMask.getMessage().contains("recompiled"));
    }

    /**
     * The predicate a controller is handed is the one the caller named.
     *
     * <p>{@code SHOW_RESOURCES} and {@code SHOW_WORKLOAD_GROUP} name the same privileges with the same
     * operator, so they are one value and the requirement they translate to cannot tell them apart by
     * equality. Comparing a predicate against a constant with {@code ==} is established practice for
     * implementations of this interface - the reference implementations in this repository did it - so being
     * handed the other half of the pair silently takes a branch away from a controller that upgrades.
     */
    @Test
    public void testTheControllerIsHandedThePredicateTheCallerNamed() {
        ArgumentCaptor<PrivPredicate> asked = ArgumentCaptor.forClass(PrivPredicate.class);
        Mockito.when(controller.checkWorkloadGroupPriv(Mockito.any(), Mockito.anyString(), asked.capture()))
                .thenReturn(true);

        try {
            plugin.checkPrivilege(SUBJECT, AuthorizedResource.workloadGroup("wg"),
                    AccessTranslation.requirementOf(PrivPredicate.SHOW_WORKLOAD_GROUP), AccessContext.NONE);
        } catch (AccessDeniedException e) {
            throw new AssertionError(e);
        }

        Assert.assertSame("the adapter handed the controller the other constant of the colliding pair",
                PrivPredicate.SHOW_WORKLOAD_GROUP, asked.getValue());
    }

    @Test
    public void testClosingTheSourceClosesTheController() {
        plugin.close();

        Mockito.verify(controller).close();
    }
}
