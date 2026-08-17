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

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
            (subject, requirement) -> grantedAtGlobalScope);

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
        Assert.assertEquals(fromTheController.getMessage(),
                new AuthorizationException(denied.getMessage()).getMessage());
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

        Mockito.verify(controller).checkGlobalPriv(USER, PrivPredicate.SELECT);
    }

    @Test
    public void testClosingTheSourceClosesTheController() {
        plugin.close();

        Mockito.verify(controller).close();
    }
}
