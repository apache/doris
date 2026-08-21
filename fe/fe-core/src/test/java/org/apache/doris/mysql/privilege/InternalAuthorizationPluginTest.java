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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.common.AuthorizationException;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * The built-in source answers "globally, then at this scope".
 *
 * <p>Both halves of that sentence are load bearing. Answering globally first is what lets an administrator
 * reach a resource no grant names, and it must happen <em>before</em> the scoped lookup rather than instead of
 * it - the scoped lookups are the expensive ones, and one of them refuses to answer at all for privileges that
 * only exist globally. The engine used to arrange this order on every source's behalf; now each source owns
 * it, so these tests watch the built-in one keep it.
 *
 * <p>They also pin the translation on the way in, without asserting anything about it directly: the mocked
 * {@link Auth} is told to expect the identity and the predicate the caller started from, so a translation
 * that produced anything else would leave every expectation unmatched.
 */
public class InternalAuthorizationPluginTest {
    private static final UserIdentity USER = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
    private static final AuthorizedSubject SUBJECT = AccessTranslation.subjectOf(USER);
    private static final AccessRequirement SELECT = AccessTranslation.requirementOf(PrivPredicate.SELECT);

    private final Auth auth = Mockito.mock(Auth.class);
    private final InternalAuthorizationPlugin plugin = new InternalAuthorizationPlugin(auth);

    private void holdsGlobally(boolean granted) {
        Mockito.when(auth.checkGlobalPriv(USER, PrivPredicate.SELECT)).thenReturn(granted);
    }

    private boolean allows(AuthorizedResource resource) {
        return allows(resource, SELECT);
    }

    private boolean allows(AuthorizedResource resource, AccessRequirement requirement) {
        try {
            plugin.checkPrivilege(SUBJECT, resource, requirement, AccessContext.NONE);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    @Test
    public void testCatalogCheckIsSkippedWhenPrivilegeIsHeldGlobally() {
        holdsGlobally(true);

        Assert.assertTrue(allows(AuthorizedResource.catalog("ctl")));
        Mockito.verify(auth, Mockito.never()).checkCtlPriv(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCatalogCheckDecidesWhenPrivilegeIsNotHeldGlobally() {
        holdsGlobally(false);
        Mockito.when(auth.checkCtlPriv(USER, "ctl", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.catalog("ctl")));
        Assert.assertFalse(allows(AuthorizedResource.catalog("other_ctl")));
    }

    @Test
    public void testDatabaseCheckIsSkippedWhenPrivilegeIsHeldGlobally() {
        holdsGlobally(true);

        Assert.assertTrue(allows(AuthorizedResource.database("ctl", "db")));
        Mockito.verify(auth, Mockito.never())
                .checkDbPriv(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDatabaseCheckDecidesWhenPrivilegeIsNotHeldGlobally() {
        holdsGlobally(false);
        Mockito.when(auth.checkDbPriv(USER, "ctl", "db", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.database("ctl", "db")));
        Assert.assertFalse(allows(AuthorizedResource.database("ctl", "other_db")));
    }

    @Test
    public void testTableCheckIsSkippedWhenPrivilegeIsHeldGlobally() {
        holdsGlobally(true);

        Assert.assertTrue(allows(AuthorizedResource.table("ctl", "db", "tbl")));
        Mockito.verify(auth, Mockito.never()).checkTblPriv(
                Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testTableCheckDecidesWhenPrivilegeIsNotHeldGlobally() {
        holdsGlobally(false);
        Mockito.when(auth.checkTblPriv(USER, "ctl", "db", "tbl", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.table("ctl", "db", "tbl")));
        Assert.assertFalse(allows(AuthorizedResource.table("ctl", "db", "other_tbl")));
    }

    @Test
    public void testColumnCheckIsSkippedWhenPrivilegeIsHeldGlobally() throws Exception {
        holdsGlobally(true);

        plugin.checkPrivilege(SUBJECT, AuthorizedResource.columns("ctl", "db", "tbl", ImmutableSet.of("col1")),
                SELECT, AccessContext.NONE);
        Mockito.verify(auth, Mockito.never()).checkColsPriv(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testColumnCheckDecidesWhenPrivilegeIsNotHeldGlobally() throws Exception {
        holdsGlobally(false);

        plugin.checkPrivilege(SUBJECT, AuthorizedResource.columns("ctl", "db", "tbl", ImmutableSet.of("col1")),
                SELECT, AccessContext.NONE);
        Mockito.verify(auth).checkColsPriv(USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);
    }

    /**
     * A refused column check answers with the column that failed, so the wording the privilege model produced
     * has to survive the trip out - restating it in terms of the whole column set would lose which one it was.
     *
     * <p>The bare wording is the claim, and {@code assertEquals} on it is the whole of the gate. The refusal
     * leaves the privilege model as an {@link AuthorizationException} and reaches the caller as one again,
     * and that class renders itself with its error code in front; carrying the rendered form across the
     * middle instead would put the prefix in twice on the way out. Counting the prefixes in the rendered
     * result cannot see that - {@code UserException.deleteUselessMsg} strips exactly one repetition, so both
     * worlds render identically - which is why what is pinned here is what crosses the boundary.
     */
    @Test
    public void testColumnDenialKeepsTheMessageNamingTheColumn() throws Exception {
        holdsGlobally(false);
        AuthorizationException fromThePrivilegeModel = new AuthorizationException("denied on col1");
        Mockito.doThrow(fromThePrivilegeModel).when(auth).checkColsPriv(
                USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);

        AccessDeniedException denied = Assert.assertThrows(AccessDeniedException.class,
                () -> plugin.checkPrivilege(SUBJECT,
                        AuthorizedResource.columns("ctl", "db", "tbl", ImmutableSet.of("col1")),
                        SELECT, AccessContext.NONE));

        Assert.assertEquals("the wording naming the column was restated on the way out, so what the user"
                        + " reads no longer says which column was refused",
                "denied on col1", denied.getMessage());
    }

    /**
     * A caller holding only global NODE_PRIV is why the global check has to run first instead of being folded
     * into the scoped one: {@link Auth} refuses NODE privileges below global level, so the scoped lookup would
     * turn the administrator away.
     */
    @Test
    public void testGloballyHeldNodePrivilegeIsNotRefusedByTheScopedCheck() {
        Mockito.when(auth.checkGlobalPriv(USER, PrivPredicate.OPERATOR)).thenReturn(true);
        Mockito.when(auth.checkDbPriv(USER, "ctl", "db", PrivPredicate.OPERATOR)).thenReturn(false);

        Assert.assertTrue(allows(AuthorizedResource.database("ctl", "db"),
                AccessTranslation.requirementOf(PrivPredicate.OPERATOR)));
    }

    /**
     * The system-wide objects are deliberately <em>not</em> preceded by a global check here: {@link Auth}
     * folds the global grants into those answers itself, and asking twice would evaluate the same grants
     * again for the same answer.
     */
    @Test
    public void testSystemWideObjectsAreLeftEntirelyToTheGrantLookup() {
        Mockito.when(auth.checkResourcePriv(USER, "res", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(allows(AuthorizedResource.resource("res")));
        Mockito.verify(auth, Mockito.never()).checkGlobalPriv(Mockito.any(), Mockito.any());
    }
}
