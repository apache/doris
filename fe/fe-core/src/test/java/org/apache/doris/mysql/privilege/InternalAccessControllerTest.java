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
import org.apache.doris.common.AuthorizationException;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * The built-in controller answers "globally, then at this scope".
 *
 * <p>Both halves of that sentence are load bearing. Answering globally first is what lets an administrator
 * reach a resource no grant names, and it must happen <em>before</em> the scoped lookup rather than instead of
 * it - the scoped lookups are the expensive ones, and one of them refuses to answer at all for privileges that
 * only exist globally. The engine used to arrange this order on every controller's behalf; now each controller
 * owns it, so these tests watch the built-in one keep it.
 */
public class InternalAccessControllerTest {
    private static final UserIdentity USER = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

    private final Auth auth = Mockito.mock(Auth.class);
    private final InternalAccessController controller = new InternalAccessController(auth);

    private void holdsGlobally(boolean granted) {
        Mockito.when(auth.checkGlobalPriv(USER, PrivPredicate.SELECT)).thenReturn(granted);
    }

    @Test
    public void testCatalogCheckIsSkippedWhenPrivilegeIsHeldGlobally() {
        holdsGlobally(true);

        Assert.assertTrue(controller.checkCtlPriv(USER, "ctl", PrivPredicate.SELECT));
        Mockito.verify(auth, Mockito.never()).checkCtlPriv(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCatalogCheckDecidesWhenPrivilegeIsNotHeldGlobally() {
        holdsGlobally(false);
        Mockito.when(auth.checkCtlPriv(USER, "ctl", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(controller.checkCtlPriv(USER, "ctl", PrivPredicate.SELECT));
        Assert.assertFalse(controller.checkCtlPriv(USER, "other_ctl", PrivPredicate.SELECT));
    }

    @Test
    public void testDatabaseCheckIsSkippedWhenPrivilegeIsHeldGlobally() {
        holdsGlobally(true);

        Assert.assertTrue(controller.checkDbPriv(USER, "ctl", "db", PrivPredicate.SELECT));
        Mockito.verify(auth, Mockito.never())
                .checkDbPriv(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDatabaseCheckDecidesWhenPrivilegeIsNotHeldGlobally() {
        holdsGlobally(false);
        Mockito.when(auth.checkDbPriv(USER, "ctl", "db", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(controller.checkDbPriv(USER, "ctl", "db", PrivPredicate.SELECT));
        Assert.assertFalse(controller.checkDbPriv(USER, "ctl", "other_db", PrivPredicate.SELECT));
    }

    @Test
    public void testTableCheckIsSkippedWhenPrivilegeIsHeldGlobally() {
        holdsGlobally(true);

        Assert.assertTrue(controller.checkTblPriv(USER, "ctl", "db", "tbl", PrivPredicate.SELECT));
        Mockito.verify(auth, Mockito.never()).checkTblPriv(
                Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testTableCheckDecidesWhenPrivilegeIsNotHeldGlobally() {
        holdsGlobally(false);
        Mockito.when(auth.checkTblPriv(USER, "ctl", "db", "tbl", PrivPredicate.SELECT)).thenReturn(true);

        Assert.assertTrue(controller.checkTblPriv(USER, "ctl", "db", "tbl", PrivPredicate.SELECT));
        Assert.assertFalse(controller.checkTblPriv(USER, "ctl", "db", "other_tbl", PrivPredicate.SELECT));
    }

    @Test
    public void testColumnCheckIsSkippedWhenPrivilegeIsHeldGlobally() throws Exception {
        holdsGlobally(true);

        controller.checkColsPriv(USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);
        Mockito.verify(auth, Mockito.never()).checkColsPriv(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testColumnCheckDecidesWhenPrivilegeIsNotHeldGlobally() throws Exception {
        holdsGlobally(false);

        controller.checkColsPriv(USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);
        Mockito.verify(auth).checkColsPriv(USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);
    }

    @Test
    public void testColumnDenialIsReportedWhenPrivilegeIsNotHeldGlobally() throws Exception {
        holdsGlobally(false);
        Mockito.doThrow(new AuthorizationException("denied")).when(auth).checkColsPriv(
                USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);

        Assert.assertThrows(AuthorizationException.class, () -> controller.checkColsPriv(
                USER, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT));
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

        Assert.assertTrue(controller.checkDbPriv(USER, "ctl", "db", PrivPredicate.OPERATOR));
    }
}
