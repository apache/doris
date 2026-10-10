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

package org.apache.doris.authorization.spi;

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * What a plugin gets for free, and what it must not get for free.
 *
 * <p>Two properties are being held still here. A plugin that says nothing refuses everything, so a question
 * an author did not think about - a resource kind added after the plugin was written, say - closes rather
 * than opens. And a plugin that answers one action at a time has its requirements taken apart the way the
 * requirement itself says: "any of these" is satisfied by one, "all of these" by nothing less than all of
 * them. Getting that backwards would not fail loudly anywhere - it would quietly let a {@code GRANT}
 * statement through for someone holding only half of what it needs.
 */
public class AuthorizationPluginContractTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("someone", "%");
    private static final AuthorizedResource TABLE = AuthorizedResource.table("ctl", "db", "tbl");

    /** A source that has been asked nothing and answers nothing. */
    private static class SilentPlugin implements AuthorizationPlugin {
        @Override
        public String name() {
            return "silent";
        }
    }

    /** A source that only knows how to answer about one action at a time. */
    private static class PerActionPlugin implements AuthorizationPlugin {
        private final Set<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
        private final List<AccessAction> asked = new ArrayList<>();

        PerActionPlugin(AccessAction... granted) {
            Collections.addAll(this.granted, granted);
        }

        @Override
        public String name() {
            return "per-action";
        }

        @Override
        public void checkAction(AuthorizedSubject subject, AuthorizedResource resource, AccessAction action,
                AccessContext context) throws AccessDeniedException {
            asked.add(action);
            if (!granted.contains(action)) {
                throw AccessDeniedException.of(subject, resource, AccessRequirement.of(action), name());
            }
        }
    }

    private static boolean allows(AuthorizationPlugin plugin, AccessRequirement requirement) {
        try {
            plugin.checkPrivilege(SUBJECT, TABLE, requirement, AccessContext.NONE);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    @Test
    public void testAPluginThatSaysNothingRefusesEverything() {
        SilentPlugin plugin = new SilentPlugin();

        Assertions.assertFalse(allows(plugin, AccessRequirement.of(AccessAction.SELECT)));
        Assertions.assertFalse(allows(plugin, AccessRequirement.anyOf(AccessAction.SELECT, AccessAction.LOAD)));
        Assertions.assertFalse(allows(plugin, AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT)));
        Assertions.assertThrows(AccessDeniedException.class, () -> plugin.checkAction(SUBJECT, TABLE,
                AccessAction.SELECT, AccessContext.NONE));
    }

    /**
     * Refusing is about access, not about policy: a source with no row filter to impose has not thereby
     * refused the table, and returning "denied" here would hide every row of every table from everyone.
     */
    @Test
    public void testSayingNothingMeansNoDataPolicyRatherThanRefusal() {
        SilentPlugin plugin = new SilentPlugin();

        Assertions.assertTrue(plugin.getRowFilters(SUBJECT, AuthorizedResource.table("ctl", "db", "tbl"),
                AccessContext.NONE).isEmpty());
        Assertions.assertTrue(plugin.getDataMasks(SUBJECT, AuthorizedResource.table("ctl", "db", "tbl"),
                Collections.singleton("col"), AccessContext.NONE).isEmpty());
    }

    @Test
    public void testAnyIsSatisfiedByOneActionOfTheSet() {
        Assertions.assertTrue(allows(new PerActionPlugin(AccessAction.LOAD),
                AccessRequirement.anyOf(AccessAction.SELECT, AccessAction.LOAD)));
        Assertions.assertFalse(allows(new PerActionPlugin(AccessAction.DROP),
                AccessRequirement.anyOf(AccessAction.SELECT, AccessAction.LOAD)));
    }

    /**
     * The shape of the check a {@code GRANT} statement makes - the grantor must hold what is being granted
     * <em>and</em> the right to grant it - so reading this as "any" would hand out privileges to someone
     * holding only one of the two.
     */
    @Test
    public void testAllNeedsEveryActionOfTheSet() {
        Assertions.assertTrue(allows(new PerActionPlugin(AccessAction.SELECT, AccessAction.GRANT),
                AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT)));
        Assertions.assertFalse(allows(new PerActionPlugin(AccessAction.SELECT),
                AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT)));
        Assertions.assertFalse(allows(new PerActionPlugin(AccessAction.GRANT),
                AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT)));
    }

    /**
     * "Any of these" stops at the first action that is granted. A source reached over the network pays per
     * question asked, and the ones a satisfied requirement no longer needs are the easiest to stop asking.
     */
    @Test
    public void testAnyStopsAtTheFirstActionThatIsGranted() {
        PerActionPlugin plugin = new PerActionPlugin(AccessAction.SELECT);

        Assertions.assertTrue(allows(plugin, AccessRequirement.anyOf(AccessAction.SELECT, AccessAction.LOAD)));
        Assertions.assertEquals(Collections.singletonList(AccessAction.SELECT), plugin.asked);
    }

    /** A refusal names the source that produced it, which is the whole point of carrying it as an object. */
    @Test
    public void testARefusalSaysWhoRefused() {
        AccessDeniedException denied = Assertions.assertThrows(AccessDeniedException.class,
                () -> new SilentPlugin().checkPrivilege(SUBJECT, TABLE,
                        AccessRequirement.of(AccessAction.SELECT), AccessContext.NONE));

        Assertions.assertEquals("silent", denied.getDeniedBy().orElse(null));
        Assertions.assertSame(TABLE, denied.getResource());
    }
}
