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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.ActionMatch;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The neutral vocabulary must say exactly what Doris's own vocabulary says - no more, no less.
 *
 * <p>This matters because the built-in privilege model is one of the sources being asked. A translation that
 * merges two privileges into one action, or that turns "all of these" into "any of these", would hand it a
 * question different from the one the caller asked and it would answer that different question truthfully.
 * The round trip is therefore tested as an identity on the bits, not as "every constant has an entry": most
 * predicates are not constants at all - {@code GRANT SELECT ON t TO u} builds one on the spot out of the
 * privileges named in the statement, and it is the only place the "all of these" form occurs.
 */
public class AccessTranslationTest {

    /** The privileges a caller can actually ask for; the rest are retired bit indices kept for old metadata. */
    private static final List<Privilege> LIVE_PRIVILEGES = Arrays.asList(
            Privilege.NODE_PRIV, Privilege.ADMIN_PRIV, Privilege.GRANT_PRIV, Privilege.SELECT_PRIV,
            Privilege.LOAD_PRIV, Privilege.ALTER_PRIV, Privilege.CREATE_PRIV, Privilege.DROP_PRIV,
            Privilege.USAGE_PRIV, Privilege.CLUSTER_USAGE_PRIV, Privilege.STAGE_USAGE_PRIV,
            Privilege.SHOW_VIEW_PRIV);

    @Test
    public void testEveryPrivilegeHasAnAction() {
        for (Privilege privilege : Privilege.values()) {
            // Retired privileges are included on purpose: a stored grant read back in the mode that owns
            // its bit index comes out as one of them, and a missing entry would only fail at run time.
            Assert.assertNotNull("privilege " + privilege + " has no access action",
                    AccessTranslation.actionOf(privilege));
        }
    }

    @Test
    public void testEveryActionHasAPrivilege() {
        for (AccessAction action : AccessAction.values()) {
            Assert.assertNotNull("action " + action + " has no privilege",
                    AccessTranslation.privilegeOf(action));
        }
    }

    @Test
    public void testEveryPredicateConstantSurvivesTheRoundTrip() {
        for (Map.Entry<String, PrivPredicate> constant : allPrivPredicates().entrySet()) {
            assertRoundTrips("PrivPredicate." + constant.getKey(), constant.getValue());
        }
    }

    @Test
    public void testEveryPredicateConstantTranslatesBackToItself() {
        // Value equality is not enough. Role grants "may see this catalog" when any object under it is
        // reachable, and the Ranger controllers pick their access type, by comparing the predicate against
        // a constant with ==. A translation that returns an equal but different object drops those branches
        // for every user who depended on them, which is what the behaviour baseline caught the first time
        // this was written.
        Map<String, PrivPredicate> constants = allPrivPredicates();
        Set<String> askTheSameQuestion = constantsSharingAQuestion(constants);

        for (Map.Entry<String, PrivPredicate> constant : constants.entrySet()) {
            PrivPredicate translated =
                    AccessTranslation.privPredicateOf(AccessTranslation.requirementOf(constant.getValue()));
            if (askTheSameQuestion.contains(constant.getKey())) {
                // Two constants naming the same privileges with the same match are interchangeable.
                Assert.assertEquals(bitsOf(constant.getValue()), bitsOf(translated));
            } else {
                Assert.assertSame("PrivPredicate." + constant.getKey() + " must translate back to itself",
                        constant.getValue(), translated);
            }
        }
    }

    @Test
    public void testEveryCombinationOfPrivilegesSurvivesTheRoundTrip() {
        int combinations = 1 << LIVE_PRIVILEGES.size();
        for (int mask = 1; mask < combinations; mask++) {
            List<Privilege> privileges = new ArrayList<>();
            for (int i = 0; i < LIVE_PRIVILEGES.size(); i++) {
                if ((mask & (1 << i)) != 0) {
                    privileges.add(LIVE_PRIVILEGES.get(i));
                }
            }
            assertRoundTrips("any of " + privileges, PrivPredicate.of(PrivBitSet.of(privileges), Operator.OR));
            assertRoundTrips("all of " + privileges, PrivPredicate.of(PrivBitSet.of(privileges), Operator.AND));
        }
    }

    @Test
    public void testGrantStatementPredicateKeepsItsAllSemantics() {
        // What GrantTablePrivilegeCommand builds: the privilege being granted plus the right to grant it,
        // and the caller must hold both. Read as "any of", holding either one would be enough to hand out
        // the other, which is the difference between an authorization model and none.
        PrivPredicate wanted = PrivPredicate.of(
                PrivBitSet.of(Privilege.SELECT_PRIV, Privilege.GRANT_PRIV), Operator.AND);

        AccessRequirement requirement = AccessTranslation.requirementOf(wanted);

        Assert.assertEquals(ActionMatch.ALL, requirement.getMatch());
        Assert.assertEquals(EnumSet.of(AccessAction.SELECT, AccessAction.GRANT), requirement.getActions());
        Assert.assertEquals(Operator.AND, AccessTranslation.privPredicateOf(requirement).getOp());
    }

    @Test
    public void testVisibilityBecomesAnyOfThePrivilegesThatImplyIt() {
        // "May this subject see the object" is not a privilege anyone grants; it is a question about
        // holding any of several. If it ever translated to a single action, every source would have to
        // invent a privilege named SHOW that nobody can be granted.
        AccessRequirement requirement = AccessTranslation.requirementOf(PrivPredicate.SHOW);

        Assert.assertEquals(ActionMatch.ANY, requirement.getMatch());
        Assert.assertEquals(EnumSet.of(AccessAction.ADMIN, AccessAction.SELECT, AccessAction.LOAD,
                AccessAction.ALTER, AccessAction.CREATE, AccessAction.DROP, AccessAction.SHOW_VIEW),
                requirement.getActions());
    }

    @Test
    public void testUsageFlavoursStayApart() {
        // Ranger folds all three into one access type, and that is Ranger's business. Folding them here
        // would leave the built-in model unable to tell which of its three privilege bits was asked about.
        AccessRequirement requirement = AccessTranslation.requirementOf(PrivPredicate.USAGE);

        Assert.assertTrue(requirement.getActions().containsAll(EnumSet.of(
                AccessAction.USAGE, AccessAction.CLUSTER_USAGE, AccessAction.STAGE_USAGE)));
        assertRoundTrips("PrivPredicate.USAGE", PrivPredicate.USAGE);
    }

    @Test
    public void testPredicateNamingNoPrivilegeIsRefused() {
        // Such a predicate is satisfied by everyone under "all of" - nothing to hold - and by no one under
        // "any of". Passing it on would decide access by accident, so it fails where it is built instead.
        PrivPredicate empty = PrivPredicate.of(PrivBitSet.of(), Operator.AND);

        Assert.assertThrows(IllegalArgumentException.class, () -> AccessTranslation.requirementOf(empty));
    }

    @Test
    public void testCloudResourceKindsRoundTrip() {
        for (ResourceTypeEnum type : ResourceTypeEnum.values()) {
            ResourceKind kind = AccessTranslation.cloudKindOf(type);
            Assert.assertEquals("cloud resource type " + type, type, AccessTranslation.cloudTypeOf(kind));
        }
    }

    @Test
    public void testOnlyCloudKindsHaveACloudResourceType() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> AccessTranslation.cloudTypeOf(ResourceKind.TABLE));
    }

    /**
     * An account survives the trip out to a source and back into the privilege tables.
     *
     * <p>The built-in model is itself one of the sources, and it looks accounts up in tables keyed by
     * account. So the neutral form has to carry everything an account is identified by - and only a test can
     * say that it does, because dropping a part of an identity does not fail: it silently matches a
     * different account's grants, or none.
     */
    @Test
    public void testAnAccountSurvivesTheRoundTrip() {
        for (UserIdentity original : Arrays.asList(
                UserIdentity.createAnalyzedUserIdentWithIp("user", "192.168.%"),
                UserIdentity.createAnalyzedUserIdentWithIp("user", "%"),
                UserIdentity.createAnalyzedUserIdentWithDomain("user", "doris.example.com"),
                UserIdentity.ROOT,
                UserIdentity.ADMIN,
                // Callers that check access with an identity they never put through analysis exist - the
                // cloud checks are reached that way. Translating one must answer, not object.
                new UserIdentity("user", "%"))) {
            UserIdentity translated = AccessTranslation.userIdentityOf(AccessTranslation.subjectOf(original));

            Assert.assertEquals(original.toString(), original, translated);
            Assert.assertEquals(original.toString(), original.getUser(), translated.getUser());
            Assert.assertEquals(original.toString(), original.getHost(), translated.getHost());
            Assert.assertEquals(original.toString(), original.isDomain(), translated.isDomain());
        }
    }

    /**
     * Two accounts that differ only in where they connect from are different accounts with different grants,
     * so they must not collapse into one subject on the way out.
     */
    @Test
    public void testAccountsDifferingOnlyInHostStayApart() {
        AuthorizedSubject anywhere = AccessTranslation.subjectOf(
                UserIdentity.createAnalyzedUserIdentWithIp("user", "%"));
        AuthorizedSubject fromOffice = AccessTranslation.subjectOf(
                UserIdentity.createAnalyzedUserIdentWithIp("user", "192.168.%"));
        AuthorizedSubject inDomain = AccessTranslation.subjectOf(
                UserIdentity.createAnalyzedUserIdentWithDomain("user", "192.168.%"));

        Assert.assertNotEquals(anywhere, fromOffice);
        Assert.assertNotEquals(fromOffice, inDomain);
    }

    private void assertRoundTrips(String what, PrivPredicate wanted) {
        PrivPredicate translated = AccessTranslation.privPredicateOf(AccessTranslation.requirementOf(wanted));
        Assert.assertEquals(what + ": privileges changed", bitsOf(wanted), bitsOf(translated));
        Assert.assertEquals(what + ": match changed", wanted.getOp(), translated.getOp());
    }

    private long bitsOf(PrivPredicate predicate) {
        return Deencapsulation.getField(predicate.getPrivs(), "set");
    }

    /** The constants some other constant is indistinguishable from: same privileges, same match. */
    private static Set<String> constantsSharingAQuestion(Map<String, PrivPredicate> constants) {
        Map<AccessRequirement, List<String>> byQuestion = new HashMap<>();
        for (Map.Entry<String, PrivPredicate> constant : constants.entrySet()) {
            byQuestion.computeIfAbsent(AccessTranslation.requirementOf(constant.getValue()),
                    key -> new ArrayList<>()).add(constant.getKey());
        }
        Set<String> shared = new HashSet<>();
        for (List<String> names : byQuestion.values()) {
            if (names.size() > 1) {
                shared.addAll(names);
            }
        }
        return shared;
    }

    private static Map<String, PrivPredicate> allPrivPredicates() {
        Map<String, PrivPredicate> sorted = new TreeMap<>();
        for (Field field : PrivPredicate.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == PrivPredicate.class) {
                try {
                    sorted.put(field.getName(), (PrivPredicate) field.get(null));
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("cannot read PrivPredicate." + field.getName(), e);
                }
            }
        }
        Assert.assertFalse("no PrivPredicate constants found", sorted.isEmpty());
        return sorted;
    }
}
