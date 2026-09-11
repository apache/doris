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

package org.apache.doris.authorization;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;

public class AccessRequirementTest {

    @Test
    public void testAnyIsSatisfiedByOneOfTheActions() {
        AccessRequirement requirement = AccessRequirement.anyOf(AccessAction.SELECT, AccessAction.LOAD);

        Assertions.assertTrue(requirement.isSatisfiedBy(EnumSet.of(AccessAction.LOAD)));
        Assertions.assertFalse(requirement.isSatisfiedBy(EnumSet.of(AccessAction.DROP)));
        Assertions.assertFalse(requirement.isSatisfiedBy(EnumSet.noneOf(AccessAction.class)));
    }

    @Test
    public void testAllNeedsEveryAction() {
        // The shape of the check a GRANT statement makes: holding one of the two is not enough.
        AccessRequirement requirement = AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT);

        Assertions.assertTrue(requirement.isSatisfiedBy(EnumSet.of(AccessAction.SELECT, AccessAction.GRANT)));
        Assertions.assertFalse(requirement.isSatisfiedBy(EnumSet.of(AccessAction.SELECT)));
        Assertions.assertFalse(requirement.isSatisfiedBy(EnumSet.of(AccessAction.GRANT)));
    }

    @Test
    public void testRequirementWithoutActionsIsRefused() {
        // Read as "all of", an empty requirement is satisfied by holding nothing at all, so a caller that
        // built one by accident would be granting whatever it was checking.
        Assertions.assertThrows(IllegalArgumentException.class, AccessRequirement::anyOf);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AccessRequirement.of(Collections.emptySet(), ActionMatch.ALL));
    }

    @Test
    public void testActionsCannotBeChangedAfterwards() {
        AccessRequirement requirement = AccessRequirement.of(AccessAction.SELECT);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> requirement.getActions().add(AccessAction.ADMIN));
    }

    @Test
    public void testMatchIsPartOfIdentity() {
        AccessRequirement any = AccessRequirement.anyOf(AccessAction.SELECT, AccessAction.GRANT);
        AccessRequirement all = AccessRequirement.allOf(AccessAction.SELECT, AccessAction.GRANT);

        Assertions.assertNotEquals(any, all);
        Assertions.assertEquals(any, AccessRequirement.anyOf(AccessAction.GRANT, AccessAction.SELECT));
        Assertions.assertEquals(any.hashCode(),
                AccessRequirement.anyOf(AccessAction.GRANT, AccessAction.SELECT).hashCode());
    }

    @Test
    public void testNullsAreRefused() {
        Assertions.assertThrows(NullPointerException.class, () -> AccessRequirement.of((AccessAction) null));
        Assertions.assertThrows(NullPointerException.class,
                () -> AccessRequirement.of(Collections.singleton(AccessAction.SELECT), null));
        Assertions.assertThrows(NullPointerException.class,
                () -> AccessRequirement.of(AccessAction.SELECT).isSatisfiedBy(null));
    }
}
