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

import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * The questions the engine asks by name mean the same thing on both sides of the plugin contract.
 *
 * <p>A source outside this repository recognises a check by comparing it against {@link AccessRequirements};
 * the engine builds that check out of a {@link PrivPredicate}. If the two ever say different things, no
 * compiler and no plugin notices: the source simply stops recognising the question and falls back to whatever
 * it does with one it does not know - for the Ranger Hive service, an access type no policy grants. So the
 * agreement is asserted here, where both are visible, rather than left to be discovered in production.
 */
public class AccessRequirementVocabularyTest {

    private static final Map<AccessRequirement, PrivPredicate> VOCABULARY =
            ImmutableMap.<AccessRequirement, PrivPredicate>builder()
                    .put(AccessRequirements.VISIBILITY, PrivPredicate.SHOW)
                    .put(AccessRequirements.SELECT, PrivPredicate.SELECT)
                    .put(AccessRequirements.LOAD, PrivPredicate.LOAD)
                    .put(AccessRequirements.ALTER, PrivPredicate.ALTER)
                    .put(AccessRequirements.CREATE, PrivPredicate.CREATE)
                    .put(AccessRequirements.DROP, PrivPredicate.DROP)
                    .put(AccessRequirements.ADMINISTRATION, PrivPredicate.ADMIN)
                    .put(AccessRequirements.ANY_PRIVILEGE, PrivPredicate.ALL)
                    .build();

    @Test
    public void testEachNamedRequirementAsksWhatItsPredicateAsks() {
        VOCABULARY.forEach((requirement, predicate) -> Assertions.assertEquals(requirement,
                AccessTranslation.requirementOf(predicate),
                "the actions " + predicate + " names have changed; " + requirement
                        + " has to change with them or plugins stop recognising the question"));
    }

    /**
     * And back: the engine still compares the predicate it gets against its own constants by identity, so a
     * named requirement has to translate to that very constant and not to an equal copy of it.
     */
    @Test
    public void testEachNamedRequirementTranslatesBackToItsOwnPredicate() {
        VOCABULARY.forEach((requirement, predicate) -> Assertions.assertSame(predicate,
                AccessTranslation.privPredicateOf(requirement)));
    }
}
