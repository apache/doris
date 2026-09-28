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

package org.apache.doris.nereids.trees.plans.commands.spm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * SHOW BASELINE PLANS LIKE operand semantics.
 *
 * Only an OMITTED LIKE is "no filter": an empty pattern operand (LIKE '') is a real
 * pattern that matches only empty values - treating it as absent admitted every baseline
 * although none of the searched SQL / status / source fields is empty.
 */
public class ShowBaselinePlansCommandTest {

    @Test
    public void testEmptyLikeIsARealPattern() throws Exception {
        Assertions.assertNull(ShowBaselinePlansCommand.buildLikeMatcher(null),
                "an omitted LIKE means no filter");
        var matcher = ShowBaselinePlansCommand.buildLikeMatcher("");
        Assertions.assertNotNull(matcher, "LIKE '' is a real pattern, not an omitted filter");
        Assertions.assertFalse(matcher.match("select 1"),
                "LIKE '' matches only empty values, so no baseline row passes it");
        Assertions.assertTrue(matcher.match(""),
                "the empty pattern matches the empty value");
    }
}
