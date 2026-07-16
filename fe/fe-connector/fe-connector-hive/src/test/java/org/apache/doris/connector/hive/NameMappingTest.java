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

package org.apache.doris.connector.hive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link NameMapping}. The connector write transaction keys its per-table and
 * per-partition action maps by {@code NameMapping}, so value-based {@code equals}/{@code hashCode}
 * is load-bearing: a broken key would split one table's actions across map buckets and drop writes.
 * These tests pin that contract, not just field storage.
 */
public class NameMappingTest {

    @Test
    public void equalValuesAreEqualAndHashAlike() {
        NameMapping a = new NameMapping(1L, "localDb", "localTbl", "remoteDb", "remoteTbl");
        NameMapping b = new NameMapping(1L, "localDb", "localTbl", "remoteDb", "remoteTbl");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void anyFieldDifferenceBreaksEquality() {
        NameMapping base = new NameMapping(1L, "ld", "lt", "rd", "rt");
        Assertions.assertNotEquals(base, new NameMapping(2L, "ld", "lt", "rd", "rt"));
        Assertions.assertNotEquals(base, new NameMapping(1L, "LD", "lt", "rd", "rt"));
        Assertions.assertNotEquals(base, new NameMapping(1L, "ld", "LT", "rd", "rt"));
        Assertions.assertNotEquals(base, new NameMapping(1L, "ld", "lt", "RD", "rt"));
        Assertions.assertNotEquals(base, new NameMapping(1L, "ld", "lt", "rd", "RT"));
    }

    @Test
    public void usableAsMapKeyByValue() {
        Map<NameMapping, String> actions = new HashMap<>();
        actions.put(new NameMapping(7L, "ld", "lt", "rd", "rt"), "action");
        // A distinct instance with identical values must resolve to the same bucket.
        Assertions.assertEquals("action", actions.get(new NameMapping(7L, "ld", "lt", "rd", "rt")));
        Assertions.assertNull(actions.get(new NameMapping(7L, "ld", "lt", "rd", "other")));
    }

    @Test
    public void fullNamesJoinDbAndTable() {
        NameMapping m = new NameMapping(1L, "localDb", "localTbl", "remoteDb", "remoteTbl");
        Assertions.assertEquals("localDb.localTbl", m.getFullLocalName());
        Assertions.assertEquals("remoteDb.remoteTbl", m.getFullRemoteName());
    }
}
