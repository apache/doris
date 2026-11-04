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

package org.apache.doris.planner;

import org.apache.doris.catalog.HashDistributionInfo.HashType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

public class StorageHashLayoutTest {
    @Test
    public void testMergeRetainsEvidenceRegardlessOfGrouping() {
        HashType[] inputs = {null, HashType.CRC32, HashType.IDENTITY};
        // Compare both tree groupings to a flat-input oracle. In particular, a NONE input
        // must not erase an algorithm that conflicts with an input merged farther up the tree.
        for (HashType first : inputs) {
            for (HashType second : inputs) {
                for (HashType third : inputs) {
                    EnumSet<HashType> known = EnumSet.noneOf(HashType.class);
                    boolean hasNone = false;
                    for (HashType input : new HashType[] {first, second, third}) {
                        if (input == null) {
                            hasNone = true;
                        } else {
                            known.add(input);
                        }
                    }
                    StorageHashLayout.State expected = known.isEmpty() ? StorageHashLayout.State.NONE
                            : known.size() > 1 ? StorageHashLayout.State.CONFLICT
                            : hasNone ? StorageHashLayout.State.UNKNOWN : StorageHashLayout.State.KNOWN;
                    StorageHashLayout left = layout(first).merge(layout(second)).merge(layout(third));
                    StorageHashLayout right = layout(first).merge(layout(second).merge(layout(third)));
                    for (StorageHashLayout actual : new StorageHashLayout[] {left, right}) {
                        Assertions.assertEquals(expected, actual.getState());
                        if (expected == StorageHashLayout.State.KNOWN) {
                            Assertions.assertEquals(known.iterator().next(), actual.getHashType());
                        } else {
                            Assertions.assertThrows(IllegalStateException.class, actual::getHashType);
                        }
                        if (expected == StorageHashLayout.State.CONFLICT) {
                            Assertions.assertThrows(IllegalStateException.class,
                                    () -> actual.checkNoConflict("test fragment"));
                        } else {
                            Assertions.assertSame(actual, actual.checkNoConflict("test fragment"));
                        }
                    }
                }
            }
        }
    }

    private static StorageHashLayout layout(HashType hashType) {
        return hashType == null ? StorageHashLayout.NONE : StorageHashLayout.known(hashType);
    }
}
