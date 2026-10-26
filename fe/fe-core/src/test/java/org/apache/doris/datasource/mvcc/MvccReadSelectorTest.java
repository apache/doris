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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class MvccReadSelectorTest {
    @Test
    public void historicalSelectorIsUnaffectedByParserOrGetterMutations() {
        TableSnapshot original = TableSnapshot.versionOf("100");
        MvccReadSelector selector = MvccReadSelector.of(Optional.of(original), Optional.empty());
        original.setValue("200");
        selector.getTableSnapshot().orElseThrow().setValue("300");
        Assertions.assertEquals("100", selector.getTableSnapshot().orElseThrow().getValue());
        Assertions.assertEquals(MvccReadSelector.of(Optional.of(TableSnapshot.versionOf("100")), Optional.empty()),
                selector);
        Assertions.assertNotEquals(MvccReadSelector.of(Optional.of(TableSnapshot.timeOf("100")), Optional.empty()),
                selector);
    }

    @Test
    public void dynamicParametersAreResolvedAgainForEachUse() {
        TableScanParams original = new TableScanParams("options", Map.of("branch", "dev"), Collections.emptyList());
        original.reuseResolvedMapParams(Map.of("snapshot-id", "100"));
        MvccReadSelector selector = MvccReadSelector.of(Optional.empty(), Optional.of(original));
        TableScanParams first = selector.getScanParams().orElseThrow();
        Assertions.assertTrue(first.getResolvedMapParams().isEmpty());
        first.reuseResolvedMapParams(Map.of("snapshot-id", "200"));
        Assertions.assertTrue(selector.getScanParams().orElseThrow().getResolvedMapParams().isEmpty());
        Assertions.assertEquals(Map.of("branch", "dev"), selector.getScanParams().orElseThrow().getMapParams());
    }

    @Test
    public void canonicalKeysPreserveMapEntryBoundariesAndIgnoreMapOrder() {
        Map<String, String> ordered = new LinkedHashMap<>();
        ordered.put("x", "1");
        ordered.put("y", "2");
        Map<String, String> reversed = new LinkedHashMap<>();
        reversed.put("y", "2");
        reversed.put("x", "1");
        MvccReadSelector a = MvccReadSelector.of(Optional.empty(), Optional.of(
                new TableScanParams("options", ordered, Collections.emptyList())));
        MvccReadSelector b = MvccReadSelector.of(Optional.empty(), Optional.of(
                new TableScanParams("options", reversed, Collections.emptyList())));
        MvccReadSelector collision = MvccReadSelector.of(Optional.empty(), Optional.of(
                new TableScanParams("options", Map.of("x", "1, y=2"), Collections.emptyList())));
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, collision);
        Assertions.assertSame(MvccReadSelector.LATEST, MvccReadSelector.of(Optional.empty(), Optional.empty()));
    }
}
