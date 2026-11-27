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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class MTMVSnapshotIdSnapshotTest {
    @Test
    public void partitionVersionIncludesThePinnedTableSnapshot() {
        MTMVSnapshotIdSnapshot before = new MTMVSnapshotIdSnapshot(10, 20);
        MTMVSnapshotIdSnapshot samePin = new MTMVSnapshotIdSnapshot(10, 20);
        Assertions.assertEquals(before, samePin);
        Assertions.assertEquals(before.hashCode(), samePin.hashCode());
        Assertions.assertNotEquals(before, new MTMVSnapshotIdSnapshot(10, 30));
        Assertions.assertNotEquals(before, new MTMVSnapshotIdSnapshot(11, 20));
        Assertions.assertEquals(10, before.getSnapshotVersion());
    }

    @Test
    public void legacyVersionsCannotMatchScopedVersionsAfterRollback() {
        // A deleted-file snapshot can retain the earlier live-entry id. Rolling back to that id
        // must not make the legacy MV contents comparable to the newly scoped version.
        MTMVSnapshotIdSnapshot legacy = new MTMVSnapshotIdSnapshot(10);
        MTMVSnapshotIdSnapshot scoped = new MTMVSnapshotIdSnapshot(10, 10);
        Assertions.assertNotEquals(legacy, scoped);
        Assertions.assertNotEquals(scoped, legacy);
        Assertions.assertEquals(legacy, new MTMVSnapshotIdSnapshot(10));
        Assertions.assertNotEquals(legacy, new MTMVSnapshotIdSnapshot(11));
    }

    @Test
    public void snapshotSerializationPreservesTheTableSnapshotAndLegacyReads() {
        MTMVSnapshotIf scoped = new MTMVSnapshotIdSnapshot(10, 20);
        String json = GsonUtils.GSON.toJson(scoped, MTMVSnapshotIf.class);
        MTMVSnapshotIf restored = GsonUtils.GSON.fromJson(json, MTMVSnapshotIf.class);
        Assertions.assertEquals(scoped, restored);
        Assertions.assertNotEquals(new MTMVSnapshotIdSnapshot(10, 30), restored);

        MTMVSnapshotIf legacy = GsonUtils.GSON.fromJson(
                "{\"clazz\":\"MTMVSnapshotIdSnapshot\",\"s\":10}", MTMVSnapshotIf.class);
        Assertions.assertEquals(new MTMVSnapshotIdSnapshot(10), legacy);
        Assertions.assertNotEquals(scoped, legacy);
    }

    @Test
    public void persistedRefreshVersionsRejectDataCommitsAndLegacySnapshots() {
        BaseTableInfo base = new BaseTableInfo(new TableNameInfo("catalog", "db", "table"));
        MTMVRefreshPartitionSnapshot partition = new MTMVRefreshPartitionSnapshot();
        partition.getPctSnapshot(base).put("p1", new MTMVSnapshotIdSnapshot(10, 20));
        partition.getPctSnapshot(base).put("legacy", new MTMVSnapshotIdSnapshot(10));
        MTMVRefreshSnapshot refresh = new MTMVRefreshSnapshot();
        refresh.updateSnapshots(Collections.singletonMap("mv_p1", partition), Collections.singleton("mv_p1"));

        MTMVRefreshSnapshot restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(refresh), MTMVRefreshSnapshot.class);
        Assertions.assertTrue(restored.equalsWithPct("mv_p1", "p1", new MTMVSnapshotIdSnapshot(10, 20), base));
        Assertions.assertFalse(restored.equalsWithPct("mv_p1", "p1", new MTMVSnapshotIdSnapshot(10, 30), base));
        Assertions.assertFalse(restored.equalsWithPct("mv_p1", "legacy", new MTMVSnapshotIdSnapshot(10, 10), base));
    }
}
