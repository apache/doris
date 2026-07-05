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

package org.apache.doris.datasource;

import org.apache.doris.catalog.info.BranchOptions;
import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.catalog.info.TagOptions;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.TagChange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Unit tests for {@link ConnectorBranchTagConverter}: every nereids info/option field must land on the right
 * neutral carrier field (Rule 9), incl. the legacy {@code BranchOptions} naming (retain -> maxSnapshotAge,
 * numSnapshots -> minSnapshotsToKeep, retention -> maxRefAge), and absent options must become {@code null}.
 */
public class ConnectorBranchTagConverterTest {

    @Test
    public void testBranchFullOptions() {
        BranchChange b = ConnectorBranchTagConverter.toBranchChange(
                new CreateOrReplaceBranchInfo("b1", true, false, true,
                        new BranchOptions(Optional.of(42L), Optional.of(86400000L),
                                Optional.of(5), Optional.of(172800000L))));
        Assertions.assertEquals("b1", b.getName());
        Assertions.assertTrue(b.isCreate());
        Assertions.assertFalse(b.isReplace());
        Assertions.assertTrue(b.isIfNotExists());
        Assertions.assertEquals(42L, b.getSnapshotId().longValue());
        Assertions.assertEquals(86400000L, b.getMaxSnapshotAgeMs().longValue());
        Assertions.assertEquals(5, b.getMinSnapshotsToKeep().intValue());
        Assertions.assertEquals(172800000L, b.getMaxRefAgeMs().longValue());
    }

    @Test
    public void testBranchEmptyOptionsBecomeNull() {
        BranchChange b = ConnectorBranchTagConverter.toBranchChange(
                new CreateOrReplaceBranchInfo("b1", false, true, false, BranchOptions.EMPTY));
        Assertions.assertFalse(b.isCreate());
        Assertions.assertTrue(b.isReplace());
        Assertions.assertFalse(b.isIfNotExists());
        Assertions.assertNull(b.getSnapshotId());
        Assertions.assertNull(b.getMaxSnapshotAgeMs());
        Assertions.assertNull(b.getMinSnapshotsToKeep());
        Assertions.assertNull(b.getMaxRefAgeMs());
    }

    @Test
    public void testTagFullOptions() {
        TagChange t = ConnectorBranchTagConverter.toTagChange(
                new CreateOrReplaceTagInfo("v1", true, false, true,
                        new TagOptions(Optional.of(9L), Optional.of(99000L))));
        Assertions.assertEquals("v1", t.getName());
        Assertions.assertTrue(t.isCreate());
        Assertions.assertFalse(t.isReplace());
        Assertions.assertTrue(t.isIfNotExists());
        Assertions.assertEquals(9L, t.getSnapshotId().longValue());
        Assertions.assertEquals(99000L, t.getMaxRefAgeMs().longValue());
    }

    @Test
    public void testTagEmptyOptionsBecomeNull() {
        TagChange t = ConnectorBranchTagConverter.toTagChange(
                new CreateOrReplaceTagInfo("v1", true, false, false, TagOptions.EMPTY));
        Assertions.assertNull(t.getSnapshotId());
        Assertions.assertNull(t.getMaxRefAgeMs());
    }

    @Test
    public void testDropBranch() {
        DropRefChange d = ConnectorBranchTagConverter.toDropRefChange(new DropBranchInfo("b1", true));
        Assertions.assertEquals("b1", d.getName());
        Assertions.assertTrue(d.isIfExists());
    }

    @Test
    public void testDropTag() {
        DropRefChange d = ConnectorBranchTagConverter.toDropRefChange(new DropTagInfo("v1", false));
        Assertions.assertEquals("v1", d.getName());
        Assertions.assertFalse(d.isIfExists());
    }
}
