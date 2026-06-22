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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IcebergTableHandle}, including the T07 MVCC / time-travel pin carriers.
 */
public class IcebergTableHandleTest {

    @Test
    public void bareHandleHasNoPin() {
        IcebergTableHandle h = new IcebergTableHandle("db1", "t1");
        // WHY: a normal (latest) read must carry NO pin so the scan reads the current snapshot. MUTATION:
        // defaulting snapshotId to 0 (a valid id) -> hasSnapshotPin true -> red.
        Assertions.assertFalse(h.hasSnapshotPin());
        Assertions.assertEquals(-1L, h.getSnapshotId());
        Assertions.assertNull(h.getRef());
        Assertions.assertEquals(-1L, h.getSchemaId());
        Assertions.assertEquals("db1", h.getDbName());
        Assertions.assertEquals("t1", h.getTableName());
    }

    @Test
    public void withSnapshotPinsByIdAndCarriesSchemaId() {
        IcebergTableHandle pinned = new IcebergTableHandle("db1", "t1").withSnapshot(42L, null, 3L);
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals(42L, pinned.getSnapshotId());
        Assertions.assertNull(pinned.getRef());
        Assertions.assertEquals(3L, pinned.getSchemaId());
        // The coordinates survive the pin.
        Assertions.assertEquals("db1", pinned.getDbName());
        Assertions.assertEquals("t1", pinned.getTableName());
    }

    @Test
    public void withSnapshotPinsByRef() {
        IcebergTableHandle pinned = new IcebergTableHandle("db1", "t1").withSnapshot(7L, "b1", 2L);
        // WHY: a tag/branch read pins by REF (useRef), so a ref pin alone must count as a pin even if an id is
        // also present. MUTATION: hasSnapshotPin checking only snapshotId -> still true here, so also assert
        // ref-only below.
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals("b1", pinned.getRef());
    }

    @Test
    public void refOnlyPinCountsAsPin() {
        IcebergTableHandle pinned = new IcebergTableHandle("db1", "t1").withSnapshot(-1L, "tag1", 5L);
        // MUTATION: hasSnapshotPin returning snapshotId>=0 only -> false here -> red (a ref pin would be lost).
        Assertions.assertTrue(pinned.hasSnapshotPin());
        Assertions.assertEquals("tag1", pinned.getRef());
        Assertions.assertEquals(-1L, pinned.getSnapshotId());
    }

    @Test
    public void pinIsPartOfIdentity() {
        IcebergTableHandle bare = new IcebergTableHandle("db1", "t1");
        IcebergTableHandle pinned = bare.withSnapshot(42L, null, 3L);
        IcebergTableHandle samePin = new IcebergTableHandle("db1", "t1").withSnapshot(42L, null, 3L);
        // WHY: the pin is part of the handle identity (a query-begin handle and a time-travel handle for the
        // same table are different reads). MUTATION: equals/hashCode ignoring the pin -> bare.equals(pinned) -> red.
        Assertions.assertNotEquals(bare, pinned);
        Assertions.assertEquals(pinned, samePin);
        Assertions.assertEquals(pinned.hashCode(), samePin.hashCode());
        Assertions.assertEquals(bare, new IcebergTableHandle("db1", "t1"));
    }
}
