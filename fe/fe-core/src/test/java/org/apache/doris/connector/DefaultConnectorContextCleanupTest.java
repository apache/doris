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

package org.apache.doris.connector;

import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.MemoryFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests the engine-side empty-directory pruning that backs {@link ConnectorContext#cleanupEmptyManagedLocation}
 * (ported into {@link DefaultConnectorContext} from legacy {@code IcebergMetadataOps}). Uses the reusable
 * {@link MemoryFileSystem} fake — no live storage. Verifies the conservative contract: a directory is removed
 * only when it (transitively) contains no files; the table path descends the engine-format child dirs first.
 */
public class DefaultConnectorContextCleanupTest {

    @Test
    public void deletesFullyEmptyDirectory() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location dir = Location.of("hdfs://nn/wh/db/t");
        fs.mkdirs(dir);
        fs.mkdirs(dir.resolve("data"));

        Assertions.assertTrue(DefaultConnectorContext.deleteEmptyDirectory(fs, dir));
        Assertions.assertFalse(fs.exists(dir));
    }

    @Test
    public void keepsDirectoryThatContainsAFile() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location dir = Location.of("hdfs://nn/wh/db/t");
        fs.mkdirs(dir);
        Location file = dir.resolve("part-0");
        fs.put(file, new byte[] {1});

        // WHY (Rule 9/12): the cleanup must NEVER delete a directory still holding data — a broken abort
        // condition would silently destroy table files. MUTATION: flipping the `return false` on a
        // non-directory entry would make this red.
        Assertions.assertFalse(DefaultConnectorContext.deleteEmptyDirectory(fs, dir));
        Assertions.assertTrue(fs.exists(dir));
        Assertions.assertTrue(fs.exists(file));
    }

    @Test
    public void deletesEmptyTableLocationWithChildDirs() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location table = Location.of("hdfs://nn/wh/db/t");
        fs.mkdirs(table);
        fs.mkdirs(table.resolve("data"));
        fs.mkdirs(table.resolve("metadata"));

        Assertions.assertTrue(
                DefaultConnectorContext.deleteEmptyTableLocation(fs, table, Arrays.asList("data", "metadata")));
        Assertions.assertFalse(fs.exists(table));
    }

    @Test
    public void keepsTableLocationWhenAChildHoldsAFile() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location table = Location.of("hdfs://nn/wh/db/t");
        fs.mkdirs(table);
        fs.mkdirs(table.resolve("metadata"));
        fs.put(table.resolve("data").resolve("part-0"), new byte[] {1});

        Assertions.assertFalse(
                DefaultConnectorContext.deleteEmptyTableLocation(fs, table, Arrays.asList("data", "metadata")));
        Assertions.assertTrue(fs.exists(table));
    }

    @Test
    public void cleanupBlankLocationIsNoOp() {
        DefaultConnectorContext ctx = new DefaultConnectorContext("test", 1L);
        // No storage supplier wired + blank location: must be a silent no-op (never throws).
        Assertions.assertDoesNotThrow(() -> ctx.cleanupEmptyManagedLocation("", Arrays.asList("data", "metadata")));
        Assertions.assertDoesNotThrow(() -> ctx.cleanupEmptyManagedLocation(null, null));
    }

    @Test
    public void cleanupWithNoStoragePropertiesIsNoOp() {
        // Default ctor wires an empty storage supplier -> no FileSystem to build -> no-op, no throw.
        DefaultConnectorContext ctx = new DefaultConnectorContext("test", 1L);
        Assertions.assertDoesNotThrow(
                () -> ctx.cleanupEmptyManagedLocation("hdfs://nn/wh/db/t", Arrays.asList("data", "metadata")));
    }
}
