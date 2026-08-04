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

import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TS3MPUPendingUpload;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link HiveWriteUtils}. These pin the pure write-path helpers that the connector
 * write transaction relies on: staging-directory containment/equality checks and the partition
 * update accumulator merge. A behavior change in any of these silently corrupts commit accounting
 * or staging cleanup, so the tests encode the exact contract, not just smoke coverage.
 */
public class HiveWriteUtilsTest {

    private static THivePartitionUpdate update(String name, long fileSize, long rowCount, String... fileNames) {
        return new THivePartitionUpdate()
                .setName(name)
                .setFileSize(fileSize)
                .setRowCount(rowCount)
                .setFileNames(new ArrayList<>(Arrays.asList(fileNames)));
    }

    @Test
    public void mergePartitionsSumsAndConcatsSameName() {
        THivePartitionUpdate first = update("p=1", 100L, 10L, "f1", "f2");
        THivePartitionUpdate second = update("p=1", 55L, 5L, "f3");

        List<THivePartitionUpdate> merged = HiveWriteUtils.mergePartitions(Arrays.asList(first, second));

        Assertions.assertEquals(1, merged.size());
        THivePartitionUpdate m = merged.get(0);
        Assertions.assertEquals("p=1", m.getName());
        Assertions.assertEquals(155L, m.getFileSize(), "file sizes must be summed");
        Assertions.assertEquals(15L, m.getRowCount(), "row counts must be summed");
        Assertions.assertEquals(Arrays.asList("f1", "f2", "f3"), m.getFileNames(), "file names must be concatenated");
    }

    @Test
    public void mergePartitionsKeepsDistinctNames() {
        List<THivePartitionUpdate> merged = HiveWriteUtils.mergePartitions(Arrays.asList(
                update("p=1", 10L, 1L, "a"),
                update("p=2", 20L, 2L, "b"),
                update("p=1", 30L, 3L, "c")));

        Map<String, THivePartitionUpdate> byName = merged.stream()
                .collect(Collectors.toMap(THivePartitionUpdate::getName, u -> u));
        Assertions.assertEquals(2, byName.size());
        Assertions.assertEquals(40L, byName.get("p=1").getFileSize());
        Assertions.assertEquals(4L, byName.get("p=1").getRowCount());
        Assertions.assertEquals(20L, byName.get("p=2").getFileSize());
    }

    @Test
    public void mergePartitionsConcatsPendingUploads() {
        THivePartitionUpdate first = update("p=1", 0L, 0L, "a");
        first.setS3MpuPendingUploads(new ArrayList<>(Arrays.asList(new TS3MPUPendingUpload())));
        THivePartitionUpdate second = update("p=1", 0L, 0L, "b");
        second.setS3MpuPendingUploads(new ArrayList<>(Arrays.asList(
                new TS3MPUPendingUpload(), new TS3MPUPendingUpload())));

        List<THivePartitionUpdate> merged = HiveWriteUtils.mergePartitions(Arrays.asList(first, second));

        Assertions.assertEquals(1, merged.size());
        Assertions.assertEquals(3, merged.get(0).getS3MpuPendingUploads().size(),
                "pending MPU uploads across BE reports for one partition must be aggregated");
    }

    @Test
    public void mergePartitionsToleratesNullPendingUploads() {
        // Neither update carries pending uploads; the merge must not NPE and just sums files.
        List<THivePartitionUpdate> merged = HiveWriteUtils.mergePartitions(Arrays.asList(
                update("p=1", 1L, 1L, "a"),
                update("p=1", 2L, 2L, "b")));
        Assertions.assertEquals(1, merged.size());
        Assertions.assertEquals(3L, merged.get(0).getFileSize());
    }

    @Test
    public void isSubDirectoryHappyPath() {
        Assertions.assertTrue(HiveWriteUtils.isSubDirectory("/warehouse/table", "/warehouse/table/p=1"));
        Assertions.assertTrue(HiveWriteUtils.isSubDirectory(
                "/warehouse/table", "/warehouse/table/.doris_staging/user/uuid"));
    }

    @Test
    public void isSubDirectoryRejectsEqualAndSiblingAndPrefix() {
        // Equal paths are not a strict subdirectory.
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory("/warehouse/table", "/warehouse/table"));
        // A shared textual prefix without a path boundary is not containment.
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory("/warehouse/table", "/warehouse/table2"));
        // Unrelated path.
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory("/warehouse/table", "/other/x"));
    }

    @Test
    public void isSubDirectoryRejectsDifferentFileSystem() {
        // Same path suffix but different authority (namenode) => different file system.
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory("hdfs://nn1/w/t", "hdfs://nn2/w/t/p=1"));
        // Different scheme.
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory("s3://bucket/w/t", "hdfs://nn/w/t/p=1"));
    }

    @Test
    public void isSubDirectoryNullSafe() {
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory(null, "/a/b"));
        Assertions.assertFalse(HiveWriteUtils.isSubDirectory("/a", null));
    }

    @Test
    public void getImmediateChildPathReturnsFirstLevel() {
        Assertions.assertEquals("/warehouse/table/.doris_staging",
                HiveWriteUtils.getImmediateChildPath(
                        "/warehouse/table", "/warehouse/table/.doris_staging/user/uuid"));
        // Direct child returns the child itself.
        Assertions.assertEquals("/warehouse/table/p=1",
                HiveWriteUtils.getImmediateChildPath("/warehouse/table", "/warehouse/table/p=1"));
    }

    @Test
    public void getImmediateChildPathReturnsNullWhenNotChild() {
        Assertions.assertNull(HiveWriteUtils.getImmediateChildPath("/warehouse/table", "/other/x"));
        Assertions.assertNull(HiveWriteUtils.getImmediateChildPath("/warehouse/table", "/warehouse/table"));
    }

    @Test
    public void pathsEqualNormalizesTrailingSlashAndFileSystem() {
        Assertions.assertTrue(HiveWriteUtils.pathsEqual("/a/b", "/a/b/"));
        Assertions.assertTrue(HiveWriteUtils.pathsEqual("hdfs://nn/a/b", "hdfs://nn/a/b"));
        Assertions.assertFalse(HiveWriteUtils.pathsEqual("/a/b", "/a/c"));
        // Different namenode => not equal even with identical path.
        Assertions.assertFalse(HiveWriteUtils.pathsEqual("hdfs://nn1/a/b", "hdfs://nn2/a/b"));
    }

    @Test
    public void pathsEqualNullSafe() {
        Assertions.assertTrue(HiveWriteUtils.pathsEqual(null, null));
        Assertions.assertFalse(HiveWriteUtils.pathsEqual(null, "/a"));
        Assertions.assertFalse(HiveWriteUtils.pathsEqual("/a", null));
    }
}
