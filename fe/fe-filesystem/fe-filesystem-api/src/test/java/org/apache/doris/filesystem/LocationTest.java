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

package org.apache.doris.filesystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for the utility methods added to {@link Location} in the L4 fix.
 */
class LocationTest {

    // --- fileName() ---

    @Test
    void testFileNameForRegularFile() {
        Assertions.assertEquals("file.parquet", Location.of("s3://bucket/dir/file.parquet").fileName());
    }

    @Test
    void testFileNameForDirectory() {
        Assertions.assertEquals("dir", Location.of("s3://bucket/dir").fileName());
    }

    @Test
    void testFileNameForTrailingSlash() {
        // trailing slash indicates a directory marker → empty string
        Assertions.assertEquals("", Location.of("s3://bucket/dir/").fileName());
    }

    @Test
    void testFileNameForBucketRoot() {
        // "s3://bucket" → last segment after the final '/' (part of "://") is "bucket"
        Assertions.assertEquals("bucket", Location.of("s3://bucket").fileName());
    }

    // --- parent() ---

    @Test
    void testParentOfFile() {
        Assertions.assertEquals(Location.of("s3://bucket/dir"),
                Location.of("s3://bucket/dir/file.txt").parent());
    }

    @Test
    void testParentOfDirectory() {
        Assertions.assertEquals(Location.of("s3://bucket"),
                Location.of("s3://bucket/dir").parent());
    }

    @Test
    void testParentOfBucketRootIsNull() {
        Assertions.assertNull(Location.of("s3://bucket").parent());
    }

    @Test
    void testParentStripsTrailingSlash() {
        // "s3://bucket/dir/" is treated as "s3://bucket/dir" before computing parent
        Assertions.assertEquals(Location.of("s3://bucket"),
                Location.of("s3://bucket/dir/").parent());
    }

    // --- resolve() ---

    @Test
    void testResolveChildPath() {
        Assertions.assertEquals(Location.of("s3://bucket/dir/file.txt"),
                Location.of("s3://bucket/dir").resolve("file.txt"));
    }

    @Test
    void testResolveNestedChild() {
        Assertions.assertEquals(Location.of("s3://bucket/dir/sub/file.txt"),
                Location.of("s3://bucket/dir").resolve("sub/file.txt"));
    }

    @Test
    void testResolveStripsLeadingSlashFromChild() {
        Assertions.assertEquals(Location.of("s3://bucket/dir/file.txt"),
                Location.of("s3://bucket/dir").resolve("/file.txt"));
    }

    @Test
    void testResolveEmptyChildReturnsSelf() {
        Location loc = Location.of("s3://bucket/dir");
        Assertions.assertEquals(loc, loc.resolve(""));
    }

    @Test
    void testResolveWithTrailingSlashBase() {
        Assertions.assertEquals(Location.of("s3://bucket/dir/file.txt"),
                Location.of("s3://bucket/dir/").resolve("file.txt"));
    }

    // --- startsWith() ---

    @Test
    void testStartsWithExactMatch() {
        Location loc = Location.of("s3://bucket/dir");
        Assertions.assertTrue(loc.startsWith(loc));
    }

    @Test
    void testStartsWithDescendant() {
        Assertions.assertTrue(Location.of("s3://bucket/dir/file.txt")
                .startsWith(Location.of("s3://bucket/dir")));
    }

    @Test
    void testStartsWithDoesNotMatchStringPrefix() {
        // "s3://bucket/directory" must NOT match prefix "s3://bucket/dir"
        Assertions.assertFalse(Location.of("s3://bucket/directory")
                .startsWith(Location.of("s3://bucket/dir")));
    }

    @Test
    void testStartsWithDifferentBranch() {
        Assertions.assertFalse(Location.of("s3://bucket/other")
                .startsWith(Location.of("s3://bucket/dir")));
    }

    @Test
    void testStartsWithPrefixHasTrailingSlash() {
        // prefix already ends with "/" — should still work correctly
        Assertions.assertTrue(Location.of("s3://bucket/dir/file.txt")
                .startsWith(Location.of("s3://bucket/dir/")));
    }
}
