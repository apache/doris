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

package org.apache.doris.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

class LocationPathParsingTest {
    @Test
    void preserveFileSystemIdentityAndOpaquePath() {
        String[] locations = {
                "s3://bucket/a//b/../c/", "s3://bucket/a b+%2F?#中文😀.parquet",
                "hdfs://namenode:8020/p=1/file", "hdfs:///path", "hdfs:/path", "file:///tmp/file",
                "abfss://container@account.dfs.core.windows.net/dir/file",
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/lake/Files/file",
                "oss://bucket.oss-dls.aliyuncs.com/file", "s3://bucket", "s3://bucket/",
                "s3://bucket name/file", "s3://bucket+name/file", "s3://bucket%20name/file",
                "hdfs://[::1]:8020/file", "connector://virtual", "S3://bucket/file",
                "/local/file", "relative/file", "//host/file", "a/b://c/d", "a:b/c",
                "s3:///", "s3:////a", "s3://bucket/?#", "s3://bucket/\u0000file", "",
                "s3://", "1s3://bucket/file", "s+3://bucket/file", "://bucket/file"
        };
        for (String location : locations) {
            assertEquivalent(location);
        }
    }

    @Test
    void preserveIdentityForSpecialCharactersInAuthorityAndPath() {
        Random random = new Random(20261014);
        String alphabet = "abcXYZ019 /:+%?#@[]*._-中文é\u0000"
                + Character.MIN_HIGH_SURROGATE + Character.MIN_LOW_SURROGATE;
        for (int i = 0; i < 2000; i++) {
            StringBuilder location = new StringBuilder(i % 2 == 0 ? "s3://" : "hdfs://");
            for (int j = 0; j < 8; j++) {
                location.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            location.append('/');
            for (int j = 0; j < 40; j++) {
                location.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            assertEquivalent(location.toString());
        }
    }

    @Test
    void reuseFilesystemIdentityAcrossFilesWithoutChangingPathBytes() {
        LocationPath.Factory factory = new LocationPath.Factory();
        LocationPath first = factory.create("s3://bucket/a");
        LocationPath second = factory.create("s3://bucket/b //中文%2F+?#");
        Assertions.assertSame(first.getFsIdentifier(), second.getFsIdentifier());
        Assertions.assertEquals("s3://bucket/b //中文%2F+?#", second.getNormalizedLocation());
        Assertions.assertSame(second, factory.create(second.getNormalizedLocation()));
        for (String location : new String[] {"s3://bucket-other/a", "hdfs://bucket/a", "hdfs:///a",
                "hdfs:///b", "s3://bucket name/a", "s3://bucket name/b", "hdfs:/a", "hdfs:/b",
                "file:///tmp/a", "file:///tmp/b", "/local/a", "/local/b", "s3://bucket", "s3://bucket/a"}) {
            LocationPath expected = LocationPath.of(location);
            LocationPath actual = factory.create(location);
            Assertions.assertEquals(expected.getFsIdentifier(), actual.getFsIdentifier(), location);
            Assertions.assertEquals(expected.getSchema(), actual.getSchema(), location);
            Assertions.assertEquals(expected.getTFileTypeForBE(), actual.getTFileTypeForBE(), location);
            Assertions.assertEquals(location, actual.getNormalizedLocation());
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create("1s3://bucket/file"));
        Assertions.assertEquals("s3://bucket", factory.create("s3://bucket/after-error").getFsIdentifier());
    }

    private static void assertEquivalent(String location) {
        // The old full-path parser is the compatibility oracle: URL metacharacters in an object
        // key are literal bytes, and authority escaping (including spaces becoming '+') is unchanged.
        URI expected;
        try {
            expected = URI.create(URLEncoder.encode(location, StandardCharsets.UTF_8)
                    .replace("%2F", "/").replace("%3A", ":"));
        } catch (IllegalArgumentException e) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> LocationPath.of(location), location);
            return;
        }
        LocationPath actual = LocationPath.of(location);
        Assertions.assertEquals((expected.getScheme() == null ? "" : expected.getScheme()) + "://"
                + (expected.getAuthority() == null ? "" : expected.getAuthority()), actual.getFsIdentifier(), location);
        Assertions.assertEquals(location, actual.getNormalizedLocation());
        Assertions.assertEquals(location, actual.toStorageLocation().toString());
    }
}
