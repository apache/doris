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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.lance.LanceManifestPaths.Recorded;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

public class LanceManifestPathsTest {
    private static final String S3_URI = "s3://bucket/table.lance";
    /** Version 3 in the V2 naming scheme is u64::MAX - 3. */
    private static final String CANONICAL = "table.lance/_versions/18446744073709551612.manifest";

    private static Recorded check(String location, String branch, String recorded) {
        return LanceManifestPaths.check(location, Optional.ofNullable(branch), 3, recorded, "db.t");
    }

    /** The canonical path in either naming scheme, however many surrounding slashes. */
    @Test
    public void testCanonicalPaths() {
        Assertions.assertEquals(Recorded.CANONICAL, check(S3_URI, null, CANONICAL));
        Assertions.assertEquals(Recorded.CANONICAL, check(S3_URI, null, "/" + CANONICAL));
        Assertions.assertEquals(Recorded.CANONICAL, check(S3_URI, null, "table.lance/_versions/3.manifest"));
        Assertions.assertEquals(Recorded.CANONICAL,
                check(S3_URI, "dev", "table.lance/tree/dev/_versions/18446744073709551612.manifest"));
        Assertions.assertEquals(Recorded.CANONICAL,
                check(S3_URI, "feature/x", "table.lance/tree/feature/x/_versions/18446744073709551612.manifest"));
        // A table at the root of its bucket, and one whose location Java's URI cannot parse.
        Assertions.assertEquals(Recorded.CANONICAL, check("s3://bucket", null, "_versions/18446744073709551612.manifest"));
        Assertions.assertEquals(Recorded.CANONICAL,
                check("s3://bucket/sales data/t.lance", null, "sales data/t.lance/_versions/3.manifest"));
    }

    /** A staged manifest beside the canonical path, named as Lance names one. */
    @Test
    public void testStagedPaths() {
        Assertions.assertEquals(Recorded.STAGED, check(S3_URI, null, CANONICAL + "-2f6a1c0e"));
        Assertions.assertEquals(Recorded.STAGED, check(S3_URI, null, "table.lance/_versions/3.manifest-2f6a1c0e"));
    }

    /**
     * Any other path names a manifest Doris would not read: another name, another version,
     * another directory, a branch version at main's path and the other way round, a staged
     * manifest in a subdirectory, or a path whose trailing slash Lance drops before reading it.
     */
    @Test
    public void testOtherPathsAreRejected() {
        for (String elsewhere : Arrays.asList("table.lance/_versions/custom.manifest",
                "table.lance/_versions/custom.manifest/",
                "other.lance/_versions/18446744073709551612.manifest",
                "other.lance/_versions/18446744073709551612.manifest-2f6a1c0e",
                "table.lance/_versions/18446744073709551613.manifest",
                "table.lance/_versions/18446744073709551612.manifest-x/y",
                "table.lance/_versions/foo.json",
                "table.lance/tree/dev/_versions/18446744073709551612.manifest",
                "")) {
            IllegalStateException rejected = Assertions.assertThrows(IllegalStateException.class,
                    () -> check(S3_URI, null, elsewhere), elsewhere);
            Assertions.assertTrue(rejected.getMessage().contains("'" + CANONICAL + "'"), rejected.getMessage());
            Assertions.assertTrue(rejected.getMessage().contains("retry"), rejected.getMessage());
        }
        Assertions.assertThrows(IllegalStateException.class, () -> check(S3_URI, "dev", CANONICAL));
        Assertions.assertThrows(IllegalStateException.class, () -> check(S3_URI, null, null));
    }

    /** Lance writes ASCII digits; the FE's locale must not change the name it expects. */
    @Test
    public void testCanonicalNameDoesNotDependOnTheLocale() {
        Locale previous = Locale.getDefault(Locale.Category.FORMAT);
        Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-SA-u-nu-arab"));
        try {
            Assertions.assertEquals(Recorded.CANONICAL, check(S3_URI, null, CANONICAL));
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, previous);
        }
    }

    @Test
    public void testNormalizedLocationPreservesAuthorityAndNormalizesPath() {
        Assertions.assertEquals("s3://bucket/warehouse/items.lance",
                LanceManifestPaths.normalizedLocation(
                        "s3://bucket/warehouse//./staged/../items.lance/?X-Amz-Signature=example#fragment"));
        Assertions.assertEquals("warehouse/items.lance",
                LanceManifestPaths.normalizedLocation("/warehouse//./staged/../items.lance/"));
        Assertions.assertNotEquals(
                LanceManifestPaths.normalizedLocation("s3://bucket-a/warehouse/items.lance"),
                LanceManifestPaths.normalizedLocation("s3://bucket-b/warehouse/items.lance"));
    }

    /**
     * The path lance-io addresses a location by: after the bucket, percent-decoded for a URL, as
     * is without a scheme.
     */
    @Test
    public void testObjectStorePathFollowsLance() {
        Assertions.assertEquals("sales data/t.lance",
                LanceManifestPaths.objectStorePath("s3://bucket/sales data/t.lance"));
        Assertions.assertEquals("sales data/t.lance",
                LanceManifestPaths.objectStorePath("s3://bucket/sales%20data/t.lance"));
        Assertions.assertEquals("a|b/t.lance", LanceManifestPaths.objectStorePath("s3://bucket/a|b/t.lance/"));
        Assertions.assertEquals("", LanceManifestPaths.objectStorePath("s3://bucket"));
        Assertions.assertEquals("", LanceManifestPaths.objectStorePath("s3://bucket/"));
        Assertions.assertEquals("t.lance",
                LanceManifestPaths.objectStorePath("s3+ddb://bucket/t.lance?ddbTableName=x"));
        Assertions.assertEquals("p/t.lance",
                LanceManifestPaths.objectStorePath("abfss://container@account.dfs.core.windows.net/p/t.lance"));
        Assertions.assertEquals("tmp/a b/t.lance", LanceManifestPaths.objectStorePath("file:///tmp/a%20b/t.lance"));
        Assertions.assertEquals("tmp/t.lance", LanceManifestPaths.objectStorePath("file:/tmp/t.lance"));
        Assertions.assertEquals("tmp/a%20b/t.lance", LanceManifestPaths.objectStorePath("/tmp/a%20b/t.lance"));
        Assertions.assertEquals("warehouse/items.lance",
                LanceManifestPaths.objectStorePath("s3://bucket/warehouse//./staged/../items.lance/"));
        Assertions.assertEquals("warehouse/items.lance",
                LanceManifestPaths.objectStorePath("/warehouse//./staged/../items.lance/"));
        Assertions.assertEquals("t%zz.lance", LanceManifestPaths.objectStorePath("s3://bucket/t%zz.lance"));
    }
}
