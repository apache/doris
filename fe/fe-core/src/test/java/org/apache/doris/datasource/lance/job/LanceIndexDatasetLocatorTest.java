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

package org.apache.doris.datasource.lance.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for dataset-locator normalization v1, the fence-key identity for the
 * target dataset: trim, lowercase scheme, userinfo rejection (credential-bearing URLs
 * are never identity), rejection of locators with neither authority nor path,
 * trailing-slash stripping with a kept root, and the absolute-path requirement for
 * scheme-less locators.
 */
public class LanceIndexDatasetLocatorTest {

    @Test
    public void lowercasesSchemeOnly() {
        Assertions.assertEquals("s3://bucket/path", LanceIndexDatasetLocator.normalize("S3://bucket/path"));
        Assertions.assertEquals("file:///data/x", LanceIndexDatasetLocator.normalize("FILE:///data/x"));
        Assertions.assertEquals("hdfs://nn:8020/data", LanceIndexDatasetLocator.normalize("HDFS://nn:8020/data"));
    }

    @Test
    public void preservesAuthorityAndPathCase() {
        // Bucket and path components are case-sensitive on the supported providers.
        Assertions.assertEquals("s3://MyBucket/MyPath", LanceIndexDatasetLocator.normalize("s3://MyBucket/MyPath"));
    }

    @Test
    public void trimsSurroundingWhitespace() {
        Assertions.assertEquals("/data/x", LanceIndexDatasetLocator.normalize("  /data/x \t\n"));
        Assertions.assertEquals("s3://bucket/p", LanceIndexDatasetLocator.normalize(" s3://bucket/p "));
    }

    @Test
    public void stripsTrailingSlashesFromPath() {
        Assertions.assertEquals("s3://b/p", LanceIndexDatasetLocator.normalize("s3://b/p///"));
        Assertions.assertEquals("/data/lance", LanceIndexDatasetLocator.normalize("/data/lance/"));
    }

    @Test
    public void keepsSchemeLessRoot() {
        Assertions.assertEquals("/", LanceIndexDatasetLocator.normalize("/"));
        Assertions.assertEquals("/", LanceIndexDatasetLocator.normalize("//"));
        Assertions.assertEquals("/", LanceIndexDatasetLocator.normalize("///"));
    }

    @Test
    public void stripsTrailingSlashBehindAuthority() {
        Assertions.assertEquals("s3://bucket", LanceIndexDatasetLocator.normalize("s3://bucket/"));
        Assertions.assertEquals("s3://bucket", LanceIndexDatasetLocator.normalize("s3://bucket//"));
        Assertions.assertEquals("s3://bucket", LanceIndexDatasetLocator.normalize("s3://bucket"));
    }

    @Test
    public void acceptsFileUrlWithEmptyAuthority() {
        Assertions.assertEquals("file:///data/x", LanceIndexDatasetLocator.normalize("file:///data/x"));
    }

    @Test
    public void keepsAuthorityPortAndQueryFreePathVerbatim() {
        Assertions.assertEquals("hdfs://nn:8020/base/table.lance",
                LanceIndexDatasetLocator.normalize("hdfs://nn:8020/base/table.lance"));
    }

    @Test
    public void allowsAtSignInPathButNotInAuthority() {
        Assertions.assertEquals("s3://bucket/p@th", LanceIndexDatasetLocator.normalize("s3://bucket/p@th"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDatasetLocator.normalize("s3://user@bucket/path"));
    }

    @Test
    public void rejectsCredentialBearingUserinfo() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDatasetLocator.normalize("s3://user:secret@bucket/path"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDatasetLocator.normalize("https://user:secret@example.com/ds"));
    }

    @Test
    public void rejectsSchemeLessRelativePath() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDatasetLocator.normalize("data/x"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDatasetLocator.normalize("./x"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDatasetLocator.normalize("bucket/path"));
    }

    @Test
    public void rejectsNullEmptyAndBlank() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize(""));
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize("   "));
    }

    @Test
    public void rejectsEmptyScheme() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize("://path"));
    }

    @Test
    public void rejectsLocatorWithNeitherAuthorityNorPath() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize("s3://"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize("file://"));
        // Trailing-slash stripping still leaves nothing behind the scheme.
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexDatasetLocator.normalize("s3:///"));
        // An empty authority with a real path stays legal.
        Assertions.assertEquals("file:///x", LanceIndexDatasetLocator.normalize("file:///x"));
    }
}
