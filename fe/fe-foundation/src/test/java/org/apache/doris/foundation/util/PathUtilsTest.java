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

package org.apache.doris.foundation.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PathUtilsTest {

    @Test
    public void testEqualsIgnoreSchemeSameHostAndPath() {
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://my-bucket/data/file.txt",
                "cos://my-bucket/data/file.txt"
        ));

        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "oss://bucket/path/",
                "obs://bucket/path"
        ));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/abc/path/",
                "obs://bucket/path"
        ));

        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "hdfs://namenode/user/hadoop",
                "file:///user/hadoop"
        ));
    }

    @Test
    public void testEqualsIgnoreSchemeDifferentHost() {
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket-a/data/file.txt",
                "cos://bucket-b/data/file.txt"
        ));

        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "hdfs://namenode1/path",
                "hdfs://namenode2/path"
        ));
    }

    @Test
    public void testEqualsIgnoreSchemeTrailingSlash() {
        // Trailing slashes are insignificant: a location with or without a trailing slash
        // refers to the same place. This now holds consistently for same-scheme comparisons too.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "oss://bucket/data/",
                "oss://bucket/data"
        ));

        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "hdfs://namenode/user/hadoop/",
                "hdfs://namenode/user/hadoop"
        ));
    }

    @Test
    public void testTrailingSlashConsistentAcrossSchemes() {
        // The result of a trailing-slash-only difference must NOT depend on the other URI's scheme.
        // same scheme:
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path/", "s3://bucket/path"));
        // cross scheme (one is s3):
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path/", "cos://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemePathCaseSensitive() {
        // Object-storage keys are case-sensitive, so path case must matter -- consistently
        // for both same-scheme and cross-scheme comparisons.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/Path", "s3://bucket/path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/Path", "cos://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeAuthorityCaseSensitive() {
        // The authority (bucket name) is also compared case-sensitively.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://BUCKET/path", "s3://bucket/path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://BUCKET/path", "cos://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeBucketRootTrailingSlash() {
        // Bucket root with and without a trailing slash refer to the same location.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/", "s3://bucket"));
        // Cross-scheme bucket root: "s3://bucket/" (path="/") vs "cos://bucket" (path="").
        // Both normalize to "" so they should compare equal. This exercises the oneIsS3 branch
        // with a root slash being normalized to match the empty path -- the production case where
        // the HMSTransaction caller compares bucket roots across schemes.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/", "cos://bucket"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "cos://bucket/", "s3://bucket"));
    }

    @Test
    public void testEqualsIgnoreSchemeMultipleTrailingSlashes() {
        // The TRAILING_SLASHES pattern (/+$) strips any number of trailing slashes, not just one.
        // Guards against the pattern being narrowed to a single-slash strip.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path//", "s3://bucket/path"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path///", "s3://bucket/path/"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket///", "s3://bucket"));
    }

    @Test
    public void testEqualsIgnoreSchemeDifferentSchemesNeitherS3TrailingSlashOnly() {
        // Two non-s3 schemes whose authority+path match modulo a trailing slash must still be
        // unequal: the !sameScheme && !oneIsS3 branch short-circuits to false before normalize()
        // is ever consulted, independent of path content.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "oss://bucket/path/", "obs://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeOpaqueUri() {
        // Opaque URIs ("s3:..." with no "//") must NOT all collapse to equal: they have null
        // authority and path, so without an opaque guard they would always compare equal. They fall
        // back to exact string comparison.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3:arbitraryA", "s3:arbitraryB"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3:bucket1/key1", "s3:bucket2/key2"));
        // Identical opaque URIs are equal via the exact-string fallback.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3:bucket/key", "s3:bucket/key"));
        // Mixed opaque vs. hierarchical: the isOpaque() guard fires when EITHER side is opaque, so
        // a malformed s3-without-"//" string compared against a valid s3:// URI falls back to exact
        // string comparison and is NOT equal. Asserted in both argument orders because the guard is
        // an OR over both arguments (uri1.isOpaque() true / uri2.isOpaque() false, and vice versa).
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3:bucket/key", "s3://bucket/key"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/key", "s3:bucket/key"));
        // A valid hierarchical URI compared against itself still takes the fast path (equal).
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/key", "s3://bucket/key"));
    }

    @Test
    public void testEqualsIgnoreSchemeEncodedSlashDistinct() {
        // In S3 the key "a%2Fb" (literal slash in the key) and "a/b" (path separator) are different
        // objects. Using the raw path keeps them distinct.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/a%2Fb", "s3://bucket/a/b"));
    }

    @Test
    public void testEqualsIgnoreSchemeEncodedTrailingSlashNotStripped() {
        // A percent-encoded trailing slash (%2F) is a literal character in the S3 key and
        // must NOT be treated as an insignificant trailing slash. normalize() checks
        // the last raw char == '/' and leaves '%2F' (last char 'F') untouched.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path%2F", "s3://bucket/path"));
        // Cross-scheme variant exercises the same code path.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path%2F", "cos://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeQueryStringDistinguishes() {
        // Query strings are compared, so locations differing only in their query are not equal.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "oss://bucket/path?v=1", "oss://bucket/path?v=2"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path?v=1", "s3://bucket/path"));
        // The query check also applies in the cross-scheme (oneIsS3) branch.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path?v=1", "cos://bucket/path?v=2"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path?v=1", "cos://bucket/path?v=1"));
    }

    @Test
    public void testEqualsIgnoreSchemeSchemelessIdentical() {
        // Two identical schemeless (bare-path) locations parse with a null scheme and must still
        // compare equal (identity property).
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "/user/warehouse/tbl", "/user/warehouse/tbl"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "/user/warehouse/tbl/", "/user/warehouse/tbl"));
        // Two different bare paths must NOT be equal: proves the schemeless structural comparison
        // actually discriminates rather than returning true for all schemeless inputs.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "/user/warehouse/a", "/user/warehouse/b"));
    }

    @Test
    public void testEqualsIgnoreSchemeNullInputs() {
        // Both null -> equal; exactly one null -> not equal.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(null, null));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(null, "s3://bucket/path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3("s3://bucket/path", null));
    }

    @Test
    public void testEqualsIgnoreSchemeSchemeCaseInsensitive() {
        // Schemes are compared case-insensitively (RFC 3986 section 3.1), so two same-scheme URIs
        // differing only in scheme case still compare equal via the sameScheme branch.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "OSS://bucket/path", "oss://bucket/path"));
        // An uppercase "S3" scheme is recognized as s3 (oneIsS3 uses equalsIgnoreCase), so it
        // triggers the cross-scheme structural comparison against a non-s3 scheme.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "S3://bucket/path", "cos://bucket/path"));
        // Path case still matters in that cross-scheme arm.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "S3://bucket/Path", "cos://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeFragmentDistinguishes() {
        // Fragments are compared, so locations differing only in their fragment are not equal.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path#sectionA", "s3://bucket/path#sectionB"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path#section", "s3://bucket/path"));
        // The fragment check also applies in the cross-scheme (oneIsS3) branch, mirroring the
        // query-string coverage: fragment and query are compared identically in the code.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path#sectionA", "cos://bucket/path#sectionB"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path#section", "cos://bucket/path#section"));
    }

    @Test
    public void testEqualsIgnoreSchemeNullAuthorityNotCrossSchemeEqual() {
        // A URI with a scheme but absent authority (triple-slash form) is malformed for object
        // storage and must NOT compare equal to a different-scheme URI just because the paths match.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "file:///path/to/file", "s3:///path/to/file"));
        // A schemeless bare path must not equal an s3 triple-slash URI with the same path segment.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "/path/data", "s3:///path/data"));
        // But an identical triple-slash URI is still equal via the exact-string fallback.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3:///path/data", "s3:///path/data"));
        // Same-scheme triple-slash differing only by a trailing slash: the malformed guard
        // (scheme present, authority absent) fires -> exact string compare -> false. This is the
        // load-bearing case for the guard: without it both URIs parse with authority=null and paths
        // "/path/" vs "/path", normalize() strips the trailing slash and they would spuriously
        // compare equal (a bug). Covered for both file:// and s3:// since each takes the guard.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "file:///path/", "file:///path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3:///path/", "s3:///path"));
        // Same-scheme triple-slash with different path content (not just a trailing slash) confirms
        // the guard is not incidentally bypassed for triple-slash inputs.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "file:///path/a", "file:///path/b"));
    }

    @Test
    public void testEqualsIgnoreSchemeNetworkPathReferenceNotCrossSchemeEqual() {
        // A network-path reference ("//bucket/path") parses with scheme=null, authority="bucket".
        // It is not a valid object-storage location and must NOT spuriously match a fully-qualified
        // s3 URI via the structural comparison.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "//bucket/path", "s3://bucket/path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path", "//bucket/path"));
        // But identical network-path references remain equal via the exact-string fallback.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "//bucket/path", "//bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeRootVsEmptyDistinct() {
        // The filesystem root "/" and the empty/current path "" are distinct locations and must NOT
        // compare equal even though both normalize() to "". Each is still equal to itself.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3("/", ""));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3("", "/"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3("/", "/"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3("", ""));
    }

    @Test
    public void testEqualsIgnoreSchemeS3aAndS3n() {
        // s3 vs s3a / s3n: schemes differ but one side is "s3", so the cross-scheme structural
        // comparison applies and matching authority+path compare equal. This pins the exact
        // contract that HiveTableSinkTest depends on (it passes s3a:// and s3n:// locations to
        // PathUtils.equalsIgnoreSchemeIfOneIsS3 expecting them to match an s3:// location).
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path", "s3a://bucket/path"));
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/path", "s3n://bucket/path"));
        // s3a is NOT s3, so a cross-scheme comparison between two non-s3 schemes (s3a vs cos)
        // must NOT match -- the !sameScheme && !oneIsS3 branch returns false. Guards against a
        // future change that widens the s3-family predicate or drops the exact "s3" match.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3a://bucket/path", "cos://bucket/path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3n://bucket/path", "cos://bucket/path"));
    }

    @Test
    public void testEqualsIgnoreSchemePortSignificant() {
        // Port is part of the raw authority; differing ports mean different servers.
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket:80/path", "s3://bucket/path"));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "hdfs://namenode:8020/data", "hdfs://namenode/data"));
        // Same port on both sides of a cross-scheme comparison still matches.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket:443/path", "cos://bucket:443/path"));
    }

    @Test
    public void testEqualsIgnoreSchemeInvalidUri() {
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/data file.txt",
                "cos://bucket/data file.txt"
        ));

        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/data file.txt",
                "cos://bucket/other file.txt"
        ));

        // The fallback for unparseable URIs is exact (case-sensitive) string comparison:
        // identical strings are equal, case-differing ones are not.
        Assertions.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/data file.txt",
                "s3://bucket/data file.txt"
        ));
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/Data file.txt",
                "s3://bucket/data file.txt"
        ));
    }
}
