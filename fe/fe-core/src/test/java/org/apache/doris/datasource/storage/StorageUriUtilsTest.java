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

package org.apache.doris.datasource.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Frozen expectations for the provider-gated OSS bucket-domain rewrite in
 * {@link StorageUriUtils#validateAndNormalizeS3Uri}. The rewrite is applied ONLY for OSS
 * bindings (legacy semantics: it lived in the OSS typed class) — review finding: a
 * shape-based gate silently re-bucketed dotted bucket names under non-OSS bindings.
 */
public class StorageUriUtilsTest {

    private static String normalize(String uri, boolean ossBinding) {
        return StorageUriUtils.validateAndNormalizeS3Uri(uri, "false", "false", ossBinding);
    }

    @Test
    public void testOssBindingRewritesBucketDomainAuthority() {
        // virtual-hosted authority under the OSS binding: chopped at the first dot
        Assertions.assertEquals("s3://b/k",
                normalize("s3://b.oss-cn-hongkong-internal.aliyuncs.com/k", true));
        Assertions.assertEquals("s3://b/k",
                normalize("oss://b.oss-cn-hongkong.aliyuncs.com/k", true));
        // legacy chopped ANY dotted authority for OSS bindings, aliyun-shaped or not
        Assertions.assertEquals("s3://b/k",
                normalize("oss://b.custom-endpoint.example.com/k", true));
        // undotted authority untouched
        Assertions.assertEquals("s3://plainbucket/k", normalize("oss://plainbucket/k", true));
    }

    @Test
    public void testNonOssBindingNeverRewrites() {
        // review counter-case: dotted BUCKET NAME under an explicit S3 binding must survive
        Assertions.assertEquals("s3://logs.prod/archive/a.parquet",
                normalize("oss://logs.prod/archive/a.parquet", false));
        Assertions.assertEquals("s3://my.bucket.name/dir/key",
                normalize("s3://my.bucket.name/dir/key", false));
        // aliyun-shaped authority under a non-OSS binding also passes through untouched
        Assertions.assertEquals("s3://b.oss-cn-hongkong.aliyuncs.com/k",
                normalize("s3://b.oss-cn-hongkong.aliyuncs.com/k", false));
    }

    @Test
    public void testHttpUrisUnaffectedByTheRewriteGate() {
        // http(s) uris skip the rewrite in BOTH modes (legacy: standard parser handles them);
        // virtual-hosted host parse extracts the first label as the bucket either way
        Assertions.assertEquals("s3://b/k",
                normalize("https://b.oss-cn-hongkong.aliyuncs.com/k", true));
        Assertions.assertEquals("s3://b/k",
                normalize("https://b.oss-cn-hongkong.aliyuncs.com/k", false));
    }
}
