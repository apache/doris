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

import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class S3URITest {
    @Test
    public void testLocationParsing() throws UserException {
        String p1 = "s3://my-bucket/path/to/file";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("path/to/file", uri1.getKey());
        Assert.assertEquals(Optional.empty(), uri1.getRegion());
        Assert.assertEquals(Optional.empty(), uri1.getEndpoint());
        Assert.assertEquals(Optional.empty(), uri1.getQueryParams());
    }

    @Test
    public void testVirtualHostStyleParsing() throws UserException {
        String p1 = "https://my-bucket.s3.us-west-1.amazonaws.com/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("resources/doc.txt", uri1.getKey());
        Assert.assertEquals("s3.us-west-1.amazonaws.com", uri1.getEndpoint().get());
        Assert.assertEquals("us-west-1", uri1.getRegion().get());
        Assert.assertEquals("abc123", uri1.getQueryParams().get().get("versionId").get(0));
        Assert.assertEquals(2, uri1.getQueryParams().get().get("partNumber").size());
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("77"));
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("88"));
    }

    @Test
    public void testPathStyleParsing() throws UserException {
        String p1 = "https://s3.us-west-1.amazonaws.com/my-bucket/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88";
        boolean isPathStyle = true;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("resources/doc.txt", uri1.getKey());
        Assert.assertEquals("s3.us-west-1.amazonaws.com", uri1.getEndpoint().get());
        Assert.assertEquals("us-west-1", uri1.getRegion().get());
        Assert.assertEquals("abc123", uri1.getQueryParams().get().get("versionId").get(0));
        Assert.assertEquals(2, uri1.getQueryParams().get().get("partNumber").size());
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("77"));
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("88"));
    }

    @Test
    public void testForceParsingStandardUri() throws UserException {
        String p1 = "s3://my-bucket.s3.us-west-1.amazonaws.com/path/to/file";
        S3URI uri1 = S3URI.create(p1, false, true);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("path/to/file", uri1.getKey());
        Assert.assertEquals("s3.us-west-1.amazonaws.com", uri1.getEndpoint().get());
        Assert.assertEquals("us-west-1", uri1.getRegion().get());
        Assert.assertEquals(Optional.empty(), uri1.getQueryParams());

        String p2 = "s3://s3.us-west-1.amazonaws.com/my-bucket/path/to/file";
        S3URI uri2 = S3URI.create(p2, true, true);

        Assert.assertEquals("my-bucket", uri2.getBucket());
        Assert.assertEquals("path/to/file", uri2.getKey());
        Assert.assertEquals("s3.us-west-1.amazonaws.com", uri2.getEndpoint().get());
        Assert.assertEquals(Optional.empty(), uri1.getQueryParams());
    }

    @Test
    public void testOSSVirtualHostStyle() throws UserException {
        String p1 = "https://my-bucket.oss-cn-bejing.aliyuncs.com/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("resources/doc.txt", uri1.getKey());
        Assert.assertEquals("oss-cn-bejing.aliyuncs.com", uri1.getEndpoint().get());
        Assert.assertEquals("oss-cn-bejing", uri1.getRegion().get());
        Assert.assertEquals("abc123", uri1.getQueryParams().get().get("versionId").get(0));
        Assert.assertEquals(2, uri1.getQueryParams().get().get("partNumber").size());
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("77"));
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("88"));
    }

    @Test
    public void testOSSPathStyle() throws UserException {
        String p1 = "https://oss-cn-bejing.aliyuncs.com/my-bucket/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88";
        boolean isPathStyle = true;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("resources/doc.txt", uri1.getKey());
        Assert.assertEquals("oss-cn-bejing.aliyuncs.com", uri1.getEndpoint().get());
        Assert.assertEquals("oss-cn-bejing", uri1.getRegion().get());
        Assert.assertEquals("abc123", uri1.getQueryParams().get().get("versionId").get(0));
        Assert.assertEquals(2, uri1.getQueryParams().get().get("partNumber").size());
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("77"));
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("88"));
    }

    @Test
    public void testCOSVirtualHostStyle() throws UserException {
        String p1 = "https://my-bucket.cos.ap-beijing.myqcloud.com/resources/doc.txt";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("resources/doc.txt", uri1.getKey());
        Assert.assertEquals("cos.ap-beijing.myqcloud.com", uri1.getEndpoint().get());
        Assert.assertEquals("ap-beijing", uri1.getRegion().get());
    }

    @Test
    public void testOBSVirtualHostStyle() throws UserException {
        String p1 = "https://my-bucket.obs.cn-north-4.myhuaweicloud.com/test_obs/000000_0";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("my-bucket", uri1.getBucket());
        Assert.assertEquals("test_obs/000000_0", uri1.getKey());
        Assert.assertEquals("obs.cn-north-4.myhuaweicloud.com", uri1.getEndpoint().get());
        Assert.assertEquals("cn-north-4", uri1.getRegion().get());
    }

    @Test
    public void testEncodedString() throws UserException {
        String p1 = "s3://bucket/path%20to%20file?txt=hello%20world&partNumber=77&partNumber=88";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path%20to%20file", uri1.getKey());
        Assert.assertEquals(Optional.empty(), uri1.getEndpoint());
        Assert.assertEquals(Optional.empty(), uri1.getRegion());
        Assert.assertEquals("hello%20world", uri1.getQueryParams().get().get("txt").get(0));
        Assert.assertEquals(2, uri1.getQueryParams().get().get("partNumber").size());
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("77"));
        Assert.assertTrue(uri1.getQueryParams().get().get("partNumber").contains("88"));
    }

    @Test
    public void testHadoopEncodedString() throws UserException {
        String p1 = "s3://bucket/path%20to%20file/abc%3Aqqq=xyz%2Fyyy zzz";
        boolean isPathStyle = false;
        boolean forceParsingStandardUri = false;
        S3URI uri1 = S3URI.create(p1, isPathStyle, forceParsingStandardUri);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path%20to%20file/abc%3Aqqq=xyz%2Fyyy zzz", uri1.getKey());
        Assert.assertEquals(Optional.empty(), uri1.getEndpoint());
        Assert.assertEquals(Optional.empty(), uri1.getRegion());
    }

    @Test(expected = UserException.class)
    public void missingBucket() throws UserException {
        S3URI.create("https:///");
    }

    @Test(expected = UserException.class)
    public void missingKey() throws UserException {
        S3URI.create("https://bucket/");
    }

    @Test(expected = UserException.class)
    public void relativePathing() throws UserException {
        S3URI.create("/path/to/file");
    }

    @Test(expected = UserException.class)
    public void invalidScheme() throws UserException {
        S3URI.create("ftp://bucket/");
    }

    @Test
    public void testQueryAndFragment() throws UserException {
        String p1 = "s3://bucket/path/to/file?query=foo#bar";
        S3URI uri1 = S3URI.create(p1);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path/to/file", uri1.getKey());
        Assert.assertEquals(Optional.empty(), uri1.getEndpoint());
        Assert.assertEquals(Optional.empty(), uri1.getRegion());
        Assert.assertEquals("foo", uri1.getQueryParams().get().get("query").get(0));

    }
}
