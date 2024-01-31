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

import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.FileSystemType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class S3UtilTest {
    @Test
    public void testLocationConvert() {
        String loc;
        loc = S3Util.convertToS3IfNecessary("hdfs://dir/file.path", new HashMap<>());
        Assertions.assertTrue(loc.startsWith("hdfs://"));

        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns");
        loc = S3Util.convertToS3IfNecessary("hdfs:///dir/file.path", props);
        Assertions.assertTrue(loc.startsWith("hdfs://") && !loc.startsWith("hdfs:///"));
        loc = S3Util.convertToS3IfNecessary("hdfs:/dir/file.path", props);
        Assertions.assertTrue(loc.startsWith("hdfs://"));
        props.put("dfs.nameservices", "");
        loc = S3Util.convertToS3IfNecessary("hdfs:/dir/file.path", props);
        Assertions.assertTrue(loc.startsWith("/dir") && !loc.startsWith("hdfs://"));

        loc = S3Util.convertToS3IfNecessary("oss://test.com", props);
        Assertions.assertTrue(loc.startsWith("oss://"));

        loc = S3Util.convertToS3IfNecessary("gcs://test.com", props);
        Assertions.assertTrue(loc.startsWith("gcs://"));

        loc = S3Util.convertToS3IfNecessary("cos://test.com", props);
        Assertions.assertTrue(loc.startsWith("cos://"));

        loc = S3Util.convertToS3IfNecessary("cosn://test.com", props);
        Assertions.assertTrue(loc.startsWith("cosn://"));

        loc = S3Util.convertToS3IfNecessary("obs://test.com", props);
        Assertions.assertTrue(loc.startsWith("obs://"));
    }


    @Test
    public void testScanRangeLocationConvert() throws Exception {
        String loc;
        Map<String, String> rangeProps = new HashMap<>();
        loc = S3Util.toScanRangeLocation("hdfs://dir/file.path", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("hdfs://"));
        Assertions.assertEquals(FileSystemFactory.getFSIdentity(loc, null).first, FileSystemType.DFS);

        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns");
        loc = S3Util.toScanRangeLocation("hdfs:///dir/file.path", props).toString();
        Assertions.assertTrue(loc.startsWith("hdfs://") && !loc.startsWith("hdfs:///"));
        loc = S3Util.toScanRangeLocation("hdfs:/dir/file.path", props).toString();
        Assertions.assertTrue(loc.startsWith("hdfs://"));
        props.put("dfs.nameservices", "");
        loc = S3Util.toScanRangeLocation("hdfs:/dir/file.path", props).toString();
        Assertions.assertTrue(loc.startsWith("/dir") && !loc.startsWith("hdfs://"));

        loc = S3Util.toScanRangeLocation("oss://test.com", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("s3://"));
        Assertions.assertEquals(FileSystemFactory.getFSIdentity(loc, null).first, FileSystemType.S3);

        loc = S3Util.toScanRangeLocation("oss://test.oss-dls.aliyuncs.com/path", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("oss://test.oss-dls.aliyuncs"));
        Assertions.assertEquals(FileSystemFactory.getFSIdentity(loc, null).first, FileSystemType.DFS);

        loc = S3Util.toScanRangeLocation("cos://test.com", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("s3://"));
        Assertions.assertEquals(FileSystemFactory.getFSIdentity(loc, null).first, FileSystemType.S3);

        loc = S3Util.toScanRangeLocation("cosn://test.com", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("cosn://"));
        Assertions.assertEquals(FileSystemFactory.getFSIdentity(loc, null).first, FileSystemType.OFS);

        loc = S3Util.toScanRangeLocation("obs://test.com", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("s3://"));
        Assertions.assertEquals(FileSystemFactory.getFSIdentity(loc, null).first, FileSystemType.S3);

        loc = S3Util.toScanRangeLocation("unknown://test.com", rangeProps).toString();
        Assertions.assertTrue(loc.startsWith("unknown://"));
    }
}
