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

public class PathUtilsTest {

    @Test
    public void testEqualsIgnoreScheme_sameHostAndPath() {
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
    public void testEqualsIgnoreScheme_differentHost() {
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
    public void testEqualsIgnoreScheme_trailingSlash() {
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "oss://bucket/data/",
                "oss://bucket/data"
        ));

        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "hdfs://namenode/user/hadoop/",
                "hdfs://namenode/user/hadoop"
        ));
    }

    @Test
    public void testEqualsIgnoreScheme_invalidURI() {
        // Special characters that break URI parsing
        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/data file.txt",
                "cos://bucket/data file.txt"
        ));

        Assertions.assertFalse(PathUtils.equalsIgnoreSchemeIfOneIsS3(
                "s3://bucket/data file.txt",
                "cos://bucket/other file.txt"
        ));
    }
}
