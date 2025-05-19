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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HdfsPropertiesUtilsTest {

    @Test
    public void testCheckLoadPropsAndReturnUri_success() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://localhost:9000/data/file.txt");

        String result = HdfsPropertiesUtils.validateAndGetUri(props);
        Assertions.assertEquals("hdfs://localhost:9000/data/file.txt", result);
    }

    @Test
    public void testCheckLoadPropsAndReturnUri_emptyProps() {
        Map<String, String> props = new HashMap<>();

        Exception exception = Assertions.assertThrows(UserException.class, () -> {
            HdfsPropertiesUtils.validateAndGetUri(props);
        });
        Assertions.assertEquals("errCode = 2, detailMessage = props is empty", exception.getMessage());
    }

    @Test
    public void testCheckLoadPropsAndReturnUri_missingUriKey() {
        Map<String, String> props = new HashMap<>();
        props.put("path", "xxx");

        Exception exception = Assertions.assertThrows(UserException.class, () -> {
            HdfsPropertiesUtils.validateAndGetUri(props);
        });
        Assertions.assertEquals("errCode = 2, detailMessage = props must contain uri", exception.getMessage());
    }

    @Test
    public void testConvertUrlToFilePath_valid() throws Exception {
        String uri = "viewfs://cluster/user/test";
        String result = HdfsPropertiesUtils.convertUrlToFilePath(uri);
        Assertions.assertEquals("viewfs://cluster/user/test", result);
    }

    @Test
    public void testConvertUrlToFilePath_invalidSchema() {
        String uri = "s3://bucket/file.txt";

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HdfsPropertiesUtils.convertUrlToFilePath(uri);
        });
        Assertions.assertTrue(exception.getMessage().contains("Invalid export path"));
    }

    @Test
    public void testConvertUrlToFilePath_blankUri() {
        String uri = "   ";

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HdfsPropertiesUtils.convertUrlToFilePath(uri);
        });
        Assertions.assertTrue(exception.getMessage().contains("uri is null"));
    }

    @Test
    public void testConstructDefaultFsFromUri_valid() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://localhost:8020/data");

        String result = HdfsPropertiesUtils.constructDefaultFsFromUri(props);
        Assertions.assertEquals("hdfs://localhost:8020", result);
    }

    @Test
    public void testConstructDefaultFsFromUri_viewfs() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "viewfs://cluster/path");

        String result = HdfsPropertiesUtils.constructDefaultFsFromUri(props);
        Assertions.assertEquals("viewfs://cluster", result);
    }

    @Test
    public void testConstructDefaultFsFromUri_invalidSchema() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "obs://bucket/test");

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HdfsPropertiesUtils.constructDefaultFsFromUri(props);
        });
        Assertions.assertTrue(exception.getMessage().contains("Invalid export path"));
    }

    @Test
    public void testConstructDefaultFsFromUri_emptyProps() {
        Map<String, String> props = new HashMap<>();
        String result = HdfsPropertiesUtils.constructDefaultFsFromUri(props);
        Assertions.assertNull(result);
    }

    @Test
    public void testConstructDefaultFsFromUri_missingUri() {
        Map<String, String> props = new HashMap<>();
        props.put("x", "y");

        String result = HdfsPropertiesUtils.constructDefaultFsFromUri(props);
        Assertions.assertNull(result);
    }

    @Test
    public void testConstructDefaultFsFromUri_blankUri() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "  ");

        String result = HdfsPropertiesUtils.constructDefaultFsFromUri(props);
        Assertions.assertNull(result);
    }

    @Test
    public void testConvertUrlToFilePath_uppercaseSchema() throws Exception {
        String uri = "HDFS://localhost:9000/test";
        String result = HdfsPropertiesUtils.convertUrlToFilePath(uri);
        Assertions.assertEquals("HDFS://localhost:9000/test", result);
    }
}
