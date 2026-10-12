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

package org.apache.doris.fs;

import org.apache.doris.filesystem.properties.FileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Cloud marker text inside an object key must not decide which filesystem serves the map: the
 * markers are host names, so a competing provider may only claim (or reject) a map by its uri host.
 */
public class ObjectKeyMarkerRoutingTest {

    private static FileSystemPluginManager manager;

    @BeforeAll
    static void setUp() {
        manager = new FileSystemPluginManager();
        manager.loadBuiltins();
    }

    @Test
    public void testMinioConfigurationKeepsMinioWhenTheKeyLooksLikeAws() {
        FileSystemProperties bound = manager.bindPrimary(minioMap("s3://bucket/archive/AMAZONAWS.COM/data"));

        Assertions.assertEquals("MINIO", bound.providerName().toUpperCase());
    }

    @Test
    public void testMinioConfigurationKeepsMinioWhenTheKeyLooksLikeCos() {
        FileSystemProperties bound = manager.bindPrimary(minioMap("s3://bucket/archive/MYQCLOUD.COM/data"));

        Assertions.assertEquals("MINIO", bound.providerName().toUpperCase());
    }

    private static Map<String, String> minioMap(String uri) {
        Map<String, String> props = new HashMap<>();
        props.put("minio.endpoint", "http://minio.internal:9000");
        props.put("minio.access_key", "ak");
        props.put("minio.secret_key", "sk");
        props.put("AWS_REGION", "us-east-1");
        props.put("uri", uri);
        return props;
    }
}
