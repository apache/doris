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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


public class AliyunDLFBasePropertiesTest {

    @Test
    void testAutoGenerateEndpointWithPublicAccess() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.access.public", "true");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", dlfProps.dlfEndpoint);
    }

    @Test
    void testAutoGenerateEndpointWithVpcAccess() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.access.public", "false");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", dlfProps.dlfEndpoint);
    }

    @Test
    void testExplicitEndpointOverridesAutoGeneration() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-beijing");
        props.put("dlf.endpoint", "custom.endpoint.com");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("custom.endpoint.com", dlfProps.dlfEndpoint);
    }

    @Test
    void testMissingEndpointAndRegionThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");

        StoragePropertiesException ex = Assertions.assertThrows(
                StoragePropertiesException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertEquals("dlf.endpoint is required.", ex.getMessage());
    }

    @Test
    void testMissingAccessKeyThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.secret_key", "sk");
        props.put("dlf.endpoint", "custom.endpoint.com");

        Exception ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("dlf.access_key is required"));
    }

    @Test
    void testMissingSecretKeyThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.endpoint", "custom.endpoint.com");

        Exception ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("dlf.secret_key is required"));
    }
}
