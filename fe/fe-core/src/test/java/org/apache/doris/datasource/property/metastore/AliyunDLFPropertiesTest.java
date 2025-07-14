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

import org.apache.doris.common.UserException;

import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

@Disabled("wait metastore integration")
public class AliyunDLFPropertiesTest {
    private static Map<String, String> baseProps;

    @BeforeAll
    public static void init() {
        baseProps = new HashMap<>();
        baseProps.put("paimon.catalog.type", "DLF");
        baseProps.put("dlf.access_key", "my-access-key");
        baseProps.put("dlf.secret_key", "my-secret-key");
        baseProps.put("dlf.region", "cn-hangzhou");
        baseProps.put("dlf.uid", "uid123");
        baseProps.put("dlf.access.public", "true");
        baseProps.put("dlf.extra.config", "extraValue");
        baseProps.put("not.dlf.key", "ignoreMe");
    }

    @Test
    public void testConstructor_shouldCaptureOnlyDlfPrefixedProps() throws UserException {
        AliyunDLFProperties props = (AliyunDLFProperties) MetastoreProperties.create(baseProps);
        Map<String, String> others = props.getOtherDlfProps();
        Assertions.assertTrue(others.containsKey("dlf.extra.config"));
        Assertions.assertFalse(others.containsKey("not.dlf.key"));
    }

    @Test
    public void testToPaimonOptions_withExplicitEndpoint() throws UserException {
        baseProps.put("dlf.endpoint", "explicit.endpoint.aliyun.com");

        AliyunDLFProperties props = (AliyunDLFProperties) MetastoreProperties.create(baseProps);
        Options options = new Options();
        props.toPaimonOptions(options);

        Assertions.assertEquals("explicit.endpoint.aliyun.com", options.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("my-access-key", options.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("my-secret-key", options.get("dlf.catalog.accessKeySecret"));
        Assertions.assertEquals("cn-hangzhou", options.get("dlf.catalog.region"));
        Assertions.assertEquals("uid123", options.get("dlf.catalog.uid"));
        Assertions.assertEquals("true", options.get("dlf.catalog.accessPublic"));
        Assertions.assertEquals("DLF_ONLY", options.get("dlf.catalog.proxyMode"));
        Assertions.assertEquals("false", options.get("dlf.catalog.createDefaultDBIfNotExist"));

        // extra config
        Assertions.assertEquals("extraValue", options.get("dlf.extra.config"));
    }

    @Test
    public void testToPaimonOptions_publicAccess() throws UserException {
        baseProps.remove("dlf.endpoint");
        baseProps.put("dlf.access.public", "TrUe"); // 测试大小写

        AliyunDLFProperties props = (AliyunDLFProperties) MetastoreProperties.create(baseProps);

        Options options = new Options();
        props.toPaimonOptions(options);

        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", options.get("dlf.catalog.endpoint"));
    }

    @Test
    public void testToPaimonOptions_privateVpcAccess() throws UserException {
        baseProps.remove("dlf.endpoint");
        baseProps.put("dlf.access.public", "true");

        AliyunDLFProperties props = (AliyunDLFProperties) MetastoreProperties.create(baseProps);
        Options options = new Options();
        props.toPaimonOptions(options);

        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", options.get("dlf.catalog.endpoint"));
    }

    @Test
    public void testToPaimonOptions_defaultVpcWhenPublicMissing() throws UserException {
        baseProps.remove("dlf.endpoint");
        baseProps.put("dlf.access.public", "false");

        AliyunDLFProperties props = (AliyunDLFProperties) MetastoreProperties.create(baseProps);

        Options options = new Options();
        props.toPaimonOptions(options);

        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", options.get("dlf.catalog.endpoint"));
    }

    @Test
    public void testToPaimonOptions_emptyConstructor() throws UserException {
        AliyunDLFProperties props = (AliyunDLFProperties) MetastoreProperties.create(baseProps);


        Options options = new Options();
        props.toPaimonOptions(options);
        // 检查关键字段存在
        Assertions.assertEquals("DLF_ONLY", options.get("dlf.catalog.proxyMode"));
        Assertions.assertEquals("false", options.get("dlf.catalog.createDefaultDBIfNotExist"));
    }

    @Test
    public void testGetResourceConfigPropName() {
        AliyunDLFProperties props = new AliyunDLFProperties(baseProps);
        Assertions.assertEquals("dlf.resource_config", props.getResourceConfigPropName());
    }
}
