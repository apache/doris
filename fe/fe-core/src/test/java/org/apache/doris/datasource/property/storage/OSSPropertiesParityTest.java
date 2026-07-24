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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core OSSProperties behaviour. */
public class OSSPropertiesParityTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        return props;
    }

    @Test
    public void testIdentity() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Assertions.assertTrue(sp instanceof OSSProperties);
        Assertions.assertEquals(StorageProperties.Type.OSS, sp.getType());
        // 2.4-1: fe-core reports "S3" for OSS (inherited), NOT "OSS".
        Assertions.assertEquals("S3", sp.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("oss"), ((OSSProperties) sp).schemas());
    }

    @Test
    public void testBasicBackendMapExact() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        // OSS family defaults: 100/10000/10000; with ak/sk present there is NO
        // AWS_CREDENTIALS_PROVIDER_TYPE key at all (base-class behaviour).
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com",
                "AWS_REGION", "cn-hangzhou",
                "AWS_ACCESS_KEY", "myAk",
                "AWS_SECRET_KEY", "mySk",
                "AWS_MAX_CONNECTIONS", "100",
                "AWS_REQUEST_TIMEOUT_MS", "10000",
                "AWS_CONNECTION_TIMEOUT_MS", "10000",
                "use_path_style", "false"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testAnonymousEmitsAnonymousProviderType() {
        // 2.4-5: OSS anonymous DOES emit AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS.
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss.support", "true");
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        StorageProperties sp = StorageProperties.createPrimary(props);
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com",
                "AWS_REGION", "cn-hangzhou",
                "AWS_ACCESS_KEY", "",
                "AWS_SECRET_KEY", "",
                "AWS_MAX_CONNECTIONS", "100",
                "AWS_REQUEST_TIMEOUT_MS", "10000",
                "AWS_CONNECTION_TIMEOUT_MS", "10000",
                "use_path_style", "false",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testNonStandardEndpointRewrittenToInternal() {
        // initNormalizeAndCheckProps rewrites any endpoint that does not match the
        // standard OSS pattern to oss-<region>-internal.aliyuncs.com (dlf.access.public
        // defaults to false). The S3-compatible form s3.<region>.aliyuncs.com is one
        // such case — freeze this surprising behaviour.
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss.support", "true");
        props.put("s3.endpoint", "s3.cn-hangzhou.aliyuncs.com");
        props.put("s3.region", "cn-hangzhou");
        props.put("s3.access_key", "myAk");
        props.put("s3.secret_key", "mySk");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com",
                sp.getBackendConfigProperties().get("AWS_ENDPOINT"));
    }

    @Test
    public void testHadoopConfigJindoAndS3aBlock() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Configuration conf = sp.getHadoopStorageConfig();
        ParityAsserts.assertConfContains(conf, ParityAsserts.map(
                "fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem",
                "fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.JindoOSS",
                "fs.oss.accessKeyId", "myAk",
                "fs.oss.accessKeySecret", "mySk",
                "fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "fs.oss.region", "cn-hangzhou",
                // the legacy s3a compatibility block is always appended first
                "fs.s3a.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "fs.s3a.endpoint.region", "cn-hangzhou",
                // ensureDisableCache over schemas {oss}
                "fs.oss.impl.disable.cache", "true"
        ));
        Assertions.assertNotNull(conf.get("io.file.buffer.size"));
    }
}
