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

/** Phase A5 golden tests freezing current fe-core COSProperties behaviour. */
public class COSPropertiesParityTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("cos.endpoint", "cos.ap-guangzhou.myqcloud.com");
        props.put("cos.access_key", "myAk");
        props.put("cos.secret_key", "mySk");
        return props;
    }

    @Test
    public void testIdentity() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Assertions.assertTrue(sp instanceof COSProperties);
        Assertions.assertEquals(StorageProperties.Type.COS, sp.getType());
        Assertions.assertEquals("S3", sp.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("cos", "cosn"), ((COSProperties) sp).schemas());
    }

    @Test
    public void testBasicBackendMapExact() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "cos.ap-guangzhou.myqcloud.com",
                "AWS_REGION", "ap-guangzhou",
                "AWS_ACCESS_KEY", "myAk",
                "AWS_SECRET_KEY", "mySk",
                "AWS_MAX_CONNECTIONS", "100",
                "AWS_REQUEST_TIMEOUT_MS", "10000",
                "AWS_CONNECTION_TIMEOUT_MS", "10000",
                "use_path_style", "false"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testHadoopConfig() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Configuration conf = sp.getHadoopStorageConfig();
        ParityAsserts.assertConfContains(conf, ParityAsserts.map(
                "fs.cos.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.cosn.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.cosn.bucket.region", "ap-guangzhou",
                "fs.cosn.userinfo.secretId", "myAk",
                "fs.cosn.userinfo.secretKey", "mySk",
                "fs.cos.impl.disable.cache", "true",
                "fs.cosn.impl.disable.cache", "true"
        ));
    }
}
