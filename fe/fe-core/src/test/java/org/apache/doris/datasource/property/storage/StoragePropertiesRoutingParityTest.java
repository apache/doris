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
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase A5 golden tests freezing createAll/createPrimary routing semantics:
 * fixed provider priority, default-HDFS fallback at index 0, and the
 * fs.xx.support=true kill-switch for guessIsMe heuristics (2.3-④ oracle).
 */
public class StoragePropertiesRoutingParityTest {

    @Test
    public void testCreateAllAddsHdfsFallbackAtIndexZero() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "myAk");
        props.put("s3.secret_key", "mySk");
        List<StorageProperties> all = StorageProperties.createAll(props);
        Assertions.assertEquals(2, all.size());
        Assertions.assertTrue(all.get(0) instanceof HdfsProperties);
        Assertions.assertFalse(((HdfsProperties) all.get(0)).isExplicitlyConfigured());
        Assertions.assertTrue(all.get(1) instanceof S3Properties);
    }

    @Test
    public void testAmbiguousPropsDoubleHitWithoutExplicitFlag() throws UserException {
        // OSS matches on the aliyuncs endpoint; S3 matches purely on the s3.region
        // key. Without an explicit fs.xx.support flag BOTH are instantiated, plus
        // the default HDFS fallback: this is the ambiguity that motivates the SPI
        // explicit-only contract (2.3-④).
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        props.put("s3.region", "cn-hangzhou");
        List<StorageProperties> all = StorageProperties.createAll(props);
        Assertions.assertEquals(3, all.size());
        Assertions.assertTrue(all.get(0) instanceof HdfsProperties);
        Assertions.assertTrue(all.get(1) instanceof OSSProperties);
        Assertions.assertTrue(all.get(2) instanceof S3Properties);
    }

    @Test
    public void testExplicitOssSupportSuppressesGuessAndFallback() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss.support", "true");
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        props.put("s3.region", "cn-hangzhou");
        List<StorageProperties> all = StorageProperties.createAll(props);
        Assertions.assertEquals(1, all.size());
        Assertions.assertTrue(all.get(0) instanceof OSSProperties);
    }

    @Test
    public void testCreatePrimaryGuessesHdfsFromUri() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://ns1/warehouse/t");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertTrue(sp instanceof HdfsProperties);
        Assertions.assertTrue(((HdfsProperties) sp).isExplicitlyConfigured());
    }

    @Test
    public void testCreatePrimaryEmptyThrows() {
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> StorageProperties.createPrimary(new HashMap<>()));
    }

    @Test
    public void testCreateAllEmptyYieldsOnlyHdfsFallback() throws UserException {
        List<StorageProperties> all = StorageProperties.createAll(new HashMap<>());
        Assertions.assertEquals(1, all.size());
        Assertions.assertTrue(all.get(0) instanceof HdfsProperties);
        Assertions.assertFalse(((HdfsProperties) all.get(0)).isExplicitlyConfigured());
    }

    @Test
    public void testOssHdfsWinsOverOssForDlsEndpoint() {
        // OSS-HDFS and OSS are mutually exclusive; the oss-dls endpoint routes to
        // OSS-HDFS before plain OSS is even considered.
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertTrue(sp instanceof OSSHdfsProperties);
    }
}
