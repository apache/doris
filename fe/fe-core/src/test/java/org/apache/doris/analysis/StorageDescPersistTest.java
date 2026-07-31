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

package org.apache.doris.analysis;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

public class StorageDescPersistTest {

    @Test
    public void testBrokerDescRestoreStoragePropertiesAfterGsonRoundTrip() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("broker.username", "user");
        properties.put("broker.password", "password");
        BrokerDesc brokerDesc = new BrokerDesc("test_broker", properties);

        BrokerDesc restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(brokerDesc), BrokerDesc.class);

        Assert.assertNotNull(restored.getStorageAdapter());
        Assert.assertEquals("BROKER", restored.getStorageAdapter().getStorageName());
        Assert.assertEquals("test_broker", restored.getStorageAdapter().getBrokerName());
        Assert.assertEquals("user", restored.getStorageAdapter().getBackendConfigProperties()
                .get("broker.username"));
    }

    @Test
    public void testBrokerLoadJobRestoreS3StoragePropertiesAfterGsonRoundTrip() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "ak");
        properties.put("s3.secret_key", "sk");
        properties.put("s3.bucket", "test-bucket");
        BrokerDesc brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, properties);
        BrokerLoadJob job = new BrokerLoadJob();
        setField(BrokerLoadJob.class.getSuperclass(), job, "brokerDesc", brokerDesc);

        BrokerLoadJob restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), BrokerLoadJob.class);
        BrokerDesc restoredBrokerDesc =
                (BrokerDesc) getField(BrokerLoadJob.class.getSuperclass(), restored, "brokerDesc");
        StorageAdapter restoredStorageProperties = restoredBrokerDesc.getStorageAdapter();

        Assert.assertNotNull(restoredStorageProperties);
        Assert.assertEquals("S3", restoredStorageProperties.getStorageName());
        Assert.assertEquals(EtlJobType.BROKER, restored.getJobType());
        Assert.assertEquals(StorageBackend.StorageType.S3, restoredBrokerDesc.getStorageType());
        Assert.assertEquals("test-bucket", restoredStorageProperties.getOrigProps().get("s3.bucket"));
        Assert.assertNotNull(restoredBrokerDesc.getStorageAdapter());
        Assert.assertEquals("S3", restoredBrokerDesc.getStorageAdapter().getStorageName());
        Assert.assertEquals("test-bucket",
                restoredBrokerDesc.getStorageAdapter().getOrigProps().get("s3.bucket"));
    }

    private static void setField(Class<?> clazz, Object target, String fieldName, Object value)
            throws ReflectiveOperationException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Class<?> clazz, Object target, String fieldName)
            throws ReflectiveOperationException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
