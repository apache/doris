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
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.fs.TestFileSystemPluginManagers;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

        Assertions.assertNotNull(restored.getStorageAdapter());
        Assertions.assertEquals("BROKER", restored.getStorageAdapter().getStorageName());
        Assertions.assertEquals("test_broker", restored.getStorageAdapter().getBrokerName());
        Assertions.assertEquals("user", restored.getStorageAdapter().getBackendConfigProperties()
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

        Assertions.assertNotNull(restoredStorageProperties);
        Assertions.assertEquals("S3", restoredStorageProperties.getStorageName());
        Assertions.assertEquals(EtlJobType.BROKER, restored.getJobType());
        Assertions.assertEquals(StorageBackend.StorageType.S3, restoredBrokerDesc.getStorageType());
        Assertions.assertEquals("test-bucket", restoredStorageProperties.getOrigProps().get("s3.bucket"));
        Assertions.assertNotNull(restoredBrokerDesc.getStorageAdapter());
        Assertions.assertEquals("S3", restoredBrokerDesc.getStorageAdapter().getStorageName());
        Assertions.assertEquals("test-bucket",
                restoredBrokerDesc.getStorageAdapter().getOrigProps().get("s3.bucket"));
    }

    /**
     * Deserialisation runs for every persisted load and export job at image load and journal replay.
     * With the job's filesystem provider absent - the plugin failed to load - it must not throw:
     * the FE would fail to load its image, or a serving follower would be killed by the next such
     * journal. The binding is left for the first use, where it fails with a Status, and succeeds
     * once the provider is back.
     */
    @Test
    public void testBrokerLoadJobRoundTripSurvivesAnAbsentProvider() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "ak");
        properties.put("s3.secret_key", "sk");
        properties.put("s3.bucket", "test-bucket");
        BrokerDesc brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, properties);
        BrokerLoadJob job = new BrokerLoadJob();
        setField(BrokerLoadJob.class.getSuperclass(), job, "brokerDesc", brokerDesc);
        String json = GsonUtils.GSON.toJson(job);

        StorageAdapter.initPluginManager(TestFileSystemPluginManagers.withoutProviders("S3"));
        try {
            BrokerLoadJob restored = Assertions.assertDoesNotThrow(
                    () -> GsonUtils.GSON.fromJson(json, BrokerLoadJob.class));
            BrokerDesc restoredDesc = (BrokerDesc) getField(BrokerLoadJob.class.getSuperclass(), restored, "brokerDesc");
            Assertions.assertNull(getField(StorageDesc.class, restoredDesc, "storageAdapter"),
                    "nothing binds at load while the provider is absent");
            Assertions.assertEquals("test-bucket", restoredDesc.getProperties().get("s3.bucket"));
            // The first use binds, and reports the absent provider instead of a broker.
            StoragePropertiesException atUse =
                    Assertions.assertThrows(StoragePropertiesException.class, restoredDesc::getStorageAdapter);
            Assertions.assertTrue(atUse.getMessage().contains("Loaded filesystem providers"), atUse.getMessage());

            StorageAdapter.initPluginManager(TestFileSystemPluginManagers.withoutProviders());
            Assertions.assertEquals("S3", restoredDesc.getStorageAdapter().getStorageName(),
                    "with the provider back, the same descriptor binds");
        } finally {
            StorageAdapter.initPluginManager(null);
        }
    }

    @Test
    public void testBrokerDescRoundTripSurvivesAnAbsentBrokerProvider() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("broker.username", "user");
        String json = GsonUtils.GSON.toJson(new BrokerDesc("test_broker", properties));

        StorageAdapter.initPluginManager(TestFileSystemPluginManagers.withoutProviders("Broker"));
        try {
            BrokerDesc restored = Assertions.assertDoesNotThrow(() -> GsonUtils.GSON.fromJson(json, BrokerDesc.class));
            Assertions.assertNull(getField(StorageDesc.class, restored, "storageAdapter"));
            Assertions.assertThrows(StoragePropertiesException.class, restored::getStorageAdapter);
        } finally {
            StorageAdapter.initPluginManager(null);
        }
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
