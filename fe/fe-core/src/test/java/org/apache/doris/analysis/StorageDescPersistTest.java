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
import org.apache.doris.thrift.TFileType;

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

    @Test
    public void testGcpNativeCredentialForExportBroker() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("provider", "GCP");
        properties.put("s3.region", "us-east1");
        properties.put("gs.credential_provider_type", "DEFAULT");

        // EXPORT forwards this BrokerDesc map to BE Writer. Verify the shared FE conversion
        // preserves the provider-specific credential while keeping the S3-compatible file type.
        BrokerDesc brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, properties);
        Assertions.assertEquals("GCS", brokerDesc.getStorageAdapter().getSpiProperties().providerName());
        Assertions.assertEquals(TFileType.FILE_S3, brokerDesc.getFileType());
        Assertions.assertEquals("GCP", brokerDesc.getBackendConfigProperties().get("provider"));
        Assertions.assertEquals("DEFAULT", brokerDesc.getBackendConfigProperties()
                .get("gs.credential_provider_type"));
        Assertions.assertEquals("s3://export-bucket/path",
                brokerDesc.getFileLocation("gs://export-bucket/path"));
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
