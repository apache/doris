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

package org.apache.doris.filesystem.obs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.ObjectStorageFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

class ObsFileSystemPropertiesTest {

    @Test
    void bind_usesFeCoreObsAliasOrder() {
        ObsFileSystemProperties properties = ObsFileSystemProperties.of(Map.of(
                "obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com",
                "obs.access_key", "obs-ak",
                "AWS_ACCESS_KEY", "aws-ak",
                "obs.secret_key", "obs-sk",
                "AWS_SECRET_KEY", "aws-sk"));

        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com", properties.getEndpoint());
        Assertions.assertEquals("cn-north-4", properties.getRegion());
        Assertions.assertEquals("obs-ak", properties.getAccessKey());
        Assertions.assertEquals("obs-sk", properties.getSecretKey());
        Assertions.assertEquals("obs-ak", properties.toFileSystemKv().get("OBS_ACCESS_KEY"));
    }

    @Test
    void bind_allowsAnonymousWhenAccessKeyAndSecretKeyAreBothBlank() {
        ObsFileSystemProperties properties = ObsFileSystemProperties.of(Map.of(
                "obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com"));

        Assertions.assertEquals("", properties.getAccessKey());
        Assertions.assertEquals("", properties.getSecretKey());
        Assertions.assertEquals("cn-north-4", properties.getRegion());
    }

    @Test
    void bind_acceptsLegacyAwsKeysForExistingObsCallers() {
        ObsFileSystemProperties properties = ObsFileSystemProperties.of(Map.of(
                "AWS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com",
                "AWS_ACCESS_KEY", "aws-ak",
                "AWS_SECRET_KEY", "aws-sk",
                "AWS_BUCKET", "legacy-bucket"));

        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com", properties.getEndpoint());
        Assertions.assertEquals("cn-north-4", properties.getRegion());
        Assertions.assertEquals("aws-ak", properties.getAccessKey());
        Assertions.assertEquals("aws-sk", properties.getSecretKey());
        Assertions.assertEquals("legacy-bucket", properties.getBucket());
        Assertions.assertEquals("aws-ak", properties.toFileSystemKv().get("OBS_ACCESS_KEY"));
        Assertions.assertEquals("aws-ak", properties.toFileSystemKv().get("AWS_ACCESS_KEY"));
    }

    @Test
    void toBackendProperties_returnsOnlyAwsCompatibleKeysForBeAdapters() {
        ObsFileSystemProperties properties = ObsFileSystemProperties.of(Map.of(
                "obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com",
                "obs.access_key", "obs-ak",
                "obs.secret_key", "obs-sk",
                "OBS_BUCKET", "obs-bucket",
                "OBS_ROLE_ARN", "obs-role"));

        BackendStorageProperties backend = properties.toBackendProperties().orElseThrow();
        Map<String, String> backendMap = backend.toMap();

        Assertions.assertEquals(BackendStorageKind.S3_COMPATIBLE, backend.backendKind());
        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com", backendMap.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-north-4", backendMap.get("AWS_REGION"));
        Assertions.assertEquals("obs-ak", backendMap.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("obs-sk", backendMap.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("obs-bucket", backendMap.get("AWS_BUCKET"));
        Assertions.assertEquals("obs-role", backendMap.get("AWS_ROLE_ARN"));
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(key -> key.startsWith("OBS_")));
    }

    @Test
    void bind_rejectsPartialStaticCredentialsLikeFeCore() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ObsFileSystemProperties.of(Map.of(
                        "obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com",
                        "obs.access_key", "ak")));

        Assertions.assertTrue(exception.getMessage().contains(
                "Both the access key and the secret key must be set"));
    }

    @Test
    void bind_requiresEndpointLikeFeCoreObs() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ObsFileSystemProperties.of(Map.of(
                        "obs.region", "cn-north-4")));

        Assertions.assertTrue(exception.getMessage().contains("Property obs.endpoint is required"));
    }

    @Test
    void provider_bindReturnsObsTypedProperties() throws IOException {
        ObsFileSystemProvider provider = new ObsFileSystemProvider();
        ObsFileSystemProperties properties = provider.bind(Map.of(
                "obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com"));
        FileSystem fileSystem = provider.create(properties);

        Assertions.assertEquals("OBS", properties.providerName());
        Assertions.assertEquals(StorageKind.OBJECT_STORAGE, properties.kind());
        Assertions.assertEquals(FileSystemType.S3, properties.type());
        Assertions.assertInstanceOf(ObsFileSystem.class, fileSystem);
        Assertions.assertEquals(ObjectStorageFileSystem.class, fileSystem.getClass().getSuperclass());
    }

    @Test
    void provider_supportsLegacyObsEndpointKey() {
        ObsFileSystemProvider provider = new ObsFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com")));
    }

    @Test
    void provider_supportsLegacyAwsEndpointKeyForObsDomain() {
        ObsFileSystemProvider provider = new ObsFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "AWS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com")));
    }
}
