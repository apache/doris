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

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.datasource.storage.ObjectStorageCredentialAdapter;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.auth.GcpCredentialSpec;
import org.apache.doris.filesystem.auth.NativeObjectStorageCredential;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.thrift.TObjStorageType;
import org.apache.doris.thrift.TS3StorageParam;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

class ObjectStorageCredentialAdapterTest {
    @AfterEach
    void restoreBuiltinRegistry() {
        StorageAdapter.initPluginManager(null);
    }

    @Test
    void hostTranslatesTypedCredentialsAndPluginOwnsPropertyNames() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(provider("GCS"));
        StorageAdapter.initPluginManager(manager);
        GcpCredentialSpec spec = new GcpCredentialSpec(GcpCredentialSpec.ProviderType.COMPUTE_ENGINE, null);
        ObjectStorageCredentialAdapter adapter = ObjectStorageCredentialAdapter.fromCredential(spec);
        ObjectStoreInfoPB.Builder cloud = ObjectStoreInfoPB.newBuilder();
        adapter.applyTo(cloud);
        Assertions.assertEquals(ObjectStoreInfoPB.Provider.GCP, cloud.getProvider());
        Assertions.assertEquals("COMPUTE_ENGINE", cloud.getCredential().getGcpCredential()
                .getCredentialProviderType().name());
        TS3StorageParam thrift = new TS3StorageParam();
        adapter.applyTo(thrift);
        Assertions.assertEquals(TObjStorageType.GCP, thrift.getProvider());
        Assertions.assertEquals("COMPUTE_ENGINE", thrift.getCredential().getGcpCredential()
                .getCredentialProviderType().name());
        Assertions.assertEquals(Map.of("plugin.identity", "COMPUTE_ENGINE"),
                ObjectStorageCredentialAdapter.toProperties("GCP", cloud.getCredential()));
    }

    @Test
    void alterDoesNotInventSourceOrDropExplicitClear() {
        ObjectStoreInfoPB.Builder cloud = ObjectStoreInfoPB.newBuilder();
        TS3StorageParam thrift = new TS3StorageParam();
        ObjectStorageCredentialAdapter adapter = ObjectStorageCredentialAdapter.fromCredential(
                new GcpCredentialSpec(null, ""));
        adapter.applyTo(cloud);
        adapter.applyTo(thrift);
        Assertions.assertFalse(cloud.getCredential().getGcpCredential().hasCredentialProviderType());
        Assertions.assertFalse(thrift.getCredential().getGcpCredential().isSetCredentialProviderType());
        Assertions.assertTrue(cloud.getCredential().getGcpCredential().hasImpersonationServiceAccount());
        Assertions.assertTrue(thrift.getCredential().getGcpCredential().isSetImpersonationServiceAccount());
        Assertions.assertEquals("", cloud.getCredential().getGcpCredential().getImpersonationServiceAccount());
        Assertions.assertEquals("", thrift.getCredential().getGcpCredential().getImpersonationServiceAccount());
    }

    @Test
    void missingOrAmbiguousPropertyOwnerFailsExplicitly() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        StorageAdapter.initPluginManager(manager);
        GcpCredentialSpec spec = new GcpCredentialSpec(GcpCredentialSpec.ProviderType.DEFAULT, null);
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageAdapter.credentialToProperties(spec));
        manager.registerProvider(provider("first"));
        manager.registerProvider(provider("second"));
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageAdapter.credentialToProperties(spec));
    }

    private FileSystemProvider<FileSystemProperties> provider(String name) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean supports(Map<String, String> properties) {
                return false;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                throw new UnsupportedOperationException("No filesystem I/O in credential conversion");
            }

            @Override
            public Optional<Map<String, String>> credentialToProperties(NativeObjectStorageCredential credential) {
                GcpCredentialSpec spec = (GcpCredentialSpec) credential;
                return Optional.of(Map.of("plugin.identity", spec.getProviderType().orElseThrow().name()));
            }
        };
    }
}
