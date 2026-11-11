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

package org.apache.doris.filesystem.gcs;

import org.apache.doris.filesystem.auth.GcpCredentialSpec;
import org.apache.doris.filesystem.auth.NativeObjectStorageCredential;
import org.apache.doris.filesystem.auth.ObjectStorageAuthentication;
import org.apache.doris.filesystem.gcs.auth.GcsAuthResolver;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

class GcsCredentialDescriptionTest {
    private final GcsFileSystemProvider provider = new GcsFileSystemProvider();

    @Test
    void nativeIdentityRoundTripsThroughUserPropertiesWithoutWireTypes() {
        for (GcpCredentialSpec.ProviderType type : GcpCredentialSpec.ProviderType.values()) {
            Map<String, String> properties = Map.of("gs.credential_provider_type", type.name(),
                    "gs.impersonation_service_account", "target@test.iam.gserviceaccount.com");
            ObjectStorageAuthentication auth = provider.resolveAuthentication(properties).orElseThrow();
            GcpCredentialSpec expected = new GcpCredentialSpec(type, "target@test.iam.gserviceaccount.com");
            Assertions.assertEquals(expected, auth.getCredential().orElseThrow());
            Assertions.assertEquals(expected, auth.getCredentialUpdates().orElseThrow());
            Assertions.assertEquals(properties, provider.credentialToProperties(expected).orElseThrow());
        }
    }

    @Test
    void clearingImpersonationDoesNotAddProviderTypeToAlter() {
        ObjectStorageAuthentication auth = provider.resolveAuthentication(Map.of(
                "provider", "GCP", "gs.impersonation_service_account", "")).orElseThrow();
        Assertions.assertEquals(new GcpCredentialSpec(GcpCredentialSpec.ProviderType.DEFAULT, null),
                auth.getCredential().orElseThrow());
        Assertions.assertEquals(new GcpCredentialSpec(null, ""), auth.getCredentialUpdates().orElseThrow());
    }

    @Test
    void onlyGcpDescriptionsAreRestoredAndAbsentSourceUsesLegacyDefault() {
        Assertions.assertEquals(Map.of("gs.credential_provider_type", "DEFAULT"),
                provider.credentialToProperties(new GcpCredentialSpec(null, null)).orElseThrow());
        NativeObjectStorageCredential foreign = new NativeObjectStorageCredential() {
            @Override
            public String getProvider() {
                return "OTHER";
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        };
        Assertions.assertTrue(provider.credentialToProperties(foreign).isEmpty());
    }

    @Test
    void pluginCrossesTheSharedApiWithoutAccessToHostWireClasses() throws Exception {
        URL[] urls = {GcsFileSystemProvider.class.getProtectionDomain().getCodeSource().getLocation(),
                GcsAuthResolver.class.getProtectionDomain().getCodeSource().getLocation()};
        try (URLClassLoader loader = new URLClassLoader(urls, getClass().getClassLoader()) {
            @Override
            protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.apache.doris.thrift.") || name.startsWith("org.apache.doris.cloud.proto.")
                        || name.startsWith("org.apache.doris.storage.protocol.")) {
                    throw new ClassNotFoundException("Protocol classes are unavailable to this plugin: " + name);
                }
                if (name.startsWith("org.apache.doris.filesystem.gcs.")) {
                    Class<?> loaded = findLoadedClass(name);
                    if (loaded == null) {
                        loaded = findClass(name);
                    }
                    if (resolve) {
                        resolveClass(loaded);
                    }
                    return loaded;
                }
                return super.loadClass(name, resolve);
            }
        }) {
            FileSystemProvider<?> isolated = (FileSystemProvider<?>) loader.loadClass(
                    GcsFileSystemProvider.class.getName()).getConstructor().newInstance();
            Assertions.assertNotSame(GcsFileSystemProvider.class, isolated.getClass());
            NativeObjectStorageCredential credential = isolated.resolveAuthentication(
                    Map.of("provider", "GCP")).orElseThrow().getCredential().orElseThrow();
            Assertions.assertEquals(GcpCredentialSpec.class, credential.getClass());
            Assertions.assertEquals(Map.of("gs.credential_provider_type", "DEFAULT"),
                    isolated.credentialToProperties(credential).orElseThrow());
        }
    }

    @Test
    void hmacAndAnonymousAuthenticationDoNotProduceNativeCredentials() {
        for (Map<String, String> properties : java.util.List.of(
                Map.of("provider", "GCP", "s3.access_key", "ak", "s3.secret_key", "sk"),
                Map.of("provider", "GCP", "gs.credential_provider_type", "ANONYMOUS"))) {
            ObjectStorageAuthentication auth = provider.resolveAuthentication(properties).orElseThrow();
            Assertions.assertFalse(auth.isNative());
            Assertions.assertTrue(auth.getCredential().isEmpty());
            Assertions.assertTrue(auth.getCredentialUpdates().isEmpty());
        }
    }
}
