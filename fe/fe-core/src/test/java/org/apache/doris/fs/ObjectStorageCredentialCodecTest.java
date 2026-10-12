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

import org.apache.doris.cloud.proto.Cloud.ObjectStoreCredentialPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.datasource.storage.ObjectStorageCredentialAdapter;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.filesystem.spi.ObjectStorageCredentialCodec;
import org.apache.doris.foundation.property.StoragePropertiesException;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

class ObjectStorageCredentialCodecTest {
    @AfterEach
    void restoreBuiltinRegistry() {
        StorageAdapter.initPluginManager(null);
    }

    @Test
    void registeredPluginOwnsTheWireFieldsAndPropertyMapping() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        RecordingCodec codec = new RecordingCodec();
        manager.registerProvider(provider("external-codec-plugin", codec));
        StorageAdapter.initPluginManager(manager);
        Assertions.assertSame(codec, StorageAdapter.credentialCodec("oss"));
        Map<String, String> fields = Map.of("plugin-owned-field", "value");
        ObjectStoreInfoPB.Builder builder = ObjectStoreInfoPB.newBuilder();
        ObjectStorageCredentialAdapter.fromCredential("OSS", fields).applyTo(builder);
        Assertions.assertEquals(ObjectStoreInfoPB.Provider.OSS, builder.getProvider());
        Assertions.assertEquals(fields, codec.fields);
        Assertions.assertEquals(codec.envelope, builder.getCredential());
        Assertions.assertEquals(Map.of("plugin.property", "restored"),
                ObjectStorageCredentialAdapter.toProperties("OSS", builder.getCredential()));
    }

    @Test
    void missingOrAmbiguousCodecFailsWithoutFallingBackToAnotherProvider() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        StorageAdapter.initPluginManager(manager);
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageAdapter.credentialCodec("OSS"));
        manager.registerProvider(provider("first", new RecordingCodec()));
        manager.registerProvider(provider("second", new RecordingCodec()));
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageAdapter.credentialCodec("OSS"));
    }

    private FileSystemProvider<FileSystemProperties> provider(String name, ObjectStorageCredentialCodec codec) {
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
            public Optional<ObjectStorageCredentialCodec> credentialCodec() {
                return Optional.of(codec);
            }
        };
    }

    private static class RecordingCodec implements ObjectStorageCredentialCodec {
        // The kernel must transfer even fields it does not interpret.
        private final ObjectStoreCredentialPB envelope = ObjectStoreCredentialPB.newBuilder()
                .setUnknownFields(UnknownFieldSet.newBuilder().addField(100,
                        UnknownFieldSet.Field.newBuilder().addVarint(42).build()).build()).build();
        private Map<String, String> fields;

        @Override
        public String provider() {
            return "OSS";
        }

        @Override
        public byte[] toCloudCredential(Map<String, String> credential) {
            fields = credential;
            return envelope.toByteArray();
        }

        @Override
        public byte[] toThriftCredential(Map<String, String> credential) {
            throw new UnsupportedOperationException("This fixture only supplies Cloud credentials");
        }

        @Override
        public Map<String, String> toProperties(byte[] cloudCredential) {
            Assertions.assertArrayEquals(envelope.toByteArray(), cloudCredential);
            return Map.of("plugin.property", "restored");
        }
    }
}
