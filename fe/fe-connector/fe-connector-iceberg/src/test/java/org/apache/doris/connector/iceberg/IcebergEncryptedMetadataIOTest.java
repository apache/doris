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

package org.apache.doris.connector.iceberg;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class IcebergEncryptedMetadataIOTest {
    private static final byte[] CONTENT = "synthetic encrypted metadata".getBytes(StandardCharsets.UTF_8);

    @Test
    void authenticatedFileIOPreservesEncryptedDataAndDeleteDescriptors() throws Exception {
        try (Fixture fixture = new Fixture()) {
            NativeEncryptionOutputFile encrypted = fixture.writeEncrypted();
            long physicalLength = fixture.raw.newInputFile(encrypted.encryptingOutputFile().location()).getLength();
            DataFile data = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(encrypted.encryptingOutputFile().location()).withFormat("parquet")
                    .withFileSizeInBytes(physicalLength).withRecordCount(1)
                    .withEncryptionKeyMetadata(encrypted.keyMetadata().copyWithLength(physicalLength)).build();
            DeleteFile delete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned()).ofPositionDeletes()
                    .withPath(data.location()).withFormat("parquet").withFileSizeInBytes(physicalLength)
                    .withRecordCount(1)
                    .withEncryptionKeyMetadata(encrypted.keyMetadata().copyWithLength(physicalLength)).build();

            fixture.assertContent(fixture.authenticated.newInputFile(data));
            fixture.assertContent(fixture.authenticated.newInputFile(delete));
            Assertions.assertEquals(2, fixture.auth.calls);
        }
    }

    @Test
    void authenticatedFileIOPreservesEncryptedManifestDescriptors() throws Exception {
        try (Fixture fixture = new Fixture()) {
            NativeEncryptionOutputFile encrypted = fixture.encryptNewFile();
            ManifestWriter<DataFile> writer = ManifestFiles.write(
                    3, PartitionSpec.unpartitioned(), (EncryptedOutputFile) encrypted, 1L);
            DataFile data = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("memory://fixture/data.parquet")
                    .withFileSizeInBytes(32).withRecordCount(1).build();
            try (ManifestWriter<DataFile> closeable = writer) {
                closeable.add(data);
            }
            ManifestFile manifest = writer.toManifestFile();
            Assertions.assertNotNull(manifest.keyMetadata());
            try (ManifestReader<DataFile> reader = ManifestFiles.read(
                    manifest, fixture.authenticated, Map.of(0, PartitionSpec.unpartitioned()))) {
                Assertions.assertEquals(data.location(), reader.iterator().next().location());
            }
            Assertions.assertTrue(fixture.auth.calls > 0);
        }
    }

    @Test
    void authenticatedFileIOPreservesEncryptedManifestListKeyChain() throws Exception {
        try (Fixture fixture = new Fixture()) {
            NativeEncryptionOutputFile encrypted = fixture.writeEncrypted();
            long physicalLength = fixture.raw.newInputFile(encrypted.encryptingOutputFile().location()).getLength();
            String keyId = fixture.manager.registerKeyMetadata(
                    encrypted.keyMetadata().copyWithLength(physicalLength)).fileKey().keyId();
            ManifestListFile manifestList = new ManifestListFile() {
                @Override
                public String location() {
                    return encrypted.encryptingOutputFile().location();
                }

                @Override
                public String encryptionKeyID() {
                    return keyId;
                }

                @Override
                public ByteBuffer decryptKeyMetadata(EncryptionManager manager) {
                    return EncryptionUtil.decryptManifestListKeyMetadata(this, manager);
                }
            };
            fixture.assertContent(fixture.authenticated.newInputFile(manifestList));
            Assertions.assertEquals(1, fixture.auth.calls);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final InMemoryFileIO raw = new InMemoryFileIO();
        private final List<String> paths = new ArrayList<>();
        private final RecordingAuthenticator auth = new RecordingAuthenticator();
        private final StandardEncryptionManager manager = new StandardEncryptionManager(
                List.of(), "fixture-key", 16, new FixtureKms());
        private final FileIO authenticated = new IcebergAuthenticatedFileIO(EncryptingFileIO.combine(raw, manager), auth);

        private NativeEncryptionOutputFile encryptNewFile() {
            String path = "memory://encryption-fixture/" + UUID.randomUUID() + ".avro";
            paths.add(path);
            return manager.encrypt(raw.newOutputFile(path));
        }

        private NativeEncryptionOutputFile writeEncrypted() throws IOException {
            NativeEncryptionOutputFile encrypted = encryptNewFile();
            try (PositionOutputStream output = encrypted.encryptingOutputFile().create()) {
                output.write(CONTENT);
            }
            return encrypted;
        }

        private void assertContent(InputFile input) throws IOException {
            try (java.io.InputStream stream = input.newStream()) {
                Assertions.assertArrayEquals(CONTENT, stream.readAllBytes());
            }
        }

        @Override
        public void close() {
            for (String path : paths) {
                raw.deleteFile(path);
            }
            raw.close();
        }
    }

    private static final class RecordingAuthenticator implements HadoopAuthenticator {
        private int calls;

        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("Direct test authenticator");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            calls++;
            try {
                return action.run();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    // This local KMS double isolates SDK envelope processing; the AWS client has separate HTTP coverage.
    private static final class FixtureKms implements KeyManagementClient {
        @Override
        public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
            return key.duplicate();
        }

        @Override
        public ByteBuffer unwrapKey(ByteBuffer key, String wrappingKeyId) {
            return key.duplicate();
        }

        @Override
        public void initialize(Map<String, String> properties) {
        }
    }
}
