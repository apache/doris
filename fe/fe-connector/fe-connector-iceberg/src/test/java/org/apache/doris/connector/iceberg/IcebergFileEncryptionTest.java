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

import org.apache.doris.thrift.TFileEncryptionInfo;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class IcebergFileEncryptionTest {
    @ParameterizedTest
    @ValueSource(ints = {16, 24, 32})
    void matchesSdkCodecAndOwnsSplitBuffers(int keyLength) {
        StandardEncryptionManager manager = new StandardEncryptionManager(
                List.of(), "synthetic-key", keyLength, new UnusedKms());
        NativeEncryptionKeyMetadata metadata = manager.encrypt(
                new InMemoryFileIO().newOutputFile("memory://fixture/data.parquet")).keyMetadata();
        for (NativeEncryptionKeyMetadata source : List.of(metadata, metadata.copyWithLength(4096))) {
            ByteBuffer encoded = source.buffer();
            ByteBuffer direct = ByteBuffer.allocateDirect(encoded.remaining() + 8);
            direct.position(3);
            direct.put(encoded.duplicate()).flip().position(3);
            IcebergFileEncryption encryption = IcebergFileEncryption.fromMetadata(direct.asReadOnlyBuffer());
            TFileEncryptionInfo first = encryption.toThrift();
            Assertions.assertEquals(1, first.getVersion());
            Assertions.assertArrayEquals(ByteBuffers.toByteArray(source.encryptionKey()), first.getKey());
            Assertions.assertArrayEquals(ByteBuffers.toByteArray(source.aadPrefix()), first.getAadPrefix());
            Arrays.fill(first.getKey(), (byte) 0);
            Arrays.fill(first.getAadPrefix(), (byte) 0);
            Assertions.assertArrayEquals(ByteBuffers.toByteArray(source.encryptionKey()), encryption.toThrift().getKey());
            Assertions.assertArrayEquals(ByteBuffers.toByteArray(source.aadPrefix()), encryption.toThrift().getAadPrefix());
            Assertions.assertEquals(3, direct.position());
        }
    }

    @Test
    void absentMetadataIsPlaintextButEmptyUnknownAndCorruptMetadataFail() throws IOException {
        Assertions.assertNull(IcebergFileEncryption.fromMetadata(null));
        for (byte[] bytes : List.of(new byte[0], new byte[] {1}, new byte[] {1, 32},
                encoded(15, 1, 1), encoded(16, 0, 0), encoded(16, 1, Integer.MAX_VALUE), new byte[65537])) {
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> IcebergFileEncryption.fromMetadata(ByteBuffer.wrap(bytes)));
            Assertions.assertEquals("Iceberg encryption [INVALID_KEY_METADATA]", error.getMessage());
            Assertions.assertNull(error.getCause());
        }
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergFileEncryption.fromMetadata(ByteBuffer.wrap(new byte[] {99})));
        Assertions.assertEquals("Iceberg encryption [UNSUPPORTED_KEY_METADATA_VERSION]", error.getMessage());
    }

    private static byte[] encoded(int keyLength, int aadBranch, long aadLength) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(1);
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(bytes, null);
        encoder.writeBytes(new byte[keyLength]);
        encoder.writeIndex(aadBranch);
        if (aadBranch == 1) {
            encoder.writeLong(aadLength);
        }
        encoder.flush();
        return bytes.toByteArray();
    }

    private static class UnusedKms implements KeyManagementClient {
        @Override
        public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
            throw new AssertionError("Per-file metadata does not require KMS wrapping");
        }

        @Override
        public ByteBuffer unwrapKey(ByteBuffer key, String wrappingKeyId) {
            throw new AssertionError("Manifest processing already unwrapped the file metadata");
        }

        @Override
        public void initialize(Map<String, String> properties) {
        }
    }
}
