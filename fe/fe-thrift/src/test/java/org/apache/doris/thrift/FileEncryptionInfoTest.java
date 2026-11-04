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

package org.apache.doris.thrift;

import com.google.gson.Gson;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

class FileEncryptionInfoTest {
    private static final byte[] KEY = "synthetic-key-12".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AAD = new byte[] {0, (byte) 255, 7, 0};
    private static final byte[] DELETE_KEY = new byte[] {0, (byte) 255, 7, 0, 1, 2, 3, 4,
            5, 6, 7, 8, 9, 10, 11, 12};

    @Test
    void binaryAndCompactKeepIndependentDataAndDeleteKeys() throws Exception {
        for (TProtocolFactory protocol : List.of(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
            TIcebergFileDesc original = descriptor();
            TIcebergFileDesc decoded = new TIcebergFileDesc();
            new TDeserializer(protocol).deserialize(decoded, new TSerializer(protocol).serialize(original));
            Assertions.assertEquals(original, decoded);
            Assertions.assertArrayEquals(KEY, decoded.getEncryptionInfo().getKey());
            Assertions.assertArrayEquals(AAD, decoded.getEncryptionInfo().getAadPrefix());
            Assertions.assertArrayEquals(DELETE_KEY, decoded.getDeleteFiles().get(0).getEncryptionInfo().getKey());
            Assertions.assertFalse(decoded.getDeleteFiles().get(1).isSetEncryptionInfo());
        }
    }

    @Test
    void javaObjectSerializationStillUsesTheExplicitThriftEncoding() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(descriptor());
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assertions.assertEquals(descriptor(), input.readObject());
        }
    }

    @Test
    void absentFieldsRoundTripAsPlaintext() throws Exception {
        TIcebergFileDesc plain = new TIcebergFileDesc().setFormatVersion(3);
        TIcebergFileDesc decoded = new TIcebergFileDesc();
        new TDeserializer().deserialize(decoded, new TSerializer().serialize(plain));
        Assertions.assertFalse(decoded.isSetEncryptionInfo());
    }

    @Test
    void nestedGeneratedStringsNeverPrintSecrets() {
        String diagnostic = descriptor().toString();
        Assertions.assertTrue(diagnostic.contains("<redacted>"), diagnostic);
        Assertions.assertFalse(diagnostic.contains("73 79 6E 74"), diagnostic);
        Assertions.assertFalse(diagnostic.contains("00 FF 07 00"), diagnostic);
    }

    @Test
    void nestedGsonNeverReflectsSecretFields() {
        String diagnostic = new Gson().toJson(Map.of("data", descriptor().getEncryptionInfo(),
                "deletes", List.of(descriptor().getDeleteFiles().get(0).getEncryptionInfo())));
        Assertions.assertFalse(diagnostic.contains("synthetic-key"), diagnostic);
        Assertions.assertFalse(diagnostic.contains("key"), diagnostic);
        Assertions.assertFalse(diagnostic.contains("aad_prefix"), diagnostic);
    }

    @Test
    void diagnosticThriftProtocolsAreLossyAndRedacted() throws Exception {
        for (TProtocolFactory protocol : List.of(new TJSONProtocol.Factory(), new TSimpleJSONProtocol.Factory())) {
            String diagnostic = new String(new TSerializer(protocol).serialize(descriptor()), StandardCharsets.UTF_8);
            Assertions.assertTrue(diagnostic.contains("redacted"), diagnostic);
            for (byte[] secret : List.of(KEY, AAD, DELETE_KEY)) {
                Assertions.assertFalse(diagnostic.contains(
                        Base64.getEncoder().withoutPadding().encodeToString(secret)), diagnostic);
            }
        }
    }

    private static TIcebergFileDesc descriptor() {
        return new TIcebergFileDesc().setFormatVersion(3)
                .setEncryptionInfo(new TFileEncryptionInfo().setVersion(1).setKey(KEY).setAadPrefix(AAD))
                .setDeleteFiles(List.of(new TIcebergDeleteFileDesc().setEncryptionInfo(
                        new TFileEncryptionInfo().setVersion(1).setKey(DELETE_KEY).setAadPrefix(KEY)),
                        new TIcebergDeleteFileDesc()));
    }
}
