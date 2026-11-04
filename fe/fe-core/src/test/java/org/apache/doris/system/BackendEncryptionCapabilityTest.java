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

package org.apache.doris.system;

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BackendEncryptionCapabilityTest {
    @Test
    void absentOrUnsupportedProtocolCannotAdvertiseAnExecutableReader() {
        for (String state : new String[] {"{}", "{\"fileEncryptionCapabilities\":{\"readerSupported\":true}}",
                "{\"fileEncryptionCapabilities\":{\"version\":9,\"readerSupported\":true}}"}) {
            Backend backend = GsonUtils.GSON.fromJson(state, Backend.class);
            Assertions.assertFalse(backend.getConnectorCapabilities().contains("iceberg-pme-reader-v1"));
        }
    }

    @Test
    void protocolRecognitionDoesNotMeanNativeReaderSupport() {
        Backend protocolOnly = GsonUtils.GSON.fromJson("{\"fileEncryptionCapabilities\":{\"version\":1}}", Backend.class);
        Assertions.assertTrue(protocolOnly.getConnectorCapabilities().contains("file-encryption-v1"));
        Assertions.assertFalse(protocolOnly.getConnectorCapabilities().contains("iceberg-pme-reader-v1"));
        Backend capable = GsonUtils.GSON.fromJson(
                "{\"fileEncryptionCapabilities\":{\"version\":1,\"readerSupported\":true}}", Backend.class);
        Assertions.assertTrue(capable.getConnectorCapabilities().contains("iceberg-pme-reader-v1"));
    }

    @Test
    void legacyHeartbeatRevokesPreviouslyAdvertisedCapabilities() {
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        BackendHbResponse capable = new BackendHbResponse(1, 9060, 8040, 8060, 1, 1,
                "fixture", Tag.VALUE_MIX, 0, 0, false, 0);
        capable.setFileEncryptionProtocolVersion(1);
        capable.setIcebergPmeReaderSupported(true);
        backend.handleHbResponse(capable, true);
        Assertions.assertTrue(backend.getConnectorCapabilities().contains("iceberg-pme-reader-v1"));
        BackendHbResponse legacy = new BackendHbResponse(1, 9060, 8040, 8060, 2, 2,
                "fixture", Tag.VALUE_MIX, 0, 0, false, 0);
        backend.handleHbResponse(legacy, true);
        Assertions.assertFalse(backend.getConnectorCapabilities().contains("file-encryption-v1"));
        Assertions.assertFalse(backend.getConnectorCapabilities().contains("iceberg-pme-reader-v1"));
    }
}
