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

import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IcebergSystemTableSerializationTest {
    @Test
    public void testGcsFileIoSerializationKeepsOnlyStorageToken() {
        GCSFileIO io = new GCSFileIO();
        io.initialize(Map.of("gcs.oauth2.token", "storage-token",
                "gcs.oauth2.token-expires-at", "4102444800000", "gcs.oauth2.refresh-credentials-enabled", "false",
                "token", "catalog-secret", "doris.vended-credentials.context-id", "fe-context",
                "doris.vended-credentials.session.credential.token", "user-secret"));
        String serialized = IcebergSystemTableSerialization.serializeToBase64(List.of(io));
        List<FileIO> restored = IcebergSystemTableSerialization.deserializeFromBase64(serialized);
        Map<String, String> properties = restored.get(0).properties();
        Assertions.assertEquals("storage-token", properties.get("gcs.oauth2.token"));
        Assertions.assertEquals("false", properties.get("gcs.oauth2.refresh-credentials-enabled"));
        Assertions.assertFalse(properties.containsKey("token"));
        Assertions.assertFalse(properties.keySet().stream().anyMatch(key -> key.startsWith("doris.vended-credentials.")));
        restored.forEach(FileIO::close);
        io.close();
    }
}
