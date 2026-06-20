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

package org.apache.doris.connector.metastore.spi.fs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** T2 parity for the filesystem backend (legacy {@code PaimonFileSystemMetaStoreProperties}). */
public class FileSystemMetaStorePropertiesTest {

    @Test
    public void carriesWarehouseAndDeclaresStorageNeeded() {
        Map<String, String> raw = new HashMap<>();
        raw.put("warehouse", "oss://bucket/wh");
        FileSystemMetaStorePropertiesImpl props = FileSystemMetaStorePropertiesImpl.of(raw);

        Assertions.assertEquals("FILESYSTEM", props.providerName());
        Assertions.assertEquals("oss://bucket/wh", props.getWarehouse());
        Assertions.assertTrue(props.needsStorage());
        props.validate(); // no throw
        Assertions.assertEquals("oss://bucket/wh", props.matchedProperties().get("warehouse"));
        Assertions.assertEquals(raw, props.rawProperties());
    }

    @Test
    public void validateRequiresWarehouse() {
        FileSystemMetaStorePropertiesImpl props = FileSystemMetaStorePropertiesImpl.of(new HashMap<>());
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, props::validate);
        Assertions.assertEquals("Property warehouse is required.", ex.getMessage());
    }
}
