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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core HttpProperties behaviour. */
public class HttpPropertiesParityTest {

    @Test
    public void testIdentityHeadersAndUri() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("fs.http.support", "true");
        props.put("uri", "https://example.com/data.csv");
        props.put("http.header.Authorization", "Bearer tok");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertTrue(sp instanceof HttpProperties);
        Assertions.assertEquals(StorageProperties.Type.HTTP, sp.getType());
        // 2.4-1: lowercase "http"
        Assertions.assertEquals("http", sp.getStorageName());
        Assertions.assertNull(sp.getHadoopStorageConfig());
        Assertions.assertEquals(props, sp.getBackendConfigProperties());
        HttpProperties hp = (HttpProperties) sp;
        ParityAsserts.assertExactMap(ParityAsserts.map("Authorization", "Bearer tok"), hp.getHeaders());
        Assertions.assertEquals("https://example.com/data.csv", sp.validateAndGetUri(props));
        // hf:// is accepted, plain ftp:// is not
        Assertions.assertEquals("hf://ds/f.parquet", sp.validateAndNormalizeUri("hf://ds/f.parquet"));
        Assertions.assertThrows(UserException.class, () -> sp.validateAndNormalizeUri("ftp://x/f"));
    }
}
