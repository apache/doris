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

package org.apache.doris.filesystem.http;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HttpFileSystemPropertiesTest {

    private Map<String, String> props(String uri) {
        Map<String, String> m = new HashMap<>();
        m.put("uri", uri);
        return m;
    }

    @Test
    public void testAcceptsHttpHttpsHfSchemes() {
        Assertions.assertEquals("http://h/a.csv", HttpFileSystemProperties.of(props("http://h/a.csv")).getUri());
        Assertions.assertEquals("https://h/a.csv", HttpFileSystemProperties.of(props("https://h/a.csv")).getUri());
        Assertions.assertEquals("hf://ds/a", HttpFileSystemProperties.of(props("hf://ds/a")).getUri());
    }

    @Test
    public void testRejectsOtherSchemes() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HttpFileSystemProperties.of(props("s3://b/k")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HttpFileSystemProperties.of(props(null)));
    }

    @Test
    public void testExtractsHeaders() {
        Map<String, String> m = props("http://h/a.csv");
        m.put("http.header.Authorization", "Bearer x");
        m.put("http.header.Accept", "text/csv");
        m.put("format", "csv");
        Map<String, String> headers = HttpFileSystemProperties.of(m).getHeaders();
        Assertions.assertEquals(2, headers.size());
        Assertions.assertEquals("Bearer x", headers.get("Authorization"));
        Assertions.assertEquals("text/csv", headers.get("Accept"));
    }

    @Test
    public void testTypeAndKindAndRaw() {
        Map<String, String> m = props("http://h/a.csv");
        HttpFileSystemProperties p = HttpFileSystemProperties.of(m);
        Assertions.assertEquals("HTTP", p.providerName());
        Assertions.assertEquals(FileSystemType.HTTP, p.type());
        Assertions.assertEquals(StorageKind.HTTP, p.kind());
        Assertions.assertEquals("http://h/a.csv", p.rawProperties().get("uri"));
        Assertions.assertTrue(p.matchedProperties().containsKey("uri"));
    }
}
