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

import org.apache.doris.filesystem.properties.FileSystemCapability;
import org.apache.doris.filesystem.properties.FileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class HttpFileSystemProviderTest {

    private final HttpFileSystemProvider provider = new HttpFileSystemProvider();

    private Map<String, String> props(String key, String value) {
        Map<String, String> m = new HashMap<>();
        m.put(key, value);
        return m;
    }

    @Test
    public void testSupportsByScheme() {
        Assertions.assertTrue(provider.supports(props("uri", "http://h/a.csv")));
        Assertions.assertTrue(provider.supports(props("uri", "https://h/a.csv")));
        Assertions.assertTrue(provider.supports(props("uri", "hf://ds/a")));
        Assertions.assertFalse(provider.supports(props("uri", "s3://b/k")));
        Assertions.assertFalse(provider.supports(new HashMap<>()));
    }

    @Test
    public void testSupportsByMarker() {
        Assertions.assertTrue(provider.supports(props("_STORAGE_TYPE_", "HTTP")));
    }

    @Test
    public void testBindReturnsHttpProperties() {
        Map<String, String> m = props("uri", "http://h/a.csv");
        m.put("http.header.Accept", "text/csv");
        FileSystemProperties fp = provider.bind(m);
        Assertions.assertTrue(fp instanceof HttpFileSystemProperties);
        Assertions.assertEquals("http://h/a.csv", ((HttpFileSystemProperties) fp).getUri());
        Assertions.assertEquals("text/csv", ((HttpFileSystemProperties) fp).getHeaders().get("Accept"));
    }

    @Test
    public void testCreateThrows() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> provider.create(props("uri", "http://h/a.csv")));
    }

    @Test
    public void testCapabilitiesReadOnly() {
        HttpFileSystemProperties bound = provider.bind(props("uri", "http://h/a.csv"));
        Assertions.assertEquals(EnumSet.of(FileSystemCapability.READ), provider.capabilities(bound));
    }

    @Test
    public void testName() {
        Assertions.assertEquals("HTTP", provider.name());
    }
}
