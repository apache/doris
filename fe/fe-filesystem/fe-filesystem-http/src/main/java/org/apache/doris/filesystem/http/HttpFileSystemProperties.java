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
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Typed properties for HTTP storage. HTTP is not a real filesystem: reads happen via the
 * http() TVF on FE (HttpUtils) and TFileType.FILE_HTTP on BE. This class only validates the URI,
 * extracts http.header.* headers, and passes the raw map through for BE backend config.
 */
public class HttpFileSystemProperties implements FileSystemProperties {

    public static final String URI_KEY = "uri";
    public static final String HEADER_PREFIX = "http.header.";

    private final Map<String, String> rawProperties;
    private final String uri;
    private final Map<String, String> headers;
    private final Map<String, String> matchedProperties;

    private HttpFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.uri = validateUri(rawProperties.get(URI_KEY));
        Map<String, String> hdrs = new LinkedHashMap<>();
        Map<String, String> matched = new LinkedHashMap<>();
        matched.put(URI_KEY, this.uri);
        for (Map.Entry<String, String> e : rawProperties.entrySet()) {
            if (e.getKey().toLowerCase().startsWith(HEADER_PREFIX)) {
                hdrs.put(e.getKey().substring(HEADER_PREFIX.length()), e.getValue());
                matched.put(e.getKey(), e.getValue());
            }
        }
        this.headers = Collections.unmodifiableMap(hdrs);
        this.matchedProperties = Collections.unmodifiableMap(matched);
    }

    public static HttpFileSystemProperties of(Map<String, String> properties) {
        return new HttpFileSystemProperties(properties);
    }

    private static String validateUri(String url) {
        if (url == null
                || (!url.startsWith("http://") && !url.startsWith("https://") && !url.startsWith("hf://"))) {
            throw new IllegalArgumentException("Invalid http/hf url: " + url);
        }
        return url;
    }

    public String getUri() {
        return uri;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String providerName() {
        return "HTTP";
    }

    @Override
    public StorageKind kind() {
        return StorageKind.HTTP;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.HTTP;
    }

    @Override
    public Map<String, String> rawProperties() {
        return rawProperties;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return matchedProperties;
    }
}
