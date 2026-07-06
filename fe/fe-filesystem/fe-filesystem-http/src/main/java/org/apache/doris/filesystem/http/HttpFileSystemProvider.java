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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemCapability;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for HTTP storage (http://, https://, hf:// URIs).
 *
 * <p>HTTP has no FileSystem implementation: the http() TVF reads via HttpUtils on FE and
 * TFileType.FILE_HTTP on BE. This provider only binds and validates properties; {@link #create}
 * throws. Use {@link #capabilities(HttpFileSystemProperties)} (READ) to gate operations
 * instead of calling create().
 */
public class HttpFileSystemProvider implements FileSystemProvider<HttpFileSystemProperties> {

    @Override
    public boolean supports(Map<String, String> properties) {
        if ("HTTP".equalsIgnoreCase(properties.get("_STORAGE_TYPE_"))) {
            return true;
        }
        String uri = properties.getOrDefault("uri", "");
        return uri.startsWith("http://") || uri.startsWith("https://") || uri.startsWith("hf://");
    }

    @Override
    public HttpFileSystemProperties bind(Map<String, String> properties) {
        return HttpFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(HttpFileSystemProperties properties) {
        throw new UnsupportedOperationException(
                "HTTP has no FileSystem; read via the http() TVF (FE HttpUtils / BE FILE_HTTP).");
    }

    @Override
    public FileSystem create(Map<String, String> properties) {
        throw new UnsupportedOperationException(
                "HTTP has no FileSystem; read via the http() TVF (FE HttpUtils / BE FILE_HTTP).");
    }

    @Override
    public Set<FileSystemCapability> capabilities(HttpFileSystemProperties boundProperties) {
        return EnumSet.of(FileSystemCapability.READ);
    }

    @Override
    public String name() {
        return "HTTP";
    }
}
