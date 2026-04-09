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

package org.apache.doris.filesystem.local;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for the local filesystem (file:// and local:// URIs).
 * For unit-testing only — not intended for production use.
 */
public class LocalFileSystemProvider implements FileSystemProvider {

    @Override
    public boolean supports(Map<String, String> properties) {
        String uri = properties.getOrDefault("uri", "");
        return uri.startsWith("file://") || uri.startsWith("local://");
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return new LocalFileSystem(properties);
    }

    @Override
    public String name() {
        return "LOCAL";
    }
}
