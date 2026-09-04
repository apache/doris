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

package org.apache.doris.pluginapiversion.testplugins;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.util.Map;

/**
 * A filesystem provider whose class bytes are copied into a temporary plugin jar. Kept out of
 * {@code org.apache.doris.filesystem.} for the reason spelled out in {@link VersionProbeConnectorProvider}:
 * a parent-first provider is never read from the plugin jar, so it could not carry a declared version.
 */
public class VersionProbeFileSystemProvider implements FileSystemProvider<FileSystemProperties> {

    @Override
    public String name() {
        return "version_probe_fs";
    }

    @Override
    public boolean supports(Map<String, String> properties) {
        return false;
    }

    @Override
    public FileSystem create(Map<String, String> properties) {
        throw new UnsupportedOperationException(
                "version_probe_fs exists to be admitted or refused at load time; it never opens a filesystem");
    }
}
