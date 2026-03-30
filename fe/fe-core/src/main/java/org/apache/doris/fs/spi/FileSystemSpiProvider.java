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

package org.apache.doris.fs.spi;

import org.apache.doris.fs.FileSystem;

import java.io.IOException;
import java.util.Map;

/**
 * Service Provider Interface (SPI) for filesystem implementations.
 *
 * This interface has zero dependencies outside JDK and FileSystem (which will
 * move to fe-filesystem-spi). Storage implementation modules register their
 * implementations via Java ServiceLoader.
 *
 * In Phase 3, this interface (along with FileSystem) will be moved to the
 * fe-filesystem-spi Maven module. Each implementation module will provide:
 * META-INF/services/org.apache.doris.filesystem.spi.FileSystemSpiProvider
 *
 * Phase 0: Only skeleton definition. Not yet wired into FileSystemFactory.
 */
public interface FileSystemSpiProvider {
    /**
     * Returns the human-readable name of this storage implementation (for logging).
     */
    String storageName();

    /**
     * Returns true if this provider can create a FileSystem from the given properties.
     * Must be fast (no I/O, no network calls).
     *
     * @param properties storage configuration key-value map, never null
     */
    boolean supports(Map<String, String> properties);

    /**
     * Creates and returns a new FileSystem instance for the given properties.
     *
     * @param properties storage configuration key-value map
     * @throws IOException if the filesystem cannot be created
     */
    FileSystem create(Map<String, String> properties) throws IOException;
}
