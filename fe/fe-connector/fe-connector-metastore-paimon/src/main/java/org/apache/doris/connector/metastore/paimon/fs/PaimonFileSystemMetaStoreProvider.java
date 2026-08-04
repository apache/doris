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

package org.apache.doris.connector.metastore.paimon.fs;

import org.apache.doris.connector.metastore.FileSystemMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProvider;

import java.util.Map;

/**
 * Selects the paimon filesystem backend: the default when {@code paimon.catalog.type} is absent/blank, or
 * an explicit {@code filesystem}.
 */
public final class PaimonFileSystemMetaStoreProvider implements MetaStoreProvider<FileSystemMetaStoreProperties> {

    @Override
    public boolean supportsType(String catalogType) {
        // Default backend: the catalog-type token is ABSENT (null), or an explicit "filesystem". A
        // present-but-other value (incl. blank/whitespace) is NOT claimed here so it falls through to the
        // dispatcher's no-supporter throw, matching legacy's reject-on-unknown (no .trim(), consistent
        // with the other providers' plain equalsIgnoreCase).
        return catalogType == null || "filesystem".equalsIgnoreCase(catalogType);
    }

    @Override
    public FileSystemMetaStoreProperties bind(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        return PaimonFileSystemMetaStoreProperties.of(properties);
    }

    @Override
    public String name() {
        return "FILESYSTEM";
    }
}
