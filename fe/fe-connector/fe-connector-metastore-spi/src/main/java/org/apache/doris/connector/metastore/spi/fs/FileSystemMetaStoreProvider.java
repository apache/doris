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

import org.apache.doris.connector.metastore.FileSystemMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreParseUtils;
import org.apache.doris.connector.metastore.spi.MetaStoreProvider;

import java.util.Map;

/**
 * Selects the filesystem backend: the default when {@code paimon.catalog.type} is absent/blank, or
 * an explicit {@code filesystem}.
 */
public final class FileSystemMetaStoreProvider implements MetaStoreProvider<FileSystemMetaStoreProperties> {

    @Override
    public boolean supports(Map<String, String> properties) {
        // Default backend: the catalog-type key is ABSENT, or an explicit "filesystem". A present-but-other
        // value (incl. blank/whitespace) is NOT claimed here so it falls through to the dispatcher's
        // no-supporter throw, matching legacy's reject-on-unknown (no .trim(), consistent with the other
        // providers' plain equalsIgnoreCase).
        String type = properties.get(MetaStoreParseUtils.CATALOG_TYPE_KEY);
        return type == null || "filesystem".equalsIgnoreCase(type);
    }

    @Override
    public FileSystemMetaStoreProperties bind(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        return FileSystemMetaStorePropertiesImpl.of(properties);
    }

    @Override
    public String name() {
        return "FILESYSTEM";
    }
}
