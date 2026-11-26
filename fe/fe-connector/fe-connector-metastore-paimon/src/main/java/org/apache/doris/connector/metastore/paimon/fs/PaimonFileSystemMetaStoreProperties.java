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
import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/**
 * Paimon filesystem (warehouse-only) metastore backend facts. The storage config is overlaid by the
 * connector at catalog-build time, so this impl carries only the warehouse and declares
 * {@link #needsStorage()} true.
 */
public final class PaimonFileSystemMetaStoreProperties extends AbstractMetaStoreProperties
        implements FileSystemMetaStoreProperties {

    private PaimonFileSystemMetaStoreProperties(Map<String, String> raw) {
        super(raw);
    }

    public static PaimonFileSystemMetaStoreProperties of(Map<String, String> raw) {
        PaimonFileSystemMetaStoreProperties props = new PaimonFileSystemMetaStoreProperties(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "FILESYSTEM";
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    @Override
    public void validate() {
        requireWarehouse();
    }

    @Override
    public String getWarehouse() {
        return warehouse;
    }
}
