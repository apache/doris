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

package org.apache.doris.connector.metastore.spi.hms;

import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;
import java.util.Set;

/** Selects the Hive Metastore backend ({@code paimon.catalog.type == hms}). */
public final class HmsMetaStoreProvider implements MetaStoreProvider<HmsMetaStoreProperties> {

    @Override
    public boolean supportsType(String catalogType) {
        return "hms".equalsIgnoreCase(catalogType);
    }

    @Override
    public HmsMetaStoreProperties bind(Map<String, String> properties, Map<String, String> storageHadoopConfig) {
        return HmsMetaStorePropertiesImpl.of(properties, storageHadoopConfig);
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(HmsMetaStorePropertiesImpl.class);
    }

    @Override
    public String name() {
        return "HMS";
    }
}
