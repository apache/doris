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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A catalog's context: a plugin conf map and the storage bindings the engine would hand over. */
class LanceTestContext implements ConnectorContext {
    private final Map<String, String> conf = new HashMap<>();
    private final List<StorageProperties> storageProperties;
    private final Map<String, String> backendStorageProperties;

    LanceTestContext() {
        this(Collections.emptyList(), Collections.emptyMap());
    }

    LanceTestContext(List<StorageProperties> storageProperties, Map<String, String> backendStorageProperties) {
        this.storageProperties = storageProperties;
        this.backendStorageProperties = backendStorageProperties;
    }

    LanceTestContext withConf(String key, String value) {
        conf.put(key, value);
        return this;
    }

    @Override
    public String getCatalogName() {
        return "lance_test";
    }

    @Override
    public long getCatalogId() {
        return 1L;
    }

    @Override
    public Map<String, String> getConnectorConfig() {
        return conf;
    }

    @Override
    public ConnectorStorageContext getStorageContext() {
        return new ConnectorStorageContext() {
            @Override
            public List<StorageProperties> getStorageProperties() {
                return storageProperties;
            }

            @Override
            public Map<String, String> getBackendStorageProperties() {
                return backendStorageProperties;
            }
        };
    }
}
