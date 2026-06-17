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

package org.apache.doris.connector.metastore.spi;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Common state for the backend property impls: the raw map, the shared {@code warehouse} property
 * (legacy {@code AbstractPaimonProperties.warehouse}, required by all paimon flavors), and the
 * {@code matchedProperties()} derivation. Subclasses bind their {@code @ConnectorProperty} fields via
 * {@code ConnectorPropertiesUtils.bindConnectorProperties} in their {@code of(...)} factory AFTER
 * construction (so subclass field initializers do not clobber the bound values).
 */
public abstract class AbstractMetaStoreProperties implements MetaStoreProperties {

    @ConnectorProperty(names = {"warehouse"}, required = false,
            description = "Warehouse root location for the catalog.")
    protected String warehouse = "";

    protected final Map<String, String> raw;

    protected AbstractMetaStoreProperties(Map<String, String> raw) {
        this.raw = raw;
    }

    @Override
    public Map<String, String> rawProperties() {
        return raw;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return MetaStoreParseUtils.matchedProperties(this, raw);
    }

    /** Shared fail-fast: {@code warehouse} is required by every paimon flavor (legacy parity). */
    protected void requireWarehouse() {
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException("Property warehouse is required.");
        }
    }
}
