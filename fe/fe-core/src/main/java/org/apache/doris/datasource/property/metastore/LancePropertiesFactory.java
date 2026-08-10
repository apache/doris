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

package org.apache.doris.datasource.property.metastore;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Selects Lance filesystem or REST properties from {@code lance.catalog.type}. */
public class LancePropertiesFactory extends AbstractMetastorePropertiesFactory {
    public LancePropertiesFactory() {
        register(AbstractLanceProperties.LANCE_FILESYSTEM, LanceFileSystemMetastoreProperties::new);
        register(AbstractLanceProperties.LANCE_REST, LanceRestMetastoreProperties::new);
    }

    @Override
    public MetastoreProperties create(Map<String, String> props) {
        String type = props.getOrDefault(AbstractLanceProperties.LANCE_CATALOG_TYPE,
                AbstractLanceProperties.LANCE_FILESYSTEM).trim().toLowerCase(Locale.ROOT);
        if (!AbstractLanceProperties.LANCE_FILESYSTEM.equals(type)
                && !AbstractLanceProperties.LANCE_REST.equals(type)) {
            throw new IllegalArgumentException("Property '" + AbstractLanceProperties.LANCE_CATALOG_TYPE
                    + "' must be 'filesystem' or 'rest', but was '" + type + "'");
        }
        Map<String, String> normalizedProperties = new HashMap<>(props);
        normalizedProperties.put(AbstractLanceProperties.LANCE_CATALOG_TYPE, type);
        return createInternal(normalizedProperties, AbstractLanceProperties.LANCE_CATALOG_TYPE,
                AbstractLanceProperties.LANCE_FILESYSTEM);
    }
}
