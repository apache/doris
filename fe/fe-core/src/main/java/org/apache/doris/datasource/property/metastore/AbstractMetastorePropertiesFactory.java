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
import java.util.function.Function;

/**
 * Abstract base class for implementing {@link MetastorePropertiesFactory}.
 * <p>
 * This class provides common logic for:
 * - Registering metastore subtypes.
 * - Selecting the appropriate constructor based on user-specified properties.
 * <p>
 * Subclasses only need to register their supported subtypes and provide the corresponding config key.
 */
public abstract class AbstractMetastorePropertiesFactory implements MetastorePropertiesFactory {

    protected final Map<String, Function<Map<String, String>, MetastoreProperties>> registeredSubTypes =
            new HashMap<>();

    /**
     * Registers a new metastore subtype with its corresponding constructor.
     *
     * @param subType     the subtype name (e.g., "hms", "glue")
     * @param constructor the function that creates the {@link MetastoreProperties} instance
     */
    protected void register(String subType, Function<Map<String, String>, MetastoreProperties> constructor) {
        registeredSubTypes.put(subType.toLowerCase(Locale.ROOT), constructor);
    }

    /**
     * Creates a {@link MetastoreProperties} instance based on the specified properties and subtype key.
     *
     * @param props       the configuration map
     * @param key         the property key used to determine the subtype (e.g., "hive.metastore.type")
     * @param defaultType the default subtype to fall back on if the key is not present (nullable)
     * @return a properly initialized {@link MetastoreProperties} instance
     * @throws IllegalArgumentException if the subtype is missing, empty, or unsupported
     */
    protected MetastoreProperties createInternal(Map<String, String> props, String key, String defaultType) {
        String subType = props.getOrDefault(key, defaultType);
        if (subType == null || subType.trim().isEmpty()) {
            throw new IllegalArgumentException(key + " is not set or is empty in properties");
        }

        Function<Map<String, String>, MetastoreProperties> constructor =
                registeredSubTypes.get(subType.toLowerCase(Locale.ROOT));
        if (constructor == null) {
            throw new IllegalArgumentException("Unsupported metastore subtype: " + subType);
        }

        MetastoreProperties instance = constructor.apply(props);
        instance.initNormalizeAndCheckProps();
        return instance;
    }
}
