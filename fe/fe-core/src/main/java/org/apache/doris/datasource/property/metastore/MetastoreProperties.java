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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectionProperties;

import lombok.Getter;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * MetastoreProperties is the base class for handling configuration of different types of metastores
 * such as Hive Metastore (HMS), AWS Glue, Aliyun DLF, Iceberg REST catalog, Google Dataproc,
 * or file-based metastores (like Hadoop).
 * <p>
 * It uses a simple factory pattern based on a registry to dynamically instantiate the correct
 * subclass according to the provided configuration.
 * <p>
 * Supported metastore types are defined in the {@link Type} enum. Multiple alias names can be mapped to each type.
 */
public class MetastoreProperties extends ConnectionProperties {

    /**
     * Enum representing supported metastore types.
     * Each type can have one or more alias strings (case-insensitive).
     */
    public enum Type {
        HMS("hms"),
        GLUE("glue"),
        DLF("dlf"),
        ICEBERG_REST("rest"),
        DATAPROC("dataproc"),
        FILE_SYSTEM("filesystem", "hadoop"),
        UNKNOWN(); // fallback, not used directly

        private final Set<String> aliases;

        Type(String... aliases) {
            this.aliases = new HashSet<>(Arrays.asList(aliases));
        }

        /**
         * Parses a string into a {@link Type} if possible.
         *
         * @param input string value (case-insensitive)
         * @return optional type if match found
         */
        public static Optional<Type> fromString(String input) {
            if (input == null) {
                return Optional.empty();
            }
            String normalized = input.trim().toLowerCase(Locale.ROOT);
            for (Type type : values()) {
                if (type.aliases.contains(normalized)) {
                    return Optional.of(type);
                }
            }
            return Optional.empty();
        }
    }

    /**
     * The resolved metastore type for this configuration.
     */
    @Getter
    protected Type type;

    /**
     * Common property keys that may specify the metastore type.
     * These are checked in order to resolve the type from provided config.
     */
    private static final List<String> POSSIBLE_TYPE_KEYS = Arrays.asList(
            "metastore.type",
            "hive.metastore.type",
            "iceberg.catalog.type",
            "paimon.catalog.type",
            "type"
    );

    /**
     * Registry mapping each {@link Type} to its constructor logic.
     */
    private static final Map<Type, Function<Map<String, String>, MetastoreProperties>> FACTORY_MAP
            = new EnumMap<>(Type.class);

    static {
        // Register all known factories here
        FACTORY_MAP.put(Type.HMS, HMSProperties::new);
        FACTORY_MAP.put(Type.GLUE, AWSGlueProperties::new);
        FACTORY_MAP.put(Type.DLF, AliyunDLFProperties::new);
        FACTORY_MAP.put(Type.ICEBERG_REST, IcebergRestProperties::new);
        FACTORY_MAP.put(Type.DATAPROC, DataProcProperties::new);
        FACTORY_MAP.put(Type.FILE_SYSTEM, FileMetastoreProperties::new);
    }

    /**
     * Factory method to create an appropriate {@link MetastoreProperties} instance from raw properties.
     *
     * @param origProps original user configuration
     * @return resolved and initialized metastore properties instance
     * @throws UserException if the configuration is invalid or unsupported
     */
    public static MetastoreProperties create(Map<String, String> origProps) throws UserException {
        Type msType = resolveType(origProps);
        return create(msType, origProps);
    }

    /**
     * Resolves the {@link Type} of metastore from the property map by checking common keys.
     *
     * @param props original property map
     * @return resolved type
     */
    private static Type resolveType(Map<String, String> props) {
        for (String key : POSSIBLE_TYPE_KEYS) {
            if (props.containsKey(key)) {
                String value = props.get(key);
                Optional<Type> opt = Type.fromString(value);
                if (opt.isPresent()) {
                    return opt.get();
                } else {
                    throw new IllegalArgumentException("Unknown metastore type value '" + value + "' for key: " + key);
                }
            }
        }
        throw new IllegalArgumentException("No metastore type found in properties. Tried keys: " + POSSIBLE_TYPE_KEYS);
    }

    /**
     * Factory method to directly create a metastore properties instance given a type.
     *
     * @param type      resolved type
     * @param origProps original configuration
     * @return constructed and validated {@link MetastoreProperties}
     * @throws UserException if validation fails
     */
    public static MetastoreProperties create(Type type, Map<String, String> origProps) throws UserException {
        Function<Map<String, String>, MetastoreProperties> constructor = FACTORY_MAP.get(type);
        if (constructor == null) {
            throw new IllegalArgumentException("Unsupported metastore type: " + type);
        }
        MetastoreProperties instance = constructor.apply(origProps);
        instance.initNormalizeAndCheckProps();
        return instance;
    }

    /**
     * Base constructor for subclasses to initialize the common state.
     *
     * @param type      metastore type
     * @param origProps original configuration
     */
    protected MetastoreProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }
}
