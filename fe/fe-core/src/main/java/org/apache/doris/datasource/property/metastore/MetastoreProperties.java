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
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public enum Type {
        HMS("hms"),
        ICEBERG("iceberg"),
        PAIMON("paimon"),
        GLUE("glue"),
        DLF("dlf"),
        DATAPROC("dataproc"),
        FILE_SYSTEM("filesystem", "hadoop"),
        UNKNOWN();

        private final Set<String> aliases;

        Type(String... aliases) {
            this.aliases = new HashSet<>(Arrays.asList(aliases));
        }

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

    @Getter
    protected Type type;

    private static final String METASTORE_TYPE_KEY = "type";

    private static final Map<Type, MetastorePropertiesFactory> FACTORY_MAP = new EnumMap<>(Type.class);

    static {
        //subclasses should be registered here
        register(Type.HMS, new HMSPropertiesFactory());
        register(Type.ICEBERG, new IcebergPropertiesFactory());
        register(Type.PAIMON, new PaimonPropertiesFactory());
    }

    public static void register(Type type, MetastorePropertiesFactory factory) {
        FACTORY_MAP.put(type, factory);
    }

    public static MetastoreProperties create(Map<String, String> props) throws UserException {
        Type type = resolveType(props);
        MetastorePropertiesFactory factory = FACTORY_MAP.get(type);
        if (factory == null) {
            throw new IllegalArgumentException("Unsupported metastore type: " + type);
        }
        return factory.create(props);
    }

    private static Type resolveType(Map<String, String> props) {
        String typeValue = props.get(METASTORE_TYPE_KEY);
        if (StringUtils.isBlank(typeValue)) {
            throw new IllegalArgumentException("Metastore type is required");
        }

        Optional<Type> typeOpt = Type.fromString(typeValue);
        if (typeOpt.isPresent()) {
            return typeOpt.get();
        }
        throw new IllegalArgumentException("Unknown metastore type value '" + typeValue + "'. "
                + "Supported types are: " + Arrays.toString(Type.values()));
    }

    protected MetastoreProperties(Type type, Map<String, String> props) {
        super(props);
        this.type = type;
    }

    protected MetastoreProperties(Map<String, String> props) {
        super(props);
    }
}
