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

package org.apache.doris.connector.trino;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.type.TypeManager;
import io.trino.type.InternalTypeManager;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility for JSON-serializing Trino SPI objects (ConnectorSplit, ConnectorTableHandle,
 * ColumnHandle, etc.) for transmission to BE.
 *
 * <p>Uses Trino's {@link HandleJsonModule} and {@link BlockJsonSerde} infrastructure
 * from the plugin's {@link TrinoBootstrap} singleton to register the proper Jackson
 * serializers/deserializers for all loaded Trino plugin types.</p>
 *
 * <p>Ported from {@code TrinoConnectorScanNode.createObjectMapperProvider()} in fe-core.</p>
 */
public class TrinoJsonSerializer {

    private final ObjectMapperProvider objectMapperProvider;

    public TrinoJsonSerializer(HandleResolver handleResolver,
            io.trino.metadata.TypeRegistry typeRegistry) {
        this.objectMapperProvider = createObjectMapperProvider(handleResolver, typeRegistry);
    }

    /**
     * Serialize any Trino SPI object to a JSON string.
     */
    @SuppressWarnings("unchecked")
    public <T> String toJson(T object) {
        io.airlift.json.JsonCodec<T> jsonCodec = (io.airlift.json.JsonCodec<T>)
                new JsonCodecFactory(objectMapperProvider).jsonCodec(object.getClass());
        return jsonCodec.toJson(object);
    }

    private static ObjectMapperProvider createObjectMapperProvider(
            HandleResolver handleResolver,
            io.trino.metadata.TypeRegistry typeRegistry) {
        ObjectMapperProvider provider = new ObjectMapperProvider();

        Set<Module> modules = new HashSet<>();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.splitModule(handleResolver));
        modules.add(HandleJsonModule.transactionHandleModule(handleResolver));
        provider.setModules(modules);

        TypeManager typeManager = new InternalTypeManager(typeRegistry);
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(
                new BlockEncodingManager(), typeManager);
        provider.setJsonSerializers(ImmutableMap.of(
                Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));

        return provider;
    }
}
