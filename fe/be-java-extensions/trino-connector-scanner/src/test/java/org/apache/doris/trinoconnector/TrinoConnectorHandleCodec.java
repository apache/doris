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

package org.apache.doris.trinoconnector;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.type.TypeManager;
import io.trino.type.InternalTypeManager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The writing half of the wire format the scanner reads: it produces the JSON strings FE sends.
 *
 * <p>A test copy of FE's TrinoJsonSerializer rather than a shared one, because the two sides are
 * meant to be independent - if the serializer the scanner is tested against were the same object
 * the scanner deserializes with, a change to both at once would look correct. It is the same four
 * handle modules FE registers, resolved through the handle resolver of the connectors this plugin
 * loaded, which is what puts the right classloader id into every {@code @type}.
 */
class TrinoConnectorHandleCodec {

    private final ObjectMapperProvider objectMapperProvider;

    TrinoConnectorHandleCodec(TrinoConnectorPluginManager pluginManager) {
        ObjectMapperProvider provider = new ObjectMapperProvider();

        Set<Module> modules = new HashSet<>();
        modules.add(HandleJsonModule.tableHandleModule(pluginManager.getHandleResolver()));
        modules.add(HandleJsonModule.columnHandleModule(pluginManager.getHandleResolver()));
        modules.add(HandleJsonModule.splitModule(pluginManager.getHandleResolver()));
        modules.add(HandleJsonModule.transactionHandleModule(pluginManager.getHandleResolver()));
        provider.setModules(modules);

        TypeManager typeManager = new InternalTypeManager(pluginManager.getTypeRegistry());
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(
                new BlockEncodingManager(), typeManager);
        provider.setJsonSerializers(ImmutableMap.of(
                Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));

        this.objectMapperProvider = provider;
    }

    @SuppressWarnings("unchecked")
    <T> String toJson(T object) {
        if (object instanceof List) {
            List<?> list = (List<?>) object;
            JsonCodec<List<Object>> codec = (JsonCodec<List<Object>>) (JsonCodec<?>)
                    new JsonCodecFactory(objectMapperProvider)
                            .listJsonCodec(list.get(0).getClass());
            return codec.toJson((List<Object>) list);
        }
        JsonCodec<T> codec = (JsonCodec<T>) new JsonCodecFactory(objectMapperProvider)
                .jsonCodec(object.getClass());
        return codec.toJson(object);
    }
}
