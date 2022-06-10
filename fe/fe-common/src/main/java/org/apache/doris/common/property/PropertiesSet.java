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

package org.apache.doris.common.property;

import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TPropertyVal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class PropertiesSet<T extends PropertySchema.SchemaGroup> {
    private static final Map<PropertySchema.SchemaGroup, PropertiesSet> emptyInstances = new HashMap<>();

    private final T schemaGroup;
    private final Map<String, Object> properties;
    private List<PropertySchema> modifiedSchemas;

    private PropertiesSet(T schemaGroup, Map<String, Object> properties) {
        this.schemaGroup = schemaGroup;
        this.properties = properties;
    }

    @SuppressWarnings("unchecked")
    public <U> U get(PropertySchema<U> prop) throws NoSuchElementException {
        return properties.containsKey(prop.getName())
                ? (U) properties.get(prop.getName()) : prop.getDefaultValue().get();
    }

    public List<PropertySchema> getModifiedSchemas() {
        if (modifiedSchemas == null) {
            synchronized (this) {
                modifiedSchemas = properties.keySet().stream()
                        .map(key -> schemaGroup.getSchemas().get(key))
                        .collect(Collectors.toList());
            }
        }
        return modifiedSchemas;
    }

    private static <TSchemaGroup extends PropertySchema.SchemaGroup, TRaw> void checkRequiredKey(
            TSchemaGroup schemaGroup, Map<String, TRaw> rawProperties) throws NoSuchElementException {
        List<String> requiredKey = schemaGroup.getSchemas().values().stream()
                .filter(propertySchema -> !propertySchema.getDefaultValue().isPresent())
                .map(PropertySchema::getName)
                .collect(Collectors.toList());

        List<String> missingKeys = requiredKey.stream()
                .filter(key -> !rawProperties.containsKey(key))
                .collect(Collectors.toList());

        if (!missingKeys.isEmpty()) {
            throw new NoSuchElementException("Missing " + missingKeys);
        }
    }

    public static <TSchemaGroup extends PropertySchema.SchemaGroup> void verifyKey(
            TSchemaGroup schemaGroup, List<String> rawProperties)
            throws IllegalArgumentException {
        rawProperties.forEach(entry -> {
            if (!schemaGroup.getSchemas().containsKey(entry.toLowerCase())) {
                throw new IllegalArgumentException("Invalid property " + entry);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Map<String, TPropertyVal> writeToThrift() {
        Map<String, TPropertyVal> ret = new HashMap<>(properties.size());
        properties.forEach((key, value) -> {
            TPropertyVal out = new TPropertyVal();
            schemaGroup.getSchemas().get(key).write(value, out);
            ret.put(key, out);
        });
        return ret;
    }

    @SuppressWarnings("unchecked")
    public void writeToData(DataOutput out) throws IOException {
        out.writeInt(properties.size());
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            schemaGroup.getSchemas().get(entry.getKey()).write(entry.getValue(), out);
        }
    }

    private interface ReadLambda<TParsed, TRaw> {
        TParsed accept(PropertySchema schema, TRaw raw);
    }

    @SuppressWarnings("unchecked")
    private static <TSchemaGroup extends PropertySchema.SchemaGroup, TParsed, TRaw> PropertiesSet<TSchemaGroup> read(
            TSchemaGroup schemaGroup, Map<String, TRaw> rawProperties, ReadLambda<TParsed, TRaw> reader)
            throws IllegalArgumentException, NoSuchElementException {
        checkRequiredKey(schemaGroup, rawProperties);
        Map<String, Object> properties = new HashMap<>(rawProperties.size());

        rawProperties.forEach((key, value) -> {
            String entryKey = key.toLowerCase();
            if (!schemaGroup.getSchemas().containsKey(entryKey)) {
                throw new IllegalArgumentException("Invalid property " + key);
            }
            PropertySchema schema = schemaGroup.getSchemas().get(entryKey);
            properties.put(entryKey, reader.accept(schema, value));
        });

        return new PropertiesSet(schemaGroup, properties);
    }

    @SuppressWarnings("unchecked")
    public static <TSchemaGroup extends PropertySchema.SchemaGroup> PropertiesSet<TSchemaGroup> empty(
            TSchemaGroup schemaGroup) {
        if (!emptyInstances.containsKey(schemaGroup)) {
            synchronized (PropertiesSet.class) {
                if (!emptyInstances.containsKey(schemaGroup)) {
                    emptyInstances.put(schemaGroup, new PropertiesSet(schemaGroup, Collections.emptyMap()));
                }
            }
        }

        return emptyInstances.get(schemaGroup);
    }

    public static <TSchemaGroup extends PropertySchema.SchemaGroup> PropertiesSet<TSchemaGroup> readFromStrMap(
            TSchemaGroup schemaGroup, Map<String, String> rawProperties)
            throws IllegalArgumentException {

        return read(schemaGroup, rawProperties, PropertySchema::read);
    }

    public static <TSchemaGroup extends PropertySchema.SchemaGroup> PropertiesSet<TSchemaGroup> readFromThrift(
            TSchemaGroup schemaGroup, Map<String, TPropertyVal> rawProperties)
            throws IllegalArgumentException {

        return read(schemaGroup, rawProperties, PropertySchema::read);
    }

    @SuppressWarnings("unchecked")
    public static <TSchemaGroup extends PropertySchema.SchemaGroup> PropertiesSet<TSchemaGroup> readFromData(
            TSchemaGroup schemaGroup, DataInput input)
            throws IllegalArgumentException, IOException {
        int size = input.readInt();
        Map<String, Object> properties = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String key = Text.readString(input).toLowerCase();
            Object val = schemaGroup.getSchemas().get(key).read(input);
            properties.put(key, val);
        }

        return new PropertiesSet(schemaGroup, properties);
    }

    @Override
    public String toString() {
        return properties.toString();
    }
}
