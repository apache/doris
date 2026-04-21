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

package org.apache.doris.connector.api;

import java.util.Objects;

/**
 * Describes a configuration property that a connector exposes.
 *
 * @param <T> the Java type of the property value
 */
public final class ConnectorPropertyMetadata<T> {

    private final String name;
    private final String description;
    private final Class<T> type;
    private final T defaultValue;
    private final boolean required;

    private ConnectorPropertyMetadata(String name, String description,
            Class<T> type, T defaultValue, boolean required) {
        this.name = Objects.requireNonNull(name, "name");
        this.description = Objects.requireNonNull(
                description, "description");
        this.type = Objects.requireNonNull(type, "type");
        this.defaultValue = defaultValue;
        this.required = required;
    }

    /** Creates an optional String property with a default value. */
    public static ConnectorPropertyMetadata<String> stringProperty(
            String name, String description, String defaultValue) {
        return new ConnectorPropertyMetadata<>(
                name, description, String.class, defaultValue, false);
    }

    /** Creates an optional int property with a default value. */
    public static ConnectorPropertyMetadata<Integer> intProperty(
            String name, String description, int defaultValue) {
        return new ConnectorPropertyMetadata<>(
                name, description, Integer.class, defaultValue, false);
    }

    /** Creates an optional boolean property with a default value. */
    public static ConnectorPropertyMetadata<Boolean> booleanProperty(
            String name, String description, boolean defaultValue) {
        return new ConnectorPropertyMetadata<>(
                name, description, Boolean.class, defaultValue, false);
    }

    /** Creates a required String property with no default. */
    public static ConnectorPropertyMetadata<String> requiredStringProperty(
            String name, String description) {
        return new ConnectorPropertyMetadata<>(
                name, description, String.class, null, true);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Class<T> getType() {
        return type;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorPropertyMetadata)) {
            return false;
        }
        ConnectorPropertyMetadata<?> that = (ConnectorPropertyMetadata<?>) o;
        return required == that.required
                && name.equals(that.name)
                && description.equals(that.description)
                && type.equals(that.type)
                && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, type, defaultValue, required);
    }

    @Override
    public String toString() {
        return "ConnectorPropertyMetadata{name='" + name
                + "', type=" + type.getSimpleName()
                + ", required=" + required + "}";
    }
}
