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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a data type in the connector's own type system.
 *
 * <p>A type is identified by its name (e.g. "INT", "VARCHAR", "DECIMAL"),
 * optional precision/scale parameters, and optional child types for
 * complex types like ARRAY or MAP.</p>
 */
public final class ConnectorType {

    private final String typeName;
    private final int precision;
    private final int scale;
    private final List<ConnectorType> children;
    private final List<String> fieldNames;

    public ConnectorType(String typeName) {
        this(typeName, -1, -1, Collections.emptyList(),
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale) {
        this(typeName, precision, scale, Collections.emptyList(),
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children) {
        this(typeName, precision, scale, children,
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children, List<String> fieldNames) {
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.precision = precision;
        this.scale = scale;
        this.children = children == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(children);
        this.fieldNames = fieldNames == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(fieldNames);
    }

    /** Factory: simple type with no parameters. */
    public static ConnectorType of(String typeName) {
        return new ConnectorType(typeName);
    }

    /** Factory: type with precision and scale. */
    public static ConnectorType of(String typeName,
            int precision, int scale) {
        return new ConnectorType(typeName, precision, scale);
    }

    /** Factory: ARRAY type with element type. */
    public static ConnectorType arrayOf(ConnectorType elementType) {
        return new ConnectorType("ARRAY", -1, -1,
                Collections.singletonList(elementType));
    }

    /** Factory: MAP type with key and value types. */
    public static ConnectorType mapOf(ConnectorType keyType,
            ConnectorType valueType) {
        return new ConnectorType("MAP", -1, -1,
                Arrays.asList(keyType, valueType));
    }

    /** Factory: STRUCT type with named fields. */
    public static ConnectorType structOf(List<String> names,
            List<ConnectorType> fieldTypes) {
        return new ConnectorType("STRUCT", -1, -1, fieldTypes, names);
    }

    public String getTypeName() {
        return typeName;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public List<ConnectorType> getChildren() {
        return children;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public String toString() {
        if (precision < 0) {
            return typeName;
        }
        if (scale < 0) {
            return typeName + "(" + precision + ")";
        }
        return typeName + "(" + precision + "," + scale + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorType)) {
            return false;
        }
        ConnectorType that = (ConnectorType) o;
        return precision == that.precision
                && scale == that.scale
                && typeName.equals(that.typeName)
                && children.equals(that.children)
                && fieldNames.equals(that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, precision, scale,
                children, fieldNames);
    }
}
