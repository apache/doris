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

package org.apache.doris.datasource.property;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for handling fields annotated with {@link ConnectorProperty}.
 * Provides methods to extract supported connector properties from a class and bind them to an object instance.
 */
public class ConnectorPropertiesUtils {

    /**
     * Retrieves all fields annotated with {@link ConnectorProperty} from the given class and its superclasses,
     * where {@code supported = true}.
     *
     * @param clazz the target class to inspect
     * @return list of supported fields annotated with {@code @ConnectorProperty}
     */
    public static List<Field> getConnectorProperties(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> currentClass = clazz;

        while (currentClass != null && currentClass != Object.class) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(ConnectorProperty.class)) {
                    ConnectorProperty connectorProperty = field.getAnnotation(ConnectorProperty.class);
                    if (connectorProperty.supported()) {
                        field.setAccessible(true);
                        fields.add(field);
                    }
                }
            }
            currentClass = currentClass.getSuperclass();
        }

        return fields;
    }

    /**
     * Binds matching property values from the given map to the corresponding fields in the target object.
     * Only fields annotated with {@code @ConnectorProperty} and marked as supported will be set.
     *
     * @param target the target object to populate
     * @param props  the key-value map of string properties
     * @throws RuntimeException if a conversion or reflection error occurs
     */
    public static void bindConnectorProperties(Object target, Map<String, String> props) {
        List<Field> supportedProps = getConnectorProperties(target.getClass());

        for (Field field : supportedProps) {
            String matchedName = getMatchedPropertyName(field, props);
            if (StringUtils.isNotBlank(matchedName) && StringUtils.isNotBlank(props.get(matchedName))) {
                try {
                    Object rawValue = props.get(matchedName);
                    Object convertedValue = convertValue(rawValue, field.getType());
                    field.set(target, convertedValue);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            "Failed to set property '" + matchedName + "' on " + target.getClass().getSimpleName()
                                    + ": " + e.getMessage(), e
                    );
                }
            }
        }
    }

    /**
     * Finds the first matching property name from the field's {@code @ConnectorProperty#names()} list
     * that exists in the provided property map.
     *
     * @param field the field to match
     * @param props the available property map
     * @return the matching property name if found; {@code null} otherwise
     */
    public static String getMatchedPropertyName(Field field, Map<String, String> props) {
        ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
        if (annotation == null) {
            return null;
        }
        for (String name : annotation.names()) {
            if (StringUtils.isNotBlank(props.get(name))) {
                return name;
            }
        }

        return null;
    }

    /**
     * Converts a string-based value into a strongly-typed object based on the target field type.
     *
     * @param value      the raw value (usually a string from configuration)
     * @param targetType the field's target type
     * @return the converted value
     * @throws IllegalArgumentException if the type is unsupported
     */
    public static Object convertValue(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }

        String str = value.toString().trim();

        if (targetType == String.class) {
            return str;
        }

        if (targetType == Integer.class || targetType == int.class) {
            return Integer.parseInt(str);
        }

        if (targetType == Boolean.class || targetType == boolean.class) {
            return Boolean.parseBoolean(str);
        }

        if (targetType == Long.class || targetType == long.class) {
            return Long.parseLong(str);
        }

        if (targetType == Double.class || targetType == double.class) {
            return Double.parseDouble(str);
        }

        throw new IllegalArgumentException("Unsupported property type: " + targetType.getName());
    }
}

