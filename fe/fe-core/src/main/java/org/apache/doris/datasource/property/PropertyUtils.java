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

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.List;

public class PropertyUtils {

    // Get all fields of a class with annotation @ConnectorProperty
    public static List<Field> getConnectorProperties(Class<?> clazz) {
        List<Field> fields = Lists.newArrayList();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(ConnectorProperty.class)) {
                // Get annotation of the field
                ConnectorProperty connectorProperty = field.getAnnotation(ConnectorProperty.class);
                if (connectorProperty.supported()) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }
}
