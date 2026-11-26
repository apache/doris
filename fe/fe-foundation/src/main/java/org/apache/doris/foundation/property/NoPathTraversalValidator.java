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

package org.apache.doris.foundation.property;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Rejects parent-directory path components in a string property.
 *
 * <p>Both slash forms are treated as separators so a value cannot become unsafe when it is
 * forwarded to a platform or library with different path-separator semantics. Comma-separated
 * resource lists are supported naturally because commas cannot hide a path component.
 */
public final class NoPathTraversalValidator implements ConnectorPropertyValidator {

    @Override
    public void validate(Object target, Field field, String propertyName, Object value,
            Map<String, String> properties) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("Path property must be a string");
        }
        for (String component : ((String) value).split("[,/\\\\]")) {
            if ("..".equals(component.trim())) {
                throw new IllegalArgumentException(
                        "Property " + propertyName + " must not contain parent-directory references");
            }
        }
    }
}
