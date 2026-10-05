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
 * Validates a converted connector property before it is assigned to its target field.
 *
 * <p>Implementations must be stateless because one validator instance may be reused across
 * property bindings.
 */
public interface ConnectorPropertyValidator {

    /**
     * Validates one connector property value.
     *
     * @param target target object being populated
     * @param field annotated target field
     * @param propertyName matched property name
     * @param value converted property value
     * @param properties complete raw property map
     * @throws IllegalArgumentException if the value is invalid
     */
    void validate(Object target, Field field, String propertyName, Object value, Map<String, String> properties);

    /** Default validator used by properties that do not declare validation rules. */
    final class None implements ConnectorPropertyValidator {
        @Override
        public void validate(Object target, Field field, String propertyName, Object value,
                Map<String, String> properties) {
        }
    }
}
