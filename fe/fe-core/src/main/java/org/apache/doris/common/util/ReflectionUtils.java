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

package org.apache.doris.common.util;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

public class ReflectionUtils {
    private static final Map<Class, Class> boxToPrimitiveTypes = ImmutableMap.<Class, Class>builder()
            .put(Boolean.class, boolean.class)
            .put(Character.class, char.class)
            .put(Byte.class, byte.class)
            .put(Short.class, short.class)
            .put(Integer.class, int.class)
            .put(Long.class, long.class)
            .put(Float.class, float.class)
            .put(Double.class, double.class)
            .build();

    public static Optional<Class> getPrimitiveType(Class<?> targetClass) {
        if (targetClass.isPrimitive()) {
            return Optional.of(targetClass);
        }
        return Optional.ofNullable(boxToPrimitiveTypes.get(targetClass));
    }
}
