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

package org.apache.doris.nereids.util;

import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;
import java.util.function.Predicate;

public class FieldChecker {
    public static <T> Predicate<T> check(String fieldName, Object value) {
        return (o) -> {
            Field field;
            try {
                field = o.getClass().getDeclaredField(fieldName);
            } catch (Throwable e) {
                throw new RuntimeException("Check " + fieldName + " failed", e);
            }
            field.setAccessible(true);
            try {
                Assertions.assertEquals(value, field.get(o));
            } catch (Throwable e) {
                throw new RuntimeException("Check " + fieldName + " failed", e);
            }
            return true;
        };
    }
}
