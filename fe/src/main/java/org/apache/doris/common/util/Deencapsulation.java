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

import org.apache.doris.common.ConstructorReflection;
import org.apache.doris.common.FieldReflection;
import org.apache.doris.common.MethodReflection;

public final class Deencapsulation {
    private Deencapsulation() {
    }

    public static <T> T getField(Object objectWithField, String fieldName) {
        return FieldReflection.getField(objectWithField.getClass(), fieldName, objectWithField);
    }

    public static <T> T getField(Object objectWithField, Class<T> fieldType) {
        return FieldReflection.getField(objectWithField.getClass(), fieldType, objectWithField);
    }

    public static <T> T getField(Class<?> classWithStaticField, String fieldName) {
        return FieldReflection.getField(classWithStaticField, fieldName, (Object)null);
    }

    public static <T> T getField(Class<?> classWithStaticField, Class<T> fieldType) {
        return FieldReflection.getField(classWithStaticField, fieldType, (Object)null);
    }

    public static void setField(Object objectWithField, String fieldName, Object fieldValue) {
        FieldReflection.setField(objectWithField.getClass(), objectWithField, fieldName, fieldValue);
    }

    public static void setField(Object objectWithField, Object fieldValue) {
        FieldReflection.setField(objectWithField.getClass(), objectWithField, (String)null, fieldValue);
    }

    public static void setField(Class<?> classWithStaticField, String fieldName, Object fieldValue) {
        FieldReflection.setField(classWithStaticField, (Object)null, fieldName, fieldValue);
    }

    public static void setField(Class<?> classWithStaticField, Object fieldValue) {
        FieldReflection.setField(classWithStaticField, (Object)null, (String)null, fieldValue);
    }

    public static <T> T invoke(Object objectWithMethod, String methodName, Object... nonNullArgs) {
        Class<?> theClass = objectWithMethod.getClass();
        return MethodReflection.invoke(theClass, objectWithMethod, methodName, nonNullArgs);
    }

    public static <T> T invoke(Class<?> classWithStaticMethod, String methodName, Object... nonNullArgs) {
        return MethodReflection.invoke(classWithStaticMethod, (Object)null, methodName, nonNullArgs);
    }

    public static <T> T newInstance(Class<? extends T> classToInstantiate, Object... nonNullArgs) {
        return ConstructorReflection.newInstance(classToInstantiate, nonNullArgs);
    }

    public static <T> T newInnerInstance(Class<? extends T> innerClassToInstantiate, Object outerClassInstance, Object... nonNullArgs) {
        return ConstructorReflection.newInnerInstance(innerClassToInstantiate, outerClassInstance, nonNullArgs);
    }
}
