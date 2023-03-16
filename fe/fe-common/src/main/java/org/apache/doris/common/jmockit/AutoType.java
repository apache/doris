/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to convert type between Java's wrapper type and primitive type
 * There are 8 wrapper/primitive types in Java:
 * |Wrapped Type         |Primitive Type
 * --------------------------------------
 * |Boolean              |boolean
 * |Character            |char
 * |Byte                 |byte
 * |Short                |short
 * |Integer              |int
 * |Float                |float
 * |Long                 |long
 * |Double               |double
 */
public class AutoType {
    private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = new HashMap();
    private static final Map<Class<?>, Class<?>> WRAPPER_TO_PRIMITIVE = new HashMap();

    public static boolean isWrapperOfPrimitiveType(Class<?> type) {
        return WRAPPER_TO_PRIMITIVE.containsKey(type);
    }

    public static Class<?> getPrimitiveType(Class<?> wrapperType) {
        return WRAPPER_TO_PRIMITIVE.get(wrapperType);
    }

    public static Class<?> getWrapperType(Class<?> primitiveType) {
        return PRIMITIVE_TO_WRAPPER.get(primitiveType);
    }

    static {
        WRAPPER_TO_PRIMITIVE.put(Boolean.class, Boolean.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Character.class, Character.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Byte.class, Byte.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Short.class, Short.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Integer.class, Integer.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Float.class, Float.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Long.class, Long.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Double.class, Double.TYPE);

        PRIMITIVE_TO_WRAPPER.put(Boolean.TYPE, Boolean.class);
        PRIMITIVE_TO_WRAPPER.put(Character.TYPE, Character.class);
        PRIMITIVE_TO_WRAPPER.put(Byte.TYPE, Byte.class);
        PRIMITIVE_TO_WRAPPER.put(Short.TYPE, Short.class);
        PRIMITIVE_TO_WRAPPER.put(Integer.TYPE, Integer.class);
        PRIMITIVE_TO_WRAPPER.put(Float.TYPE, Float.class);
        PRIMITIVE_TO_WRAPPER.put(Long.TYPE, Long.class);
        PRIMITIVE_TO_WRAPPER.put(Double.TYPE, Double.class);
    }
}
