/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

import java.util.regex.Pattern;

/**
 * Modify from mockit.internal.util.ParameterReflection JMockit v1.13
 * Util class to verify parameter of methods.
 */
public final class ParameterReflection {
    public static final Class<?>[] NO_PARAMETERS = new Class[0];

    public static final Pattern JAVA_LANG = Pattern.compile("java.lang.", 16);

    private ParameterReflection() {
    }

    /**
     * check if every member in {@declaredTypes} is completely equal to the corresponding member {@specifiedTypes}.
     */
    static boolean matchesParameterTypes(Class<?>[] declaredTypes, Class<?>[] specifiedTypes) {
        if (declaredTypes == null || specifiedTypes == null) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < declaredTypes.length; ++i) {
            Class<?> declaredType = declaredTypes[i];
            Class<?> specifiedType = specifiedTypes[i];
            if (!isSameType(declaredType, specifiedType)) {
                return false;
            }
        }

        return true;
    }

    /**
     * check if every member in {@paramTypes} is acceptable to the corresponding member in {@argTypes}.
     */
    static boolean acceptsArgumentTypes(Class<?>[] paramTypes, Class<?>[] argTypes) {
        if (paramTypes == null || argTypes == null) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < paramTypes.length; ++i) {
            Class<?> parType = paramTypes[i];
            Class<?> argType = argTypes[i];
            if (!isSameType(parType, argType) && !parType.isAssignableFrom(argType)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Get all types from objects {@args}.
     */
    static Class<?>[] getArgumentTypesFromArgumentValues(Object... args) {
        if (args == null) {
            throw new IllegalArgumentException();
        }
        if (args.length == 0) {
            return NO_PARAMETERS;
        } else {
            Class<?>[] argTypes = new Class[args.length];

            for (int i = 0; i < args.length; ++i) {
                argTypes[i] = getArgumentTypeFromArgumentValue(i, args);
            }

            return argTypes;
        }
    }

    /**
     * Get type from {@args} by index.
     */
    static Class<?> getArgumentTypeFromArgumentValue(int i, Object[] args) {
        Object arg = args[i];
        if (arg == null) {
            throw new IllegalArgumentException("Invalid null value passed as argument " + i);
        } else {
            Class argType;
            if (arg instanceof Class) {
                argType = (Class) arg;
                args[i] = null;
            } else {
                argType = GeneratedClasses.getMockedClass(arg);
            }

            return argType;
        }
    }

    /**
     * return true if {@currentTypes} is more specific than {@previousTypes}.
     */
    static boolean hasMoreSpecificTypes(Class<?>[] currentTypes, Class<?>[] previousTypes) {
        if (currentTypes == null || previousTypes == null) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < currentTypes.length; ++i) {
            Class<?> current = wrappedIfPrimitive(currentTypes[i]);
            Class<?> previous = wrappedIfPrimitive(previousTypes[i]);
            if (current != previous && previous.isAssignableFrom(current)) {
                return true;
            }
        }

        return false;
    }

    /**
     * return the type names of {@paramTypes} wrapped in brackets.
     */
    static String getParameterTypesDescription(Class<?>[] paramTypes) {
        if (paramTypes == null) {
            throw new IllegalArgumentException();
        }
        StringBuilder paramTypesDesc = new StringBuilder(200);
        paramTypesDesc.append('(');
        String sep = "";

        for (Class paramType : paramTypes) {
            String typeName = JAVA_LANG.matcher(paramType.getCanonicalName()).replaceAll("");
            paramTypesDesc.append(sep).append(typeName);
            sep = ", ";
        }

        paramTypesDesc.append(')');
        return paramTypesDesc.toString();
    }

    /**
     * return real parameters array of inner-class belong to the outer-class instance {@firstValue Object}.
     * the parameter[0] of a inner-class constructor is always the instance of its outer-class.
     */
    static Object[] argumentsWithExtraFirstValue(Object[] args, Object firstValue) {
        Object[] args2 = new Object[1 + args.length];
        args2[0] = firstValue;
        System.arraycopy(args, 0, args2, 1, args.length);
        return args2;
    }

    // return wrapped type if its type is primitive.
    private static Class<?> wrappedIfPrimitive(Class<?> parameterType) {
        if (parameterType.isPrimitive()) {
            Class<?> wrapperType = AutoType.getWrapperType(parameterType);

            assert wrapperType != null;

            return wrapperType;
        } else {
            return parameterType;
        }
    }

    // return true if the two types are same type.
    private static boolean isSameType(Class<?> firstType, Class<?> secondType) {
        return firstType == secondType
                || firstType.isPrimitive() && firstType == AutoType.getPrimitiveType(secondType)
                || secondType.isPrimitive() && secondType == AutoType.getPrimitiveType(firstType);
    }
}
