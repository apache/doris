/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Modify from mockit.internal.util.ConstructorReflection JMockit v1.13
 * Util class to invoke constructor of specified class.
 */
public final class ConstructorReflection {

    private ConstructorReflection() {
    }

    /**
     * invoke the {@constructor} with parameters {@initArgs}.
     */
    public static <T> T invoke(Constructor<T> constructor, Object... initArgs) {
        if (constructor == null || initArgs == null) {
            throw new IllegalArgumentException();
        }
        makeAccessible(constructor);

        try {
            return constructor.newInstance(initArgs);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new IllegalStateException("Should never get here", cause);
            }
        }
    }

    /**
     * invoke the constructor with parameters {@nonNullArgs Object...}.
     */
    public static <T> T newInstance(Class<? extends T> aClass, Object... nonNullArgs) {
        if (aClass == null || nonNullArgs == null) {
            throw new IllegalArgumentException();
        } else {
            Class<?>[] argTypes = ParameterReflection.getArgumentTypesFromArgumentValues(nonNullArgs);
            Constructor<T> constructor = findCompatibleConstructor(aClass, argTypes);
            return invoke(constructor, nonNullArgs);
        }
    }

    /**
     * invoke the constructor with no parameters of {@aClass Class<T>}.
     */
    private static <T> T newInstance(Class<T> aClass) {
        return (T) newInstance((Class) aClass, ParameterReflection.NO_PARAMETERS);
    }

    /**
     * invoke the default constructor of {@aClass Class<T>}.
     * if the default constructor is not available, try to invoke the one constructor with no parameters.
     */
    public static <T> T newInstanceUsingDefaultConstructor(Class<T> aClass) {
        if (aClass == null) {
            throw new IllegalArgumentException();
        }
        try {
            return aClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            return newInstance(aClass);
        }
    }

    /**
     * invoke the default constructor of {@aClass Class<T>}.
     */
    public static <T> T newInstanceUsingDefaultConstructorIfAvailable(Class<T> aClass) {
        if (aClass == null) {
            throw new IllegalArgumentException();
        }
        try {
            return aClass.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    /**
     * invoke inner-class constructor with outer-class instance {@outerInstance} and parameters {@nonNullArgs}.
     */
    public static <T> T newInnerInstance(Class<? extends T> innerClass, Object outerInstance, Object... nonNullArgs) {
        if (innerClass == null || outerInstance == null || nonNullArgs == null) {
            throw new IllegalArgumentException();
        } else {
            Object[] initArgs = ParameterReflection.argumentsWithExtraFirstValue(nonNullArgs, outerInstance);
            return newInstance(innerClass, initArgs);
        }
    }

    /**
     * Get non-inner-class constructor with {@argTypes Class<?>[]}.
     * if more than one constructor was found, choose the more specific one. (i.e. constructor with parameters that have more concrete types is more specific)
     * if no constructor was found, will check if {@theClass} is a inner class. Then a IllegalArgumentException exception will be thrown.
     */
    private static <T> Constructor<T> findCompatibleConstructor(Class<?> theClass, Class<?>[] argTypes) {
        if (theClass == null || argTypes == null) {
            throw new IllegalArgumentException();
        }
        Constructor<T> found = null;
        Class<?>[] foundParameters = null;
        Constructor<?>[] declaredConstructors = theClass.getDeclaredConstructors();
        Constructor[] declaredConstructorsArray = declaredConstructors;

        for (Constructor<?> declaredConstructor : declaredConstructorsArray) {
            Class<?>[] declaredParamTypes = declaredConstructor.getParameterTypes();
            int gap = declaredParamTypes.length - argTypes.length;
            if (gap == 0 && (ParameterReflection.matchesParameterTypes(declaredParamTypes, argTypes)
                    || ParameterReflection.acceptsArgumentTypes(declaredParamTypes, argTypes))
                    && (found == null || ParameterReflection.hasMoreSpecificTypes(declaredParamTypes, foundParameters))) {
                found = (Constructor<T>) declaredConstructor;
                foundParameters = declaredParamTypes;
            }
        }

        if (found != null) {
            return found;
        } else {
            Class<?> declaringClass = theClass.getDeclaringClass();
            Class<?>[] paramTypes = declaredConstructors[0].getParameterTypes();
            // check if this constructor is belong to a inner class
            // the parameter[0] of inner class's constructor is a instance of outer class
            if (paramTypes[0] == declaringClass && paramTypes.length > argTypes.length) {
                throw new IllegalArgumentException("Invalid instantiation of inner class; use newInnerInstance instead");
            } else {
                String argTypesDesc = ParameterReflection.getParameterTypesDescription(argTypes);
                throw new IllegalArgumentException("No compatible constructor found: " + theClass.getSimpleName() + argTypesDesc);
            }
        }
    }

    // ensure that field is accessible
    public static void makeAccessible(AccessibleObject classMember) {
        if (!classMember.isAccessible()) {
            classMember.setAccessible(true);
        }
    }
}
