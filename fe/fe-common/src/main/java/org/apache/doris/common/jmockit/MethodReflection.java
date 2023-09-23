/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Modify from mockit.internal.util.MethodReflection JMockit v1.13
 * Util class to get and invoke method from specified class.
 */
public final class MethodReflection {
    private MethodReflection() {
    }

    public static <T> T invoke(Class<?> theClass, Object targetInstance, String methodName, Object... methodArgs) {
        if (theClass == null || methodName == null) {
            throw new IllegalArgumentException();
        }
        boolean staticMethod = targetInstance == null;
        Class<?>[] argTypes = ParameterReflection.getArgumentTypesFromArgumentValues(methodArgs);
        Method method = staticMethod ? findCompatibleStaticMethod(theClass, methodName, argTypes) : findCompatibleMethod(theClass, methodName, argTypes);
        if (staticMethod && !Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException("Attempted to invoke non-static method without an instance to invoke it on");
        } else {
            T result = invoke(targetInstance, method, methodArgs);
            return result;
        }
    }

    public static <T> T invoke(Object targetInstance, Method method, Object... methodArgs) {
        if (method == null || methodArgs == null) {
            throw new IllegalArgumentException();
        }
        makeAccessible(method);

        try {
            return (T) method.invoke(targetInstance, methodArgs);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failure to invoke method: " + method, e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                ThrowOfCheckedException.doThrow((Exception) cause);
                return null;
            }
        }
    }

    /**
     * Get a static method with {@methodName String} and {@argTypes Class<?>[]}.
     * If no method was found, a IllegalArgumentException will be thrown.
     */
    private static Method findCompatibleStaticMethod(Class<?> theClass, String methodName, Class<?>[] argTypes) {
        if (theClass == null || methodName == null || argTypes == null) {
            throw new IllegalArgumentException();
        }
        Method methodFound = findCompatibleMethodInClass(theClass, methodName, argTypes);
        if (methodFound != null) {
            return methodFound;
        } else {
            String argTypesDesc = ParameterReflection.getParameterTypesDescription(argTypes);
            throw new IllegalArgumentException("No compatible static method found: " + methodName + argTypesDesc);
        }
    }

    /**
     * Get a non-static method with {@methodName String} and {@argTypes Class<?>[]}.
     */
    public static Method findCompatibleMethod(Class<?> theClass, String methodName, Class<?>[] argTypes) {
        if (theClass == null || methodName == null || argTypes == null) {
            throw new IllegalArgumentException();
        }
        Method methodFound = findCompatibleMethodIfAvailable(theClass, methodName, argTypes);
        if (methodFound != null) {
            return methodFound;
        } else {
            String argTypesDesc = ParameterReflection.getParameterTypesDescription(argTypes);
            throw new IllegalArgumentException("No compatible method found: " + methodName + argTypesDesc);
        }
    }

    /**
     * Get method with {@methodName String} and {@argTypes Class<?>[]} from {@theClass Class<?>}.
     * If more than one method is found, choose the more specific one. (i.e. method with parameters that have more concrete types is more specific)
     */
    private static Method findCompatibleMethodInClass(Class<?> theClass, String methodName, Class<?>[] argTypes) {
        if (theClass == null || methodName == null || argTypes == null) {
            throw new IllegalArgumentException();
        }
        Method found = null;
        Class<?>[] foundParamTypes = null;
        Method[] methods = theClass.getDeclaredMethods();

        for (Method declaredMethod : methods) {
            if (declaredMethod.getName().equals(methodName)) {
                Class<?>[] declaredParamTypes = declaredMethod.getParameterTypes();
                int gap = declaredParamTypes.length - argTypes.length;
                if (gap == 0 && (ParameterReflection.matchesParameterTypes(declaredParamTypes, argTypes)
                        || ParameterReflection.acceptsArgumentTypes(declaredParamTypes, argTypes))
                        && (foundParamTypes == null
                            || ParameterReflection.hasMoreSpecificTypes(declaredParamTypes, foundParamTypes))) {
                    found = declaredMethod;
                    foundParamTypes = declaredParamTypes;
                }
            }
        }

        return found;
    }

    /**
     * Get method with {@methodName String} and {@argTypes Class<?>[]} from {@theClass Class<?>} as well as its super class.
     * If more than one method is found, choose the more specify one. (i.e. choose the method with parameters that have more concrete types)
     */
    private static Method findCompatibleMethodIfAvailable(Class<?> theClass, String methodName, Class<?>[] argTypes) {
        if (theClass == null || methodName == null || argTypes == null) {
            throw new IllegalArgumentException();
        }
        Method methodFound = null;

        while (true) {
            Method compatibleMethod = findCompatibleMethodInClass(theClass, methodName, argTypes);
            if (compatibleMethod != null && (methodFound == null || ParameterReflection.hasMoreSpecificTypes(compatibleMethod.getParameterTypes(), methodFound.getParameterTypes()))) {
                methodFound = compatibleMethod;
            }

            Class<?> superClass = theClass.getSuperclass();
            if (superClass == null || superClass == Object.class) {
                return methodFound;
            }

            theClass = superClass;
        }
    }



    // ensure that field is accessible
    public static void makeAccessible(AccessibleObject classMember) {
        if (!classMember.isAccessible()) {
            classMember.setAccessible(true);
        }
    }

    // return true if the two types are same type.
    private static boolean isSameType(Class<?> firstType, Class<?> secondType) {
        return firstType == secondType
                || firstType.isPrimitive() && firstType == AutoType.getPrimitiveType(secondType)
                || secondType.isPrimitive() && secondType == AutoType.getPrimitiveType(firstType);
    }
}
