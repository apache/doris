/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

import java.lang.reflect.Proxy;

/**
 * Modify from mockit.internal.util.GeneratedClasses JMockit v1.13
 * Helper class to return type of mocked-object
 */
public final class GeneratedClasses {
    private static final String IMPLCLASS_PREFIX = "$Impl_";
    private static final String SUBCLASS_PREFIX = "$Subclass_";

    private GeneratedClasses() {
    }

    static boolean isGeneratedImplementationClass(Class<?> mockedType) {
        return isGeneratedImplementationClass(mockedType.getName());
    }

    static boolean isGeneratedImplementationClass(String className) {
        return className.contains(IMPLCLASS_PREFIX);
    }

    static boolean isGeneratedSubclass(String className) {
        return className.contains(SUBCLASS_PREFIX);
    }

    static boolean isGeneratedClass(String className) {
        return isGeneratedSubclass(className) || isGeneratedImplementationClass(className);
    }

    static Class<?> getMockedClassOrInterfaceType(Class<?> aClass) {
        if (!Proxy.isProxyClass(aClass) && !isGeneratedImplementationClass(aClass)) {
            return isGeneratedSubclass(aClass.getName()) ? aClass.getSuperclass() : aClass;
        } else {
            return aClass.getInterfaces()[0];
        }
    }

    static Class<?> getMockedClass(Object mock) {
        return getMockedClassOrInterfaceType(mock.getClass());
    }
}
