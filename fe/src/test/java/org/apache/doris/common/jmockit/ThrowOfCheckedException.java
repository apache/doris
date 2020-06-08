/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

/**
 * Modify from mockit.internal.reflection.ThrowOfCheckedException JMockit v1.13
 */
public final class ThrowOfCheckedException {
    private static Exception exceptionToThrow;

    ThrowOfCheckedException() throws Exception {
        throw exceptionToThrow;
    }

    public static synchronized void doThrow(Exception checkedException) {
        exceptionToThrow = checkedException;
        ConstructorReflection.newInstanceUsingDefaultConstructor(ThrowOfCheckedException.class);
    }
}
