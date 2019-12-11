package org.apache.doris.common;

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
