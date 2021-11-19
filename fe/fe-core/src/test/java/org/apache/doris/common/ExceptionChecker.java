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

package org.apache.doris.common;

import com.google.common.base.Strings;

import junit.framework.AssertionFailedError;

public class ExceptionChecker {

    /**
     * A runnable that can throw any checked exception.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    public static void expectThrowsNoException(ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            throw new AssertionFailedError(e.getMessage());
        }
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingRunnable runnable) {
        return expectThrows(expectedType,
                "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown", null, runnable);
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and
     * returns it.
     * Will also check if the given `exceptionMsg` is with exception.
     */
    public static <T extends Throwable> T expectThrowsWithMsg(Class<T> expectedType, String exceptionMsg,
            ThrowingRunnable runnable) {
        return expectThrows(expectedType,
                "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown", exceptionMsg,
                runnable);
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, String noExceptionMessage,
            String exceptionMsg, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            e.printStackTrace();
            if (expectedType.isInstance(e)) {
                if (!Strings.isNullOrEmpty(exceptionMsg)) {
                    if (!e.getMessage().contains(exceptionMsg)) {
                        AssertionFailedError assertion = new AssertionFailedError(
                                "expected msg: " + exceptionMsg + ", actual: " + e.getMessage());
                        assertion.initCause(e);
                        throw assertion;
                    }
                }
                return expectedType.cast(e);
            }
            AssertionFailedError assertion = new AssertionFailedError(
                    "Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e);
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError(noExceptionMessage);
    }
}
