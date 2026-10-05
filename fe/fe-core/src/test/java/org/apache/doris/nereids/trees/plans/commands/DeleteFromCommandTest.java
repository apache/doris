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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

public class DeleteFromCommandTest {

    @Test
    public void testBuildDeleteFallbackExceptionPreservesBothFailureCauses() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception("initial predicate failure");
        Exception fallbackException = new Exception("fallback execution failure");

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify the merged exception surfaces the fallback failure and keeps the initial failure.
        Assertions.assertEquals(
                "Delete fallback execution failed: fallback execution failure"
                        + ". Initial predicate check failed: initial predicate failure",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testBuildDeleteFallbackExceptionFallsBackToThrowableToString() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception((String) null);
        Exception fallbackException = new Exception((String) null);

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify null messages still produce debuggable text.
        Assertions.assertEquals(
                "Delete fallback execution failed: java.lang.Exception"
                        + ". Initial predicate check failed: java.lang.Exception",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    // Use reflection to validate the helper without exposing it only for tests.
    private AnalysisException invokeBuildDeleteFallbackException(DeleteFromCommand command,
            Exception initialException, Exception fallbackException)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = DeleteFromCommand.class.getDeclaredMethod("buildDeleteFallbackException",
                Exception.class, Exception.class);
        method.setAccessible(true);
        return (AnalysisException) method.invoke(command, initialException, fallbackException);
    }
}
