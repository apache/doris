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

import org.apache.doris.common.NereidsException;
import org.apache.doris.connector.spi.DiagnosticException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import javax.annotation.Nullable;

/** Applies connector-owned diagnostic policies at FE exception output boundaries. */
public final class ExceptionDiagnosticUtils {
    private ExceptionDiagnosticUtils() {
    }

    public static String message(Throwable error) {
        DiagnosticException diagnostic = findDiagnosticException(error);
        return diagnostic == null ? ExceptionUtils.getRootCauseMessage(error) : diagnostic.getDiagnosticMessage();
    }

    @Nullable
    public static String sanitizedStackTrace(Throwable error) {
        DiagnosticException diagnostic = findDiagnosticException(error);
        return diagnostic == null ? null : diagnostic.getDiagnosticStackTrace(error);
    }

    /** Keep ordinary Throwable logging while applying connector policies to the full outer exception. */
    public static void log(Logger logger, Level level, String message, Object argument, Throwable error) {
        String trace = sanitizedStackTrace(error);
        if (trace == null) {
            logger.log(level, message, argument, error);
        } else {
            logger.log(level, message + "\n{}", argument, trace);
        }
    }

    @Nullable
    private static DiagnosticException findDiagnosticException(Throwable error) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = error; current != null && visited.add(current);) {
            if (current instanceof DiagnosticException
                    && ((DiagnosticException) current).getDiagnosticMessage() != null) {
                return (DiagnosticException) current;
            }
            current = current instanceof NereidsException
                    ? ((NereidsException) current).getException() : current.getCause();
        }
        return null;
    }
}
