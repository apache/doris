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

package org.apache.doris.jni.toolkit.jdbc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** JDBC diagnostics shared by FE metadata clients and BE readers/writers. */
public final class JdbcExceptionUtils {
    private static final Pattern REMOTE_CODES = Pattern.compile(
            "\\[remote_sqlstate=(?:[A-Za-z0-9]{5}|unknown), remote_vendor_error_code=-?[0-9]+,");
    private static final Pattern JDBC_URL = Pattern.compile("(?i)jdbc:[^\\s\"'<>]+");
    private static final Pattern CREDENTIAL = Pattern.compile(
            "(?i)(password|passwd|pwd|user|username|token|secret|access[_-]?key)(\\s*[=:]\\s*)"
                    + "(\"[^\"]*\"|'[^']*'|[^\\s;&,]+)");

    private JdbcExceptionUtils() {
    }

    /**
     * Collects both JDBC chains, with identity-based cycle detection. Identical diagnostics are
     * emitted once, but errors with the same message and different remote codes stay distinct.
     * Non-SQL exceptions retain their messages without inventing remote error codes.
     */
    public static String format(String context, Throwable error, String... sensitiveValues) {
        return format(context, error, true, sensitiveValues);
    }

    /** Appends only SQL diagnostics, preserving non-SQL exception messages and formatting. */
    public static String appendSqlDiagnostics(String context, Throwable error, String... sensitiveValues) {
        return format(context, error, false, sensitiveValues);
    }

    private static String format(String context, Throwable error, boolean includeMessages, String[] sensitiveValues) {
        // RuntimeException constructors allow an absent cause; retain that existing JDBC API behavior.
        if (error == null) {
            return redact(context, sensitiveValues);
        }
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        Set<String> messages = new LinkedHashSet<>();
        Set<String> sqlMessages = new LinkedHashSet<>();
        Set<String> diagnostics = new LinkedHashSet<>();
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.add(error);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            String message = redact(current.getMessage(), sensitiveValues);
            if (current instanceof SQLException) {
                SQLException sqlError = (SQLException) current;
                String sqlState = sqlError.getSQLState();
                sqlState = sqlState == null || sqlState.trim().isEmpty() ? "unknown" : sqlState;
                diagnostics.add("[remote_sqlstate=" + sqlState
                        + ", remote_vendor_error_code=" + sqlError.getErrorCode()
                        + ", message=" + message + "]");
                sqlMessages.add(message);
                if (sqlError.getNextException() != null) {
                    pending.addLast(sqlError.getNextException());
                }
            } else if (includeMessages && !message.isEmpty()) {
                messages.add(message);
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
        }
        messages.removeAll(sqlMessages);
        String safeContext = redact(context, sensitiveValues);
        diagnostics.removeIf(diagnostic -> safeContext.contains(diagnostic)
                || messages.stream().anyMatch(message -> message.contains(diagnostic)));
        messages.remove(safeContext);
        messages.addAll(diagnostics);
        String details = String.join(" | ", messages);
        return safeContext.isEmpty() ? details : safeContext + (details.isEmpty() ? "" : ": " + details);
    }

    /** Redacts configured secrets and JDBC URLs before diagnostic text crosses an output boundary. */
    public static String redact(String text, String... sensitiveValues) {
        if (text == null) {
            return "";
        }
        // Remote codes are scalar diagnostics, not credential text. In particular a short
        // numeric password must not change SQLSTATE or the vendor code on repeated formatting.
        Matcher codes = REMOTE_CODES.matcher(text);
        StringBuilder result = new StringBuilder();
        int start = 0;
        while (codes.find()) {
            result.append(redactText(text.substring(start, codes.start()), sensitiveValues));
            result.append(codes.group());
            start = codes.end();
        }
        return result.append(redactText(text.substring(start), sensitiveValues)).toString();
    }

    private static String redactText(String text, String[] sensitiveValues) {
        String result = text;
        for (String value : sensitiveValues) {
            if (value != null && !value.isEmpty()) {
                result = result.replace(value, "***");
            }
        }
        result = JDBC_URL.matcher(result).replaceAll("[redacted JDBC URL]");
        return CREDENTIAL.matcher(result).replaceAll("$1$2***");
    }

    public static String stackTrace(Throwable error, String... sensitiveValues) {
        StringWriter output = new StringWriter();
        error.printStackTrace(new PrintWriter(output));
        return redact(output.toString(), sensitiveValues);
    }
}
