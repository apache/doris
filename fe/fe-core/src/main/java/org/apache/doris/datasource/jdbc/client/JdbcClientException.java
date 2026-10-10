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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.connector.spi.DiagnosticException;
import org.apache.doris.jni.toolkit.jdbc.JdbcExceptionUtils;

public class JdbcClientException extends RuntimeException implements DiagnosticException {
    private final String[] sensitiveValues;

    public JdbcClientException(String format, Throwable cause, Object... msg) {
        super(JdbcExceptionUtils.appendSqlDiagnostics(formatMessage(format, msg), cause), cause);
        sensitiveValues = new String[0];
    }

    public JdbcClientException(String format, Object... msg) {
        super(JdbcExceptionUtils.redact(formatMessage(format, msg)));
        sensitiveValues = new String[0];
    }

    JdbcClientException(Throwable cause, String diagnosticMessage, String... sensitiveValues) {
        super(diagnosticMessage, cause);
        this.sensitiveValues = sensitiveValues.clone();
    }

    @Override
    public String getDiagnosticMessage() {
        return getMessage();
    }

    @Override
    public String getDiagnosticStackTrace(Throwable error) {
        return JdbcExceptionUtils.stackTrace(error, sensitiveValues);
    }

    static String formatMessage(String format, Object... msg) {
        if (msg == null || msg.length == 0) {
            return format;
        } else {
            return String.format(format, escapePercentInArgs(msg));
        }
    }

    private static Object[] escapePercentInArgs(Object... args) {
        if (args == null) {
            return null;
        }
        Object[] escapedArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof String) {
                escapedArgs[i] = ((String) args[i]).replace("%", "%%");
            } else {
                escapedArgs[i] = args[i];
            }
        }
        return escapedArgs;
    }

    public static String getAllExceptionMessages(Throwable throwable) {
        return JdbcExceptionUtils.format("", throwable);
    }

    public static String getAllExceptionMessages(Throwable throwable, String... sensitiveValues) {
        return JdbcExceptionUtils.format("", throwable, sensitiveValues);
    }
}
