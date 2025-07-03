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

public class JdbcClientException extends RuntimeException {
    public JdbcClientException(String format, Throwable cause, Object... msg) {
        super(formatMessage(format, msg), cause);
    }

    public JdbcClientException(String format, Object... msg) {
        super(formatMessage(format, msg));
    }

    private static String formatMessage(String format, Object... msg) {
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
        StringBuilder sb = new StringBuilder();
        while (throwable != null) {
            String message = throwable.getMessage();
            if (message != null && !message.isEmpty()) {
                if (sb.length() > 0) {
                    sb.append(" | Caused by: ");
                }
                sb.append(message);
            }
            throwable = throwable.getCause();
        }
        return sb.toString();
    }
}
