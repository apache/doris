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

package org.apache.doris.nereids.util;

import org.apache.doris.qe.SqlModeHelper;

/**
 * Utilities for decoding and rendering SQL string literals.
 */
public final class SqlLiteralUtils {

    private SqlLiteralUtils() {
    }

    /**
     * Decode backslash escape sequences used in SQL string literals.
     */
    public static String unescapeBackSlash(String value) {
        StringBuilder result = new StringBuilder();
        int length = value.length();
        for (int i = 0; i < length; ++i) {
            char current = value.charAt(i);
            if (current == '\\' && (i + 1) < length) {
                switch (value.charAt(i + 1)) {
                    case 'n':
                        result.append('\n');
                        break;
                    case 't':
                        result.append('\t');
                        break;
                    case 'r':
                        result.append('\r');
                        break;
                    case 'b':
                        result.append('\b');
                        break;
                    case '0':
                        result.append('\0');
                        break;
                    case 'Z':
                        result.append('\032');
                        break;
                    case '_':
                    case '%':
                        result.append('\\');
                        result.append(value.charAt(i + 1));
                        break;
                    default:
                        result.append(value.charAt(i + 1));
                        break;
                }
                i++;
            } else {
                result.append(current);
            }
        }
        return result.toString();
    }

    /**
     * Decode a STRING_LITERAL token according to the current SQL mode.
     */
    public static String parseStringLiteral(String text) {
        String value = text.substring(1, text.length() - 1);
        if (text.charAt(0) == '\'') {
            value = value.replace("''", "'");
        } else {
            value = value.replace("\"\"", "\"");
        }
        return SqlModeHelper.hasNoBackSlashEscapes() ? value : unescapeBackSlash(value);
    }

    /**
     * Quote a value as a STRING_LITERAL that can be parsed under the current SQL mode.
     */
    public static String quoteStringLiteral(String value) {
        String escaped = SqlModeHelper.hasNoBackSlashEscapes()
                ? value : value.replace("\\", "\\\\");
        return "\"" + escaped.replace("\"", "\"\"") + "\"";
    }
}
