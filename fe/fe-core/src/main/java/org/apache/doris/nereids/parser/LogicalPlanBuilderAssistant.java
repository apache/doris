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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Logical plan builder assistant for buildIn dialect and other dialect.
 * The same logical in {@link org.apache.doris.nereids.parser.LogicalPlanBuilder}
 * can be extracted to here.
 */
public class LogicalPlanBuilderAssistant {

    // acording to MySQL's dev doc: https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
    private static final Set<Character> NEED_TRIM_CHARS_FOR_ALIAS
            = ImmutableSet.of('\0', '\b', '\n', '\r', '\t', '\032', ' ');

    private LogicalPlanBuilderAssistant() {
    }

    /**
     * EscapeBackSlash such \n, \t
     */
    public static String escapeBackSlash(String str) {
        StringBuilder sb = new StringBuilder();
        int strLen = str.length();
        for (int i = 0; i < strLen; ++i) {
            char c = str.charAt(i);
            if (c == '\\' && (i + 1) < strLen) {
                switch (str.charAt(i + 1)) {
                    case 'n':
                        sb.append('\n');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case '0':
                        sb.append('\0'); // Ascii null
                        break;
                    case 'Z': // ^Z must be escaped on Win32
                        sb.append('\032');
                        break;
                    case '_':
                    case '%':
                        sb.append('\\'); // remember prefix for wildcard
                        sb.append(str.charAt(i + 1));
                        break;
                    default:
                        sb.append(str.charAt(i + 1));
                        break;
                }
                i++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * MySQL will trim any inviable char and blank at the beginning of the string,
     * and stop at the first \0 if it exists. we follow the behavior.
     *
     * @param value the StringLikeLiteral value
     * @return the alias generated from the value
     */
    public static String getStringLiteralAlias(String value) {
        int i = 0;
        while (i < value.length() && NEED_TRIM_CHARS_FOR_ALIAS.contains(value.charAt(i))) {
            i++;
        }
        int j = i;
        while (j < value.length() && value.charAt(j) != '\0') {
            j++;
        }
        return value.substring(i, j);
    }

    /**
     * Wrap plan withCheckPolicy.
     */
    public static LogicalPlan withCheckPolicy(LogicalPlan plan) {
        return new LogicalCheckPolicy<>(plan);
    }
}
