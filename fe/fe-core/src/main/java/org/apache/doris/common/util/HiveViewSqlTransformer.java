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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for transforming Hive view SQL to ensure case consistency.
 * This transformer converts SQL keywords, table names, and column names to lowercase
 * while preserving the case of string literals enclosed in quotes.
 */
public class HiveViewSqlTransformer {

    // Pattern to match quoted strings (both single and double quotes)
    private static final Pattern QUOTE_PATTERN = Pattern.compile("(\"[^\"]*\"|'[^']*')");

    /**
     * Formats the input SQL by converting all non-quoted content to lowercase.
     * This helps ensure case consistency when processing Hive view definitions,
     * as Hive is case-insensitive but Doris may be case-sensitive in certain contexts.
     *
     * @param input the original SQL string from Hive view definition
     * @return formatted SQL with non-quoted content in lowercase
     */
    public static String format(String input) {
        if (input == null) {
            return null;
        }
        
        Matcher quoteMatcher = QUOTE_PATTERN.matcher(input);
        StringBuffer result = new StringBuffer();
        int lastIndex = 0;
        
        // Process each quoted string separately
        while (quoteMatcher.find()) {
            // Convert non-quoted content to lowercase
            result.append(input.substring(lastIndex, quoteMatcher.start()).toLowerCase());
            // Preserve quoted content as-is
            result.append(quoteMatcher.group());
            lastIndex = quoteMatcher.end();
        }
        
        // Handle remaining non-quoted content
        if (lastIndex < input.length()) {
            result.append(input.substring(lastIndex).toLowerCase());
        }
        
        return result.toString();
    }
}
