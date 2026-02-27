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

import com.google.re2j.Pattern;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility to convert a restricted glob pattern into a regex.
 *
 * Supported glob syntax:
 * - '*' matches any sequence of characters
 * - '?' matches any single character
 * - '[...]' matches any character in the brackets
 * - '[!...]' matches any character not in the brackets
 * - '\\' escapes the next character
 */
public final class GlobRegexUtil {
    // Small LRU to cap compiled pattern memory
    private static final int REGEX_CACHE_CAPACITY = 256;
    private static final Map<String, Pattern> REGEX_CACHE = new LinkedHashMap<String, Pattern>(
            REGEX_CACHE_CAPACITY, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Pattern> eldest) {
            return size() > REGEX_CACHE_CAPACITY;
        }
    };

    private GlobRegexUtil() {
    }

    public static Pattern getOrCompilePattern(String globPattern) {
        synchronized (REGEX_CACHE) {
            Pattern cached = REGEX_CACHE.get(globPattern);
            if (cached != null) {
                return cached;
            }
            String regex = globToRegex(globPattern);
            Pattern compiled = Pattern.compile(regex);
            REGEX_CACHE.put(globPattern, compiled);
            return compiled;
        }
    }

    public static String globToRegex(String pattern) {
        StringBuilder regexBuilder = new StringBuilder();
        regexBuilder.append("^");
        boolean isEscaped = false;
        int patternLength = pattern.length();
        for (int index = 0; index < patternLength; index++) {
            char currentChar = pattern.charAt(index);
            if (isEscaped) {
                appendEscapedRegexChar(regexBuilder, currentChar);
                isEscaped = false;
                continue;
            }
            if (currentChar == '\\') {
                isEscaped = true;
                continue;
            }
            if (currentChar == '*') {
                regexBuilder.append(".*");
                continue;
            }
            if (currentChar == '?') {
                regexBuilder.append('.');
                continue;
            }
            if (currentChar == '[') {
                int classIndex = index + 1;
                boolean classClosed = false;
                boolean isClassEscaped = false;
                StringBuilder classBuffer = new StringBuilder();
                if (classIndex < patternLength
                        && (pattern.charAt(classIndex) == '!' || pattern.charAt(classIndex) == '^')) {
                    classBuffer.append('^');
                    classIndex++;
                }
                for (; classIndex < patternLength; classIndex++) {
                    char classChar = pattern.charAt(classIndex);
                    if (isClassEscaped) {
                        classBuffer.append(classChar);
                        isClassEscaped = false;
                        continue;
                    }
                    if (classChar == '\\') {
                        isClassEscaped = true;
                        continue;
                    }
                    if (classChar == ']') {
                        classClosed = true;
                        break;
                    }
                    classBuffer.append(classChar);
                }
                if (!classClosed) {
                    throw new IllegalArgumentException("Unclosed character class in glob pattern: " + pattern);
                }
                regexBuilder.append('[').append(classBuffer).append(']');
                index = classIndex;
                continue;
            }
            appendEscapedRegexChar(regexBuilder, currentChar);
        }
        if (isEscaped) {
            appendEscapedRegexChar(regexBuilder, '\\');
        }
        regexBuilder.append("$");
        return regexBuilder.toString();
    }

    private static void appendEscapedRegexChar(StringBuilder regexBuilder, char ch) {
        switch (ch) {
            case '.':
            case '^':
            case '$':
            case '+':
            case '*':
            case '?':
            case '(':
            case ')':
            case '|':
            case '{':
            case '}':
            case '[':
            case ']':
            case '\\':
                regexBuilder.append('\\').append(ch);
                break;
            default:
                regexBuilder.append(ch);
                break;
        }
    }
}
