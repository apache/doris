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
import com.google.common.collect.Sets;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

// Wrap for Java pattern and matcher
public class PatternMatcher {
    public static final PatternMatcher MATCH_ANY = new PatternMatcher(Pattern.compile(".*"));
    private Pattern pattern;
    // The name of 'user', 'database' and 'table' don't support complex matching in grant statement.
    // Only using '%' to match any string. In other cases, it's string case-sensitive(or not) equivalent matching,
    // so using the origin string to determine whether it matches.
    private String originString;
    private boolean caseSensitive;

    private static final Set<Character> FORBIDDEN_CHARS = Sets.newHashSet('<', '(', '[', '{', '^', '=',
                                                                          '$', '!', '|', ']', '}', ')',
                                                                          '?', '*', '+', '>', '@');

    public PatternMatcher(Pattern pattern) {
        this.pattern = pattern;
    }

    public PatternMatcher(String originString, boolean caseSensitive) {
        this.originString = caseSensitive ? originString : originString.toLowerCase(Locale.ROOT);
        this.caseSensitive = caseSensitive;
    }

    public boolean match(String candidate) {
        if (candidate == null) {
            return false;
        }
        if (pattern != null) {
            return pattern.matcher(candidate).matches();
        }
        if (caseSensitive) {
            return candidate.equals(originString);
        } else {
            return candidate.toLowerCase(Locale.ROOT).equals(originString);
        }
    }

    /**
     * Use in grant statement to support case-sensitive(or not) equivalent matching.
     *
     * @param originString The string to match.
     * @param caseSensitive Case sensitive.
     */
    public static PatternMatcher createFlatPattern(String originString, boolean caseSensitive) {
        return createFlatPattern(originString, caseSensitive, false);
    }

    /**
     * Use in grant statement to support case-sensitive(or not) equivalent matching, or arbitrary matching.
     *
     * @param originString The string to match. If matchAny = true, this parameter has no effect.
     * @param caseSensitive Case sensitive.
     * @param matchAny match any string.
     */
    public static PatternMatcher createFlatPattern(
            String originString, boolean caseSensitive, boolean matchAny) {
        if (matchAny) {
            return MATCH_ANY;
        }
        return new PatternMatcher(originString, caseSensitive);
    }

    /*
     * Mysql has only 2 patterns.
     * '%' to match any character sequence
     * '_' to master any single character.
     * So we convert '%' to '.*', and '_' to '.'
     *
     * eg:
     *      abc% -> abc.*
     *      ab_c -> ab.c
     *
     * We also need to handle escape character '\'.
     * User use '\' to escape reserved words like '%', '_', or '\' it self
     *
     * eg:
     *      ab\%c = ab%c
     *      ab\_c = ab_c
     *      ab\\c = ab\c
     *
     * We also have to ignore meaningless '\' like：'ab\c', convert it to 'abc'.
     * The following characters are not permitted:
     *   <([{^=$!|]})?*+>
     */
    private static String convertMysqlPattern(String mysqlPattern) throws PatternMatcherException {
        String newMysqlPattern = mysqlPattern;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < newMysqlPattern.length(); ++i) {
            char ch = newMysqlPattern.charAt(i);
            checkPermittedCharactor(ch);
            switch (ch) {
                case '%':
                    sb.append(".*");
                    break;
                case '.':
                    sb.append("\\.");
                    break;
                case '_':
                    sb.append(".");
                    break;
                case '\\': {
                    if (i == newMysqlPattern.length() - 1) {
                        // last character of this pattern. leave this '\' as it is
                        sb.append('\\');
                        break;
                    }
                    // we need to look ahead the next character
                    // to decide ignore this '\' or treat it as escape character.
                    char nextChar = newMysqlPattern.charAt(i + 1);
                    switch (nextChar) {
                        case '%':
                        case '_':
                        case '\\':
                            // this is a escape character, eat this '\' and get next character.
                            sb.append(nextChar);
                            ++i;
                            break;
                        default:
                            // ignore this '\' and continue;
                            break;
                    }
                    break;
                }
                default:
                    sb.append(ch);
                    break;
            }
        }

        // Replace all the '\' to '\\' in Java pattern
        newMysqlPattern = sb.toString();
        sb = new StringBuilder();
        for (int i = 0; i < newMysqlPattern.length(); ++i) {
            char ch = newMysqlPattern.charAt(i);
            switch (ch) {
                case '\\':
                    if (i == newMysqlPattern.length() - 1) {
                        // last character of this pattern. leave this '\' as it is
                        sb.append('\\').append('\\');
                        break;
                    }
                    // look ahead
                    if (newMysqlPattern.charAt(i + 1) == '.') {
                        // leave '\.' as it is.
                        sb.append('\\').append('.');
                        i++;
                        break;
                    }
                    sb.append('\\').append('\\');
                    break;
                default:
                    sb.append(ch);
                    break;
            }
        }

        // System.out.println("result: " + sb.toString());
        return sb.toString();
    }

    private static void checkPermittedCharactor(char c) throws PatternMatcherException {
        if (FORBIDDEN_CHARS.contains(c)) {
            throw new PatternMatcherException("Forbidden charactor: '" + c + "'");
        }
    }

    public static PatternMatcher createMysqlPattern(String mysqlPattern, boolean caseSensitive)
            throws PatternMatcherException {
        PatternMatcher matcher;

        // Match nothing
        String newMysqlPattern = Strings.nullToEmpty(mysqlPattern);

        String javaPattern = convertMysqlPattern(newMysqlPattern);
        try {
            if (caseSensitive) {
                matcher = new PatternMatcher(Pattern.compile(javaPattern));
            } else {
                matcher = new PatternMatcher(Pattern.compile(javaPattern, Pattern.CASE_INSENSITIVE));
            }
        } catch (Exception e) {
            throw new PatternMatcherException("Bad pattern in SQL: " + e.getMessage());
        }
        return matcher;
    }
}
