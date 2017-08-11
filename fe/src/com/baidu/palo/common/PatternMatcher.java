// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common;

import com.google.common.base.Strings;

import java.util.regex.Pattern;

// Wrap for Java pattern and matcher
public class PatternMatcher {
    private Pattern pattern;

    public boolean match(String candidate) {
        if (pattern == null || candidate == null) {
            // No pattern, how can I explain this? Return false now.
            // No candidate, return false.
            return false;
        }
        if (pattern.matcher(candidate).matches()) {
            return true;
        }
        return false;
    }

    private static String convertMysqlPattern(String mysqlPattern)  {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mysqlPattern.length(); ++i) {
            char ch = mysqlPattern.charAt(i);
            switch (ch) {
                case '%':
                    sb.append(".*");
                    break;
                case '_':
                    sb.append(".");
                    break;
                case '\\': {
                    if (i == mysqlPattern.length() - 1) {
                        // Last character of this pattern.
                        sb.append(mysqlPattern.charAt(i));
                    } else {
                        sb.append(mysqlPattern.charAt(++i));
                    }
                    break;
                }
                default:
                    sb.append(ch);
                    break;
            }
        }
        // Replace all the '\' to '\'.'\' in Java pattern
        mysqlPattern = sb.toString();
        sb = new StringBuilder();
        for (int i = 0; i < mysqlPattern.length(); ++i) {
            char ch = mysqlPattern.charAt(i);
            switch (ch) {
                case '\\':
                    sb.append('\\').append('\\');
                    break;
                default:
                    sb.append(ch);
                    break;
            }
        }

        return sb.toString();
    }

    public static PatternMatcher createMysqlPattern(String mysqlPattern) throws AnalysisException {
        PatternMatcher matcher = new PatternMatcher();

        // Match nothing
        mysqlPattern = Strings.nullToEmpty(mysqlPattern);

        String javaPattern = convertMysqlPattern(mysqlPattern);
        try {
            matcher.pattern = Pattern.compile(javaPattern, Pattern.CASE_INSENSITIVE);
        } catch (Exception e) {
            throw new AnalysisException("Bad pattern in SQL.");
        }
        return matcher;
    }
}
