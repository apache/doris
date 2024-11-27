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

package org.apache.doris.regression.util

import groovy.transform.CompileStatic
import org.apache.doris.regression.suite.ScriptContext
import org.apache.doris.regression.suite.SuiteContext
import org.apache.tools.ant.util.DateUtils

@CompileStatic
class TeamcityUtils {
    static String postfix = ""
    static String prefix = ""

    static String getSuiteName(String name) {
        if (prefix != "") {
            name = prefix + "-" + name
        }
        if (postfix != "") {
            name = name + "-" + postfix
        }
        return name
    }

    static String formatNow() {
        return DateUtils.format(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    }

    static String formatStdOut(SuiteContext suiteContext, String msg) {
        String timestamp = formatNow()
        String name = getSuiteName(suiteContext.flowName)
        return "##teamcity[testStdOut name='${name}' out='${escape(msg)}' flowId='${suiteContext.flowId}' timestamp='${timestamp}']"
    }

    static String formatStdErr(SuiteContext suiteContext, String msg) {
        String timestamp = formatNow()
        String name = getSuiteName(suiteContext.flowName)
        return "##teamcity[testStdErr name='${name}' out='${escape(msg)}' flowId='${suiteContext.flowId}' timestamp='${timestamp}']"
    }

    static void testStarted(SuiteContext suiteContext) {
        String timestamp = formatNow()
        String name = getSuiteName(suiteContext.flowName)
        println("##teamcity[flowStarted flowId='${suiteContext.flowId}' timestamp='${timestamp}']")
        println("##teamcity[testStarted name='${name}' flowId='${suiteContext.flowId}' timestamp='${timestamp}']")
    }

    static void testFailed(SuiteContext suiteContext, String msg, String details) {
        String timestamp = formatNow()
        String name = getSuiteName(suiteContext.flowName)
        println("##teamcity[testFailed name='${name}' message='${escape(msg)}' flowId='${suiteContext.flowId}' details='${escape(details)}' timestamp='${timestamp}']")
    }

    static void testFinished(SuiteContext suiteContext, long elapsed) {
        String timestamp = formatNow()
        String name = getSuiteName(suiteContext.flowName)
        println("##teamcity[testFinished name='${name}' flowId='${suiteContext.flowId}' duration='${elapsed}' timestamp='${timestamp}']")
        println("##teamcity[flowFinished flowId='${suiteContext.flowId}' timestamp='${timestamp}']")
    }

    static String escape(String str) {
        StringBuilder sb = new StringBuilder()
        char[] chars = str.toCharArray()
        for (int i = 0; i < chars.length; ++i) {
            char c = chars[i]

            switch (c) {
                case '|': sb.append("||"); break
                case '\'': sb.append("|'"); break
                case '[': sb.append("|["); break
                case ']': sb.append("|]"); break
                case '\n': sb.append("|n"); break
                case '\r': sb.append("|r"); break
                case '\\':
                    if (i + 5 < chars.length && chars[i + 1] == 'u' && isHex(chars[i + 2]) && isHex(chars[i + 3]) && isHex(chars[i + 4]) && isHex(chars[i + 5])) {
                        sb.append("|0x")
                        ++i
                        break
                    }
                default: sb.append(c)
            }
        }
        return sb.toString()
    }

    static boolean isHex(char c) {
        return (('0' as char) <= c && c <= ('9' as char)) || (('A' as char) <= c && c <= ('F' as char)) || (('a' as char) <= c && c <= ('f' as char))
    }
}
