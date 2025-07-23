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
import java.util.regex.Matcher
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@CompileStatic
class PromethuesChecker {
    private static final Logger log = LoggerFactory.getLogger(PromethuesChecker.class)

    static boolean regexp(String s) {
        if (s == null) return false
        s =~ /^[a-zA-Z_][a-zA-Z0-9_]*(\{[a-zA-Z_][a-zA-Z0-9_]*="[^"]+"(,[a-zA-Z_][a-zA-Z0-9_]*="[^"]+")*\})?\s+-?([0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?)$/
    }

    static boolean check(String str) {
        // counter gauge summary histogram
        String type = ""
        if (str == null || str.trim().isEmpty()) return false

        def lines = str.split('\n')
        boolean allValid = true

        for (String line : lines) {
            line = line.trim()
            if (line.isEmpty()) continue
            if (line.startsWith("# HELP ")) continue

            if (line.startsWith("# TYPE ")) {
                def matcher = (line =~ /^# TYPE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(counter|gauge|histogram|summary)$/) as Matcher
                if (matcher.matches()) {
                    type = matcher.group(2)?.toLowerCase()
                } else {
                    allValid = false
                }
            } else {
                switch (type) {
                    case "counter":
                    case "gauge":
                    case "summary":
                    case "histogram":
                        if (!regexp(line)) {
                            log.info("invalid metric format ${line} type ${type}, please check regexp or metric format".toString())
                            allValid = false
                        }
                        break
                    default:
                        allValid = false
                        log.info("unknow metric type ${type}".toString())
                        break
                }
            }
        }

        return allValid
    }
}
