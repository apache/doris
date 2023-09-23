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

package org.apache.doris.regression.suite

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.util.logging.Slf4j

@Slf4j
@CompileStatic
abstract class SuiteScript extends Script {
    public ScriptContext context
    public final Logger logger = LoggerFactory.getLogger(getClass())

    void init(ScriptContext scriptContext) {
        this.context = scriptContext
    }

    void suite(String suiteName, String group = getDefaultGroups(new File(context.config.suitePath), context.file), Closure suiteBody) {
        if (!group.split(',').any {
            def match = it =~ /^p\d+$/
            if (match.find())
                return true
            }) {
            group +=",p0"
        }

        if (!context.suiteFilter.call(suiteName, group)) {
            return
        }

        log.info("run ${context.file.absolutePath}")
        try {
            context.createAndRunSuite(suiteName, group, suiteBody)
        } catch (Throwable t) {
            log.warn("Unexcept exception when run ${suiteName} in ${context.file.absolutePath} failed", t)
        }
    }

    static String getDefaultGroups(File suiteRoot, File scriptFile) {
        String path = suiteRoot.relativePath(scriptFile.parentFile)
        String groupPath = path;
        if (path.indexOf(File.separator + "sql") > 0) {
            groupPath = path.substring(0, path.indexOf(File.separator + "sql"))
        }
        List<String> groups = ["default"]

        def grouped_p = groupPath.split(File.separator)
            .collect {it.trim()}
            .findAll {it != "." && it != ".." && !it.isEmpty()}
            .reverse()
            .any {
                def match = it =~ /_p\d+$/
                if (match.find()) {
                    groups.add(match.group(0).substring(1))
                    return true
                }
            }

        if (!grouped_p) {
             // There is no specified group, mark it as p0
             groups.add("p0")
        }
        return groups.join(",")
    }
}
