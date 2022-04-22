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

import groovy.util.logging.Slf4j
import org.apache.doris.regression.util.SqlUtils

interface ScriptSource {
    SuiteScript toScript(ScriptContext scriptContext, GroovyShell shell)
    File getFile()
}

class GroovyFileSource implements ScriptSource {
    private File file

    GroovyFileSource(File file) {
        this.file = file
    }

    @Override
    SuiteScript toScript(ScriptContext scriptContext, GroovyShell shell) {
        SuiteScript suiteScript = shell.parse(file) as SuiteScript
        suiteScript.init(scriptContext)
        return suiteScript
    }

    @Override
    File getFile() {
        return file
    }
}

@Slf4j
class SqlFileSource implements ScriptSource {
    private File suiteRoot
    private File file

    SqlFileSource(File suiteRoot, File file) {
        this.suiteRoot = suiteRoot
        this.file = file
    }

    String getGroup() {
        return SuiteScript.getDefaultGroups(suiteRoot, file)
    }

    @Override
    SuiteScript toScript(ScriptContext scriptContext, GroovyShell shell) {
        String suiteName = file.name.substring(0, file.name.lastIndexOf("."))
        String groupName = getGroup()
        boolean order = suiteName.endsWith("_order")
        String tag = suiteName
        String sql = file.text

        List<String> sqls
        try {
            sqls = SqlUtils.splitAndGetNonEmptySql(sql)
        } catch (Throwable t) {
            sqls = [sql]
            log.warn("Try to execute whole file text as one sql, because can not split sql:\n${sql}", t)
        }

        SuiteScript script = new SuiteScript() {
            @Override
            Object run() {
                suite(suiteName, groupName) {
                    for (int i = 0; i < sqls.size(); ++i) {
                        String singleSql = sqls.get(i)
                        String tagName = (i == 0) ? tag : "${tag}_${i + 1}"
                        quickTest(tagName, singleSql, order)
                    }
                }
            }
        }
        script.init(scriptContext)
        return script
    }

    @Override
    File getFile() {
        return file
    }
}
