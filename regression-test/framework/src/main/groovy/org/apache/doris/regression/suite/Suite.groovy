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

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

import com.google.common.collect.ImmutableList
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.OutputUtils
import org.apache.doris.regression.action.ExplainAction
import org.apache.doris.regression.action.StreamLoadAction
import org.apache.doris.regression.action.SuiteAction
import org.apache.doris.regression.action.TestAction
import org.apache.doris.regression.util.JdbcUtils
import org.junit.jupiter.api.Assertions

import static org.apache.doris.regression.util.DataUtils.sortByToString

@Slf4j
abstract class Suite extends Script implements GroovyInterceptable {
    SuiteContext context
    String name
    String group

    void init(String name, String group, SuiteContext context) {
        this.name = name
        this.group = group
        this.context = context
    }

    String getConf(String key, String defaultValue = null) {
        String value = context.config.otherConfigs.get(key)
        return value == null ? defaultValue : value
    }

    Properties getConfs(String prefix) {
        Properties p = new Properties()
        for (String name : context.config.otherConfigs.stringPropertyNames()) {
            if (name.startsWith(prefix + ".")) {
                p.put(name.substring(prefix.length() + 1), context.config.getProperty(name))
            }
        }
        return p
    }

    String toCsv(List<Object> rows) {
        StringBuilder sb = new StringBuilder()
        for (int i = 0; i < rows.size(); ++i) {
            Object row = rows.get(i)
            if (!(row instanceof List)) {
                row = ImmutableList.of(row)
            }
            sb.append(OutputUtils.toCsvString(row as List)).append("\n")
        }
        sb.toString()
    }

    Object parseJson(String str) {
        def jsonSlurper = new JsonSlurper()
        return jsonSlurper.parseText(str)
    }

    Object sql(String sqlStr, boolean isOrder = false) {
        log.info("Execute sql: ${sqlStr}".toString())
        def result = JdbcUtils.executeToList(context.conn, sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    Object order_sql(String sqlStr) {
        return sql(sqlStr,  true)
    }

    List<List<Object>> sortRows(List<List<Object>> result) {
        if (result == null) {
            return null
        }
        return DataUtils.sortByToString(result)
    }

    void explain(Closure actionSupplier) {
        runAction(new ExplainAction(context), actionSupplier)
    }

    void test(Closure actionSupplier) {
        runAction(new TestAction(context), actionSupplier)
    }

    void streamLoad(Closure actionSupplier) {
        runAction(new StreamLoadAction(context), actionSupplier)
    }

    void runAction(SuiteAction action, Closure actionSupplier) {
        actionSupplier.setDelegate(action)
        actionSupplier.setResolveStrategy(Closure.DELEGATE_FIRST)
        actionSupplier.call()
        action.run()
    }

    void quickTest(String tag, String sql, boolean order = false) {
        log.info("Execute tag: ${tag}, sql: ${sql}".toString())

        if (context.config.generateOutputFile || context.config.forceGenerateOutputFile) {
            def result = JdbcUtils.executorToStringList(context.conn, sql)
            if (order) {
                result = sortByToString(result)
            }
            Iterator<List<Object>> realResults = result.iterator()
            // generate and save to .out file
            def writer = context.getOutputWriter(context.config.forceGenerateOutputFile)
            writer.write(realResults, tag)
        } else {
            if (context.outputIterator == null) {
                String res = "Missing outputFile: ${context.outputFile.getAbsolutePath()}"
                List excelContentList = [context.file.getName(), context.file, context.file, res]
                context.recorder.reportDiffResult(excelContentList)
                throw new IllegalStateException("Missing outputFile: ${context.outputFile.getAbsolutePath()}")
            }

            if (!context.outputIterator.hasNext()) {
                String res = "Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}"
                List excelContentList = [context.file.getName(), tag, context.file, res]
                context.recorder.reportDiffResult(excelContentList)
                throw new IllegalStateException("Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}")
            }

            try {
                Iterator<List<Object>> expectCsvResults = context.outputIterator.next() as Iterator
                List<List<Object>> realResults = JdbcUtils.executorToStringList(context.conn, sql)
                if (order) {
                    realResults = sortByToString(realResults)
                }
                def res = OutputUtils.assertEquals(expectCsvResults, realResults.iterator(), "Tag '${tag}' wrong")
                if (res) {
                    List excelContentList = [context.file.getName(), tag, sql.trim(), res]
                    context.recorder.reportDiffResult(excelContentList)
                    throw new IllegalStateException("'${tag}' line not match . Detailed results is : '${res}'")
                }
            } catch (Throwable t) {
                if (t.toString().contains('line not match . Detailed results is')) {
                    throw t
                } else {
                    List excelContentList = [context.file.getName(), tag, sql.trim(), t]
                    context.recorder.reportDiffResult(excelContentList)
                    throw new IllegalStateException("'${tag}' run failed . Detailed failure information is : '${t}'", t)
                }
            }
        }
    }

    @Override
    Object invokeMethod(String name, Object args) {
        // qt: quick test
        if (name.startsWith("qt_")) {
            return quickTest(name.substring("qt_".length()), (args as Object[])[0] as String)
        } else if (name.startsWith("order_qt_")) {
            return quickTest(name.substring("order_qt_".length()), (args as Object[])[0] as String, true)
        } else if (name.startsWith("assert") && name.length() > "assert".length()) {
            // delegate to junit Assertions dynamically
            return Assertions."$name"(*args) // *args: spread-dot
        } else if (name.startsWith("try_")) {
            String realMethod = name.substring("try_".length())
            try {
                return this."$realMethod"(*args)
            } catch (Throwable t) {
                // do nothing
            }
        } else {
            // invoke origin method
            return metaClass.invokeMethod(this, name, args)
        }
    }

    private Object invokeAssertions(String name, Object args) {

    }
}

