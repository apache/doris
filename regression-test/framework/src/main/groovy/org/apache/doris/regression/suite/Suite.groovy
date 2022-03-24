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

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import groovy.json.JsonSlurper
import com.google.common.collect.ImmutableList
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.OutputUtils
import org.apache.doris.regression.action.ExplainAction
import org.apache.doris.regression.action.StreamLoadAction
import org.apache.doris.regression.action.SuiteAction
import org.apache.doris.regression.action.TestAction
import org.apache.doris.regression.util.JdbcUtils
import org.junit.jupiter.api.Assertions
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors
import java.util.stream.LongStream

import static org.apache.doris.regression.util.DataUtils.sortByToString

abstract class Suite extends Script implements GroovyInterceptable {
    SuiteContext context
    String name
    String group
    final Logger logger = LoggerFactory.getLogger(getClass())

    final List<Closure> successCallbacks = new Vector<>()
    final List<Closure> failCallbacks = new Vector<>()
    final List<Closure> finishCallbacks = new Vector<>()
    final List<Throwable> lazyCheckExceptions = new Vector<>()
    final List<Future> lazyCheckFutures = new Vector<>()

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

    void onSuccess(Closure callback) {
        successCallbacks.add(callback)
    }

    void onFail(Closure callback) {
        failCallbacks.add(callback)
    }

    void onFinish(Closure callback) {
        finishCallbacks.add(callback)
    }

    LongStream range(long startInclusive, long endExclusive) {
        return LongStream.range(startInclusive, endExclusive)
    }

    LongStream rangeClosed(long startInclusive, long endInclusive) {
        return LongStream.rangeClosed(startInclusive, endInclusive)
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

    public <T> T lazyCheck(Closure<T> closure) {
        try {
            T result = closure.call()
            if (result instanceof Future) {
                lazyCheckFutures.add(result)
            }
            return result
        } catch (Throwable t) {
            lazyCheckExceptions.add(t)
            return null
        }
    }

    void doLazyCheck() {
        if (!lazyCheckExceptions.isEmpty()) {
            throw lazyCheckExceptions.get(0)
        }
        lazyCheckFutures.forEach { it.get() }
    }

    public <T> Tuple2<T, Long> timer(Closure<T> actionSupplier) {
        long startTime = System.currentTimeMillis()
        T result = actionSupplier.call()
        long endTime = System.currentTimeMillis()
        return [result, endTime - startTime]
    }

    public <T> ListenableFuture<T> thread(String threadName = null, Closure<T> actionSupplier) {
        return MoreExecutors.listeningDecorator(context.executorService).submit((Callable<T>) {
            def originThreadName = Thread.currentThread().name
            try {
                Thread.currentThread().setName(threadName == null ? originThreadName : threadName)
                return actionSupplier.call()
            } finally {
                try {
                    context.closeThreadLocal()
                } catch (Throwable t) {
                    logger.warn("Close thread local context failed", t)
                }
                Thread.currentThread().setName(originThreadName)
            }
        })
    }

    public <T> ListenableFuture<T> lazyCheckThread(String threadName = null, Closure<T> actionSupplier) {
        return lazyCheck {
            thread(threadName, actionSupplier)
        }
    }

    public <T> ListenableFuture<T> combineFutures(ListenableFuture<T> ... futures) {
        return Futures.allAsList(futures)
    }

    public <T> ListenableFuture<List<T>> combineFutures(Iterable<? extends ListenableFuture<? extends T>> futures) {
        return Futures.allAsList(futures)
    }

    public <T> T connect(String user = context.config.jdbcUser, String password = context.config.jdbcPassword,
                         String url = context.config.jdbcUrl, Closure<T> actionSupplier) {
        return context.connect(user, password, url, actionSupplier)
    }

    List<List<Object>> sql(String sqlStr, boolean isOrder = false) {
        logger.info("Execute sql: ${sqlStr}".toString())
        def result = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> order_sql(String sqlStr) {
        return sql(sqlStr,  true)
    }

    List<List<Object>> sortRows(List<List<Object>> result) {
        if (result == null) {
            return null
        }
        return DataUtils.sortByToString(result)
    }

    String selectUnionAll(List list) {
        def toSelectString = { Object value ->
            if (value == null) {
                return "null"
            } else if (value instanceof Number) {
                return value.toString()
            } else {
                return "'${value.toString()}'".toString()
            }
        }
        AtomicBoolean isFirst = new AtomicBoolean(true)
        String sql = list.stream()
            .map({ row ->
                StringBuilder sb = new StringBuilder("SELECT ")
                if (row instanceof List) {
                    if (isFirst.get()) {
                        String columns = row.withIndex().collect({ column, index ->
                            "${toSelectString(column)} AS c${index + 1}"
                        }).join(", ")
                        sb.append(columns)
                        isFirst.set(false)
                    } else {
                        String columns = row.collect({ column ->
                            "${toSelectString(column)}"
                        }).join(", ")
                        sb.append(columns)
                    }
                } else {
                    if (isFirst.get()) {
                        sb.append(toSelectString(row)).append(" AS c1")
                        isFirst.set(false)
                    } else {
                        sb.append(toSelectString(row))
                    }
                }
                return sb.toString()
            }).collect(Collectors.joining("\nUNION ALL\n"))
        return sql
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
        logger.info("Execute tag: ${tag}, sql: ${sql}".toString())

        if (context.config.generateOutputFile || context.config.forceGenerateOutputFile) {
            def result = JdbcUtils.executorToStringList(context.getConnection(), sql)
            if (order) {
                result = sortByToString(result)
            }
            Iterator<List<Object>> realResults = result.iterator()
            // generate and save to .out file
            def writer = context.getOutputWriter(context.config.forceGenerateOutputFile)
            writer.write(realResults, tag)
        } else {
            if (!context.outputFile.exists()) {
                String res = "Missing outputFile: ${context.outputFile.getAbsolutePath()}"
                List excelContentList = [context.file.getName(), context.file, context.file, res]
                context.recorder.reportDiffResult(excelContentList)
                throw new IllegalStateException("Missing outputFile: ${context.outputFile.getAbsolutePath()}")
            }

            if (!context.getOutputIterator().hasNextTagBlock(tag)) {
                String res = "Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}"
                List excelContentList = [context.file.getName(), tag, context.file, res]
                context.recorder.reportDiffResult(excelContentList)
                throw new IllegalStateException("Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}")
            }

            OutputUtils.TagBlockIterator expectCsvResults = context.getOutputIterator().next()
            List<List<Object>> realResults = JdbcUtils.executorToStringList(context.getConnection(), sql)
            if (order) {
                realResults = sortByToString(realResults)
            }
            String errorMsg = null
            try {
                errorMsg = OutputUtils.checkOutput(expectCsvResults, realResults.iterator(), "Check tag '${tag}' failed")
            } catch (Throwable t) {
                List excelContentList = [context.file.getName(), tag, sql.trim(), t]
                context.recorder.reportDiffResult(excelContentList)
                throw new IllegalStateException("Check tag '${tag}' failed", t)
            }
            if (errorMsg != null) {
                List excelContentList = [context.file.getName(), tag, sql.trim(), errorMsg]
                context.recorder.reportDiffResult(excelContentList)
                throw new IllegalStateException(errorMsg)
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
                return null
            }
        } else {
            // invoke origin method
            return metaClass.invokeMethod(this, name, args)
        }
    }
}

