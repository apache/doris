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

package org.apache.doris.regression

import com.google.common.collect.Lists
import groovy.transform.CompileStatic
import jodd.util.Wildcard
import org.apache.doris.regression.suite.event.EventListener
import org.apache.doris.regression.suite.GroovyFileSource
import org.apache.doris.regression.suite.ScriptContext
import org.apache.doris.regression.suite.ScriptSource
import org.apache.doris.regression.suite.SqlFileSource
import org.apache.doris.regression.suite.event.RecorderEventListener
import org.apache.doris.regression.suite.event.StackEventListeners
import org.apache.doris.regression.suite.SuiteScript
import org.apache.doris.regression.suite.event.TeamcityEventListener
import org.apache.doris.regression.util.Recorder
import groovy.util.logging.Slf4j
import org.apache.commons.cli.*
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.codehaus.groovy.control.CompilerConfiguration

import java.beans.Introspector
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.function.Predicate

@Slf4j
@CompileStatic
class RegressionTest {

    static ClassLoader classloader
    static CompilerConfiguration compileConfig
    static GroovyShell shell
    static ExecutorService scriptExecutors
    static ExecutorService suiteExecutors
    static ExecutorService singleSuiteExecutors
    static ExecutorService actionExecutors
    static ThreadLocal<Integer> threadLoadedClassNum = new ThreadLocal<>()
    static final int cleanLoadedClassesThreshold = 20
    static String nonConcurrentTestGroup = "nonConcurrent"

    static void main(String[] args) {
        CommandLine cmd = ConfigOptions.initCommands(args)
        if (cmd == null) {
            return
        }

        Config config = Config.fromCommandLine(cmd)
        initGroovyEnv(config)
        boolean success = true
        for (int i = 0; i < config.times; i++) {
            log.info("=== run ${i} time ===")
            Recorder recorder = runScripts(config)
            success = printResult(config, recorder)
        }
        actionExecutors.shutdown()
        suiteExecutors.shutdown()
        singleSuiteExecutors.shutdown()
        scriptExecutors.shutdown()
        log.info("Test finished")
        if (!success) {
            System.exit(1)
        }
    }

    static void initGroovyEnv(Config config) {
        log.info("parallel = ${config.parallel}, suiteParallel = ${config.suiteParallel}, actionParallel = ${config.actionParallel}")
        classloader = new GroovyClassLoader()
        compileConfig = new CompilerConfiguration()
        compileConfig.setScriptBaseClass((SuiteScript as Class).name)
        shell = new GroovyShell(classloader, new Binding(), compileConfig)

        BasicThreadFactory scriptFactory = new BasicThreadFactory.Builder()
            .namingPattern("script-thread-%d")
            .priority(Thread.MAX_PRIORITY)
            .build();
        scriptExecutors = Executors.newFixedThreadPool(config.parallel, scriptFactory)

        BasicThreadFactory suiteFactory = new BasicThreadFactory.Builder()
            .namingPattern("suite-thread-%d")
            .priority(Thread.MAX_PRIORITY)
            .build();
        suiteExecutors = Executors.newFixedThreadPool(config.suiteParallel, suiteFactory)

        BasicThreadFactory singleSuiteFactory = new BasicThreadFactory.Builder()
            .namingPattern("non-concurrent-thread-%d")
            .priority(Thread.MAX_PRIORITY)
            .build();
        singleSuiteExecutors = Executors.newFixedThreadPool(1, singleSuiteFactory)

        BasicThreadFactory actionFactory = new BasicThreadFactory.Builder()
            .namingPattern("action-thread-%d")
            .priority(Thread.MAX_PRIORITY)
            .build();
        actionExecutors = Executors.newFixedThreadPool(config.actionParallel, actionFactory)

        loadPlugins(config)
    }

    static List<ScriptSource> findScriptSources(String root, Predicate<String> directoryFilter,
                                                Predicate<String> fileFilter) {
        if (root == null) {
            log.warn('Not specify suite path')
            return new ArrayList<ScriptSource>()
        }
        List<ScriptSource> sources = new ArrayList<>()
        // 1. generate groovy for sql, excluding ddl
        def rootFile = new File(root)
        rootFile.eachFileRecurse { f ->
            if (f.isFile() && f.name.endsWith('.sql') && f.getParentFile().name != "ddl"
                    && fileFilter.test(f.name) && directoryFilter.test(f.getParent())) {
                sources.add(new SqlFileSource(rootFile, f))
            }
        }

        // 2. collect groovy sources.
        rootFile.eachFileRecurse { f ->
            if (f.isFile() && f.name.endsWith('.groovy') && fileFilter.test(f.name)
                    && directoryFilter.test(f.getParent())) {
                sources.add(new GroovyFileSource(f))
            }
        }
        return sources
    }

    static void runScript(Config config, ScriptSource source, Recorder recorder, boolean isSingleThreadScript) {
        def suiteFilter = { String suiteName, String groupName ->
            canRun(config, suiteName, groupName, isSingleThreadScript)
        }
        def file = source.getFile()
        int failureLimit = Integer.valueOf(config.otherConfigs.getOrDefault("max_failure_num", "-1").toString());
        if (Recorder.isFailureExceedLimit(failureLimit)) {
            // too much failure, skip all following suits
            log.warn("too much failure ${Recorder.getFailureOrFatalNum()}, limit ${failureLimit}, skip following suits: ${file}")
            recorder.onSkip(file.absolutePath);
            return;
        }
        def eventListeners = getEventListeners(config, recorder)
        ExecutorService executors = null
        if (isSingleThreadScript) {
            executors = singleSuiteExecutors
        } else {
            executors = suiteExecutors
        }

        new ScriptContext(file, executors, actionExecutors,
                config, eventListeners, suiteFilter).start { scriptContext ->
            try {
                SuiteScript suiteScript = source.toScript(scriptContext, shell)
                suiteScript.run()
            } finally {
                // avoid jvm metaspace oom
                cleanLoadedClassesIfNecessary()
            }
        }
    }

    static void runScripts(Config config, Recorder recorder,
                           Predicate<String> directoryFilter, Predicate<String> fileNameFilter) {
        def scriptSources = findScriptSources(config.suitePath, directoryFilter, fileNameFilter)
        if (config.randomOrder) {
            Collections.shuffle(scriptSources)
        }
//        int totalFile = scriptSources.size()

        List<Future> futures = Lists.newArrayList()
        scriptSources.eachWithIndex { source, i ->
//            log.info("Prepare scripts [${i + 1}/${totalFile}]".toString())
            def future = scriptExecutors.submit {
                runScript(config, source, recorder, false)
            }
            futures.add(future)
        }

        // wait all scripts
        for (Future future : futures) {
            try {
                future.get()
            } catch (Throwable t) {
                // do nothing, because already save to Recorder
            }
        }

        log.info('Start to run single scripts')
        futures.clear()
        scriptSources.eachWithIndex { source, i ->
//            log.info("Prepare scripts [${i + 1}/${totalFile}]".toString())
            def future = scriptExecutors.submit {
                runScript(config, source, recorder, true)
            }
            futures.add(future)
        }

        // wait all scripts
        for (Future future : futures) {
            try {
                future.get()
            } catch (Throwable t) {
                // do nothing, because already save to Recorder
            }
        }
    }

    static Recorder runScripts(Config config) {
        def recorder = new Recorder()
        def directoryFilter = config.getDirectoryFilter()
        if (!config.withOutLoadData) {
            log.info('Start to run load scripts')
            runScripts(config, recorder, directoryFilter,
                    { fileName -> fileName.substring(0, fileName.lastIndexOf(".")) == "load" })
        }
        log.info('Start to run scripts')
        runScripts(config, recorder, directoryFilter, 
                { fileName -> fileName.substring(0, fileName.lastIndexOf(".")) != "load" })

        return recorder
    }

    static boolean filterSuites(Config config, String suiteName) {
        if (config.suiteWildcard.isEmpty() && config.excludeSuiteWildcard.isEmpty()) {
            return true
        }
        if (!config.suiteWildcard.isEmpty() && !config.suiteWildcard.any {
                    suiteWildcard -> Wildcard.match(suiteName, suiteWildcard)
                }) {
            return false
        }
        if (!config.excludeSuiteWildcard.isEmpty() && config.excludeSuiteWildcard.any {
                    excludeSuiteWildcard -> Wildcard.match(suiteName, excludeSuiteWildcard)
                }) {
            return false
        }
        return true
    }

    static boolean filterGroups(Config config, String group) {
        if (config.groups.isEmpty() && config.excludeGroupSet.isEmpty()) {
            return true
        }
        Set<String> suiteGroups = group.split(',').collect { g -> g.trim() }.toSet()
        if (!config.groups.isEmpty() && config.groups.intersect(suiteGroups).isEmpty()) {
            return false
        }
        if (!config.excludeGroupSet.isEmpty() && !config.excludeGroupSet.intersect(suiteGroups).isEmpty()) {
            return false
        }
        return true
    }

    static boolean canRun(Config config, String suiteName, String group, boolean isSingleThreadScript) {
        Set<String> suiteGroups = group.split(',').collect { g -> g.trim() }.toSet();
        if (isSingleThreadScript) {
            if (!suiteGroups.contains(nonConcurrentTestGroup)) {
                return false
            }
        } else {
            if (suiteGroups.contains(nonConcurrentTestGroup)) {
                return false
            }
        }

        return filterGroups(config, group) && filterSuites(config, suiteName)
    }

    static List<EventListener> getEventListeners(Config config, Recorder recorder) {
        StackEventListeners listeners = new StackEventListeners()

        // RecorderEventListener **MUST BE** first listener
        listeners.addListener(new RecorderEventListener(recorder))

        // other listeners
        String stdoutAppenderType = System.getProperty("stdoutAppenderType")
        if (stdoutAppenderType != null && stdoutAppenderType.equalsIgnoreCase("teamcity")) {
            listeners.addListener(new TeamcityEventListener())
        }
        return [listeners] as List<EventListener>
    }

    static void cleanLoadedClassesIfNecessary() {
        Integer loadedClassNum = threadLoadedClassNum.get()
        if (loadedClassNum == null) {
            loadedClassNum = 0
        }
        loadedClassNum += 1
        if (loadedClassNum >= cleanLoadedClassesThreshold) {
            // release dynamic script class: ThreadGroupContext.getContext().beanInfoCache()
            Introspector.flushCaches()
            loadedClassNum = 0
        }
        threadLoadedClassNum.set(loadedClassNum)
    }

    static boolean printResult(Config config, Recorder recorder) {
        int allSuiteNum = recorder.successList.size() + recorder.failureList.size() + recorder.skippedList.size()
        int failedSuiteNum = recorder.failureList.size()
        int fatalScriptNum = recorder.fatalScriptList.size()
        int skippedNum = recorder.skippedList.size()

        // print success list
        if (!recorder.successList.isEmpty()) {
            String successList = recorder.successList.collect { info ->
                "${info.file.absolutePath}: group=${info.group}, name=${info.suiteName}"
            }.join('\n')
            log.info("Success suites:\n${successList}".toString())
        }

        // print skipped list
        if (!recorder.skippedList.isEmpty()) {
            String skippedList = recorder.skippedList.collect { info -> "${info}" }.join('\n')
            log.info("Skipped suites:\n${skippedList}".toString())
        }

        boolean pass = false;
        // print failure list
        if (!recorder.failureList.isEmpty() || !recorder.fatalScriptList.isEmpty()) {
            if (!recorder.failureList.isEmpty()) {
                def failureList = recorder.failureList.collect() { info ->
                    "${info.file.absolutePath}: group=${info.group}, name=${info.suiteName}"
                }.join('\n')
                log.info("Failure suites:\n${failureList}".toString())
            }
            if (!recorder.fatalScriptList.isEmpty()) {
                def failureList = recorder.fatalScriptList.collect() { info ->
                    "${info.file.absolutePath}"
                }.join('\n')
                log.info("Fatal scripts:\n${failureList}".toString())
            }
            printFailed()
        } else {
            printPassed()
            pass = true;
        }

        log.info("Test ${allSuiteNum} suites, failed ${failedSuiteNum} suites, fatal ${fatalScriptNum} scripts, skipped ${skippedNum} scripts".toString())
        return pass;
    }

    static void loadPlugins(Config config) {
        if (config.pluginPath.is(null) || config.pluginPath.isEmpty()) {
            return
        }
        def pluginPath = new File(config.pluginPath)
        if (!pluginPath.exists() || !pluginPath.isDirectory()) {
            return
        }
        pluginPath.eachFileRecurse({ it ->
            if (it.name.endsWith(".groovy")) {
                ScriptContext context = new ScriptContext(it, suiteExecutors, actionExecutors,
                        config, [], { name -> true })
                File pluginFile = it
                context.start({
                    try {
                        SuiteScript pluginScript = new GroovyFileSource(pluginFile).toScript(context, shell)
                        log.info("Begin to load plugin: ${pluginFile.getCanonicalPath()}")
                        pluginScript.run()
                        log.info("Loaded plugin: ${pluginFile.getCanonicalPath()}")
                    } catch (Throwable t) {
                        log.error("Load plugin failed: ${pluginFile.getCanonicalPath()}", t)
                    }
                })
            }
        })
    }

    static void printPassed() {
        log.info('''All suites success.
                 | ____   _    ____ ____  _____ ____
                 ||  _ \\ / \\  / ___/ ___|| ____|  _ \\
                 || |_) / _ \\ \\___ \\___ \\|  _| | | | |
                 ||  __/ ___ \\ ___) |__) | |___| |_| |
                 ||_| /_/   \\_\\____/____/|_____|____/
                 |'''.stripMargin())
    }

    static void printFailed() {
        log.info('''Some suites failed.
                 | _____ _    ___ _     _____ ____
                 ||  ___/ \\  |_ _| |   | ____|  _ \\
                 || |_ / _ \\  | || |   |  _| | | | |
                 ||  _/ ___ \\ | || |___| |___| |_| |
                 ||_|/_/   \\_\\___|_____|_____|____/
                 |'''.stripMargin())
    }
}
