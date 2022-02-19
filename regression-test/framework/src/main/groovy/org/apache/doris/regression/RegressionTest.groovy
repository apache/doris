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

import groovy.transform.CompileStatic
import jodd.util.Wildcard
import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.Recorder
import groovy.util.logging.Slf4j
import org.apache.commons.cli.*
import org.apache.doris.regression.util.SuiteInfo
import org.codehaus.groovy.control.CompilerConfiguration

import java.util.stream.Collectors

@Slf4j
@CompileStatic
class RegressionTest {
    static ClassLoader classloader
    static CompilerConfiguration compileConfig
    static GroovyShell shell

    static void main(String[] args) {
        CommandLine cmd = ConfigOptions.initCommands(args)
        if (cmd == null) {
            return
        }

        Config config = Config.fromCommandLine(cmd)
        initGroovyEnv(config)
        Recorder recorder = runSuites(config)
        printResult(config, recorder)
    }

    static void initGroovyEnv(Config config) {
        classloader = new GroovyClassLoader()
        compileConfig = new CompilerConfiguration()
        compileConfig.setScriptBaseClass((Suite as Class).name)
        shell = new GroovyShell(classloader, new Binding(), compileConfig)
    }

    static List<File> findSuiteFiles(String root) {
        if (root == null) {
            log.warn("Not specify suite path")
            return new ArrayList<File>()
        }
        List<File> files = new ArrayList<>()
        new File(root).eachFileRecurse { f ->
            if (f.isFile() && f.name.endsWith(".groovy")) {
                files.add(f)
            }
        }
        return files
    }

    static String parseGroup(Config config, File suiteFile) {
        String group = new File(config.suitePath).relativePath(suiteFile)
        int separatorIndex = group.lastIndexOf(File.separator)
        if (separatorIndex == -1) {
            return ""
        } else {
            return group.substring(0, separatorIndex)
        }
    }

    static Recorder runSuites(Config config) {
        def files = findSuiteFiles(config.suitePath)
        def recorder = new Recorder()
        List<SuiteFile> runScripts = files.stream().map({ file ->
            String suiteName = file.name.substring(0, file.name.lastIndexOf("."))
            String group = parseGroup(config, file)
            return new SuiteFile(file, suiteName, group)
        }).filter({ sf ->
            canRun(config, sf.suiteName, sf.group)
        }).collect(Collectors.toList())

        log.info("Start to run suites")
        int totalFile = runScripts.size()
        runScripts.eachWithIndex { sf, i ->
            File file = sf.file
            String suiteName = sf.suiteName
            String group = sf.group
            def suiteConn = config.getConnection()
            new SuiteContext(file, suiteConn, config, recorder).withCloseable { context ->
                try {
                    log.info("[${i + 1}/${totalFile}] Run ${suiteName} in $file".toString())
                    Suite suite = shell.parse(file) as Suite
                    suite.init(suiteName, group, context)
                    suite.run()
                    recorder.onSuccess(new SuiteInfo(file, group, suiteName))
                    log.info("Run ${suiteName} in ${file.absolutePath} succeed".toString())
                } catch (Throwable t) {
                    recorder.onFailure(new SuiteInfo(file, group, suiteName))
                    log.error("Run ${suiteName} in ${file.absolutePath} failed".toString(), t)
                }
            }
        }
        return recorder
    }

    static boolean canRun(Config config, String suiteName, String group) {
        Set<String> suiteGroups = group.split(",").collect {g -> g.trim()}.toSet()
        if (config.suiteWildcard.size() == 0 ||
                (suiteName != null && (config.suiteWildcard.any {
                    suiteWildcard -> Wildcard.match(suiteName, suiteWildcard)
                }))) {

            if (config.groups == null || config.groups.isEmpty()
                    || !config.groups.intersect(suiteGroups).isEmpty()) {
                return true
            }
        }
        return false
    }

    static void printResult(Config config, Recorder recorder) {
        int allSuiteNum = recorder.successList.size() + recorder.failureList.size()
        int failedSuiteNum = recorder.failureList.size()
        log.info("Test ${allSuiteNum} suites, failed ${failedSuiteNum} suites".toString())

        // print success list
        {
            String successList = recorder.successList.collect { info ->
                "${info.file.absolutePath}: group=${info.group}, name=${info.suiteName}"
            }.join("\n")
            log.info("successList suites:\n${successList}".toString())
        }

        // print failure list
        if (!recorder.failureList.isEmpty()) {
            def failureList = recorder.failureList.collect() { info ->
                "${info.file.absolutePath}: group=${info.group}, name=${info.suiteName}"
            }.join("\n")
            log.info("Failure suites:\n${failureList}".toString())
            printFailed()
            throw new IllegalStateException("Test failed")
        } else {
            printPassed()
        }
    }

    static void printPassed() {
        log.info("""All suites success.
                 | ____   _    ____ ____  _____ ____  
                 ||  _ \\ / \\  / ___/ ___|| ____|  _ \\ 
                 || |_) / _ \\ \\___ \\___ \\|  _| | | | |
                 ||  __/ ___ \\ ___) |__) | |___| |_| |
                 ||_| /_/   \\_\\____/____/|_____|____/                         
                 |""".stripMargin())
    }

    static void printFailed() {
        log.info("""Some suites failed.
                 | _____ _    ___ _     _____ ____  
                 ||  ___/ \\  |_ _| |   | ____|  _ \\ 
                 || |_ / _ \\  | || |   |  _| | | | |
                 ||  _/ ___ \\ | || |___| |___| |_| |
                 ||_|/_/   \\_\\___|_____|_____|____/ 
                 |""".stripMargin())
    }

    static class SuiteFile {
        File file
        String suiteName
        String group

        SuiteFile(File file, String suiteName, String group) {
            this.file = file
            this.suiteName = suiteName
            this.group = group
        }
    }
}
