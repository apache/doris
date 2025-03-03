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

package org.apache.doris.regression.action

import ch.qos.logback.core.rolling.RollingFileAppender
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import groovy.util.logging.Slf4j
import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.suite.SuiteContext
import org.openjdk.jmc.common.IMCFrame
import org.openjdk.jmc.common.IMCMethod
import org.openjdk.jmc.common.IMCStackTrace
import org.openjdk.jmc.common.item.IItem
import org.openjdk.jmc.common.item.IItemCollection
import org.openjdk.jmc.common.item.IItemIterable
import org.openjdk.jmc.common.item.ItemFilters
import org.openjdk.jmc.common.item.ItemToolkit
import org.openjdk.jmc.flightrecorder.JfrAttributes
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit
import org.openjdk.jmc.flightrecorder.jdk.JdkAttributes
import org.openjdk.jmc.flightrecorder.jdk.JdkTypeIDs
import org.slf4j.LoggerFactory

@Slf4j
class FlightRecordAction implements SuiteAction {
    private SuiteContext context
    private boolean cleanUp = true
    private Closure record
    private Closure callback

    private static File logPath
    private String processName = "DorisFE"
    private List<String> extraConfig = []

    static {
        ch.qos.logback.classic.Logger loggerOfSuite =
                LoggerFactory.getLogger(Suite.class) as ch.qos.logback.classic.Logger
        def context = loggerOfSuite.getLoggerContext()
        logPath = new File("log")
        for (final def logger in context.getLoggerList()) {
            def it = logger.iteratorForAppenders()
            while (it.hasNext()) {
                def appender = it.next()
                if (appender instanceof RollingFileAppender) {
                    logPath = new File(new File(appender.getFile()).parentFile.absolutePath)
                    log.info("Log path: ${logPath.getPath()}")
                    break
                }
            }
        }
    }

    FlightRecordAction(SuiteContext context) {
        this.context = context
    }

    private void cleanUp(boolean cleanUp) {
        this.cleanUp = cleanUp
    }

    private void processName(String processName) {
        this.processName = processName
    }

    private void extraConfig(List<String> extraConfig) {
        this.extraConfig = extraConfig
    }

    void record(Closure record) {
        this.record = record
    }

    void callback(@ClosureParams(value = FromString, options = [
            "org.openjdk.jmc.common.item.IItemCollection",
            "org.openjdk.jmc.common.item.IItemCollection,Throwable,Long,Long"]) Closure<Boolean> callback) {
        this.callback = callback
    }

    static long getAllocationBytes(IItemCollection collection, boolean filterNereids) {
        IItemCollection allocSample = collection.apply(ItemFilters.type(JdkTypeIDs.OBJ_ALLOC_SAMPLE))

        long allocationBytes = 0
        for (IItemIterable iItems : allocSample) {
            for (IItem iItem : iItems) {
                if (filterNereids) {
                    IMCStackTrace stackTrace = ItemToolkit.getItemType(iItem)
                            .getAccessor(JfrAttributes.EVENT_STACKTRACE.getKey())
                            .getMember(iItem)
                    if (stackTrace == null) {
                        continue
                    }

                    boolean isNereids = false
                    for (IMCFrame frame : stackTrace.getFrames()) {
                        IMCMethod method = frame.getMethod()
                        String methodName = method.getMethodName()
                        String fullName = method.getType().getFullName()
                        if (fullName.equals("org.apache.doris.qe.StmtExecutor") && methodName.equals("executeByNereids")) {
                            isNereids = true
                            break
                        }
                    }
                    if (!isNereids) {
                        continue
                    }
                }

                long weightBytes = ItemToolkit.getItemType(iItem).getAccessor(JdkAttributes.SAMPLE_WEIGHT.getKey())
                        .getMember(iItem).longValue()
                allocationBytes += weightBytes
            }
        }
        return allocationBytes
    }

    @Override
    void run() {
        Optional<String> recordName = Optional.empty()
        try {
            def startTime = System.currentTimeMillis()
            if (record == null || callback == null) {
                callbackError(startTime, new IllegalStateException("record or callback is null"))
                return
            }

            def fePid = getFePid(startTime)
            if (!fePid.isPresent()) {
                return
            }

            recordName = startRecord(startTime, fePid.get())
            if (!recordName.isPresent()) {
                return
            }

            try {
                this.record.call()
            } catch (Throwable t) {
                callbackError(startTime, t)
                return
            }

            def stopResult = stopRecord(startTime, fePid.get(), recordName.get())
            if (!stopResult.isPresent()) {
                return
            }

            IItemCollection records = null
            try {
                records = JfrLoaderToolkit.loadEvents(getRecordFile(recordName.get()))
            } catch (Throwable t) {
                callbackError(startTime, new IllegalStateException("Parse records falied: " + t.toString(), t))
                return
            }

            if (callback.parameterTypes.size() == 1) {
                callback(records)
            } else {
                callback(records, null, startTime, System.currentTimeMillis())
            }
        } finally {
            if (cleanUp && recordName.isPresent()) {
                def file = getRecordFile(recordName.get())
                log.info("cleanup: ${file.absolutePath}")
                try {
                    file.delete()
                } catch (Throwable t) {
                    // ignore
                }
            }
        }
    }

    private Optional<String> stopRecord(long startTime, int pid, String name) {
        log.info("stop record")
        def stopResult = exec(["jcmd", "${pid}", "JFR.stop", "name=${name}"].execute())
        if (stopResult.exception != null || stopResult.exitCode != 0) {
            Throwable e = new IllegalStateException("Can not stop record flight: " + new String(stopResult.stderr), stopResult.exception)
            callbackError(startTime, e)
            return Optional.empty()
        }
        log.info(new String(stopResult.stdout))
        return Optional.of(new String(stopResult.stdout))
    }

    private Optional<String> startRecord(long startTime, int pid) {
        String name = "flight_record_${startTime}".toString()
        String file = getRecordFile(name).absolutePath
        log.info("start record: ${file}")
        List<String> commands = ["jcmd", "${pid}", "JFR.start", "name=${name}", "filename=${file}"]
        if (extraConfig != null) {
            commands.addAll(extraConfig)
        }
        def startResult = exec(commands.execute())
        if (startResult.exception != null || startResult.exitCode != 0) {
            Throwable e = new IllegalStateException("Can not start record flight: " + new String(startResult.stderr), startResult.exception)
            callbackError(startTime, e)
            return Optional.empty()
        }
        log.info(new String(startResult.stdout))
        return Optional.of(name)
    }

    private File getRecordFile(String name) {
        return new File(logPath,  name + ".jfr")
    }

    private Optional<Integer> getFePid(long startTime) {
        def fePidResult = exec(["jps"].execute()
                .pipeTo(["grep", processName].execute())
                .pipeTo(["awk", "{print \$1}"].execute())
                .pipeTo(["head", "-1"].execute()))
        if (fePidResult.exception != null || fePidResult.exitCode != 0) {
            Throwable e = new IllegalStateException("Can not get frontend pid: " + new String(fePidResult.stderr), fePidResult.exception)
            callbackError(startTime, e)
            return Optional.empty()
        }

        def pidStr = new String(fePidResult.stdout)
        if (pidStr.isEmpty()) {
            throw new IllegalStateException("Can not found process: ${processName}")
        }
        int fePid = pidStr.toInteger()
        log.info("fe pid: ${fePid}")
        return Optional.of(fePid)
    }

    private void callbackError(long startTime, Throwable exception) {
        if (callback.parameterTypes.size() == 1) {
            throw exception
        }
        callback(null, exception, startTime, System.currentTimeMillis())
    }

    private ProcessResult exec(Process process) {
        def exitCode = 0
        try {
            exitCode = process.waitFor()
            return new ProcessResult(
                    process,
                    exitCode,
                    process.inputStream.bytes,
                    process.errorStream.bytes,
                    null
            )
        } catch (Throwable t) {
            return new ProcessResult(
                process,
                exitCode,
                null,
                process.errorStream.bytes,
                t
            )
        }
    }

    private static class ProcessResult {
        Process process
        int exitCode
        byte[] stdout
        byte[] stderr
        Throwable exception

        ProcessResult(Process process, int exitCode, byte[] stdout, byte[] stderr, Throwable exception) {
            this.process = process
            this.exitCode = exitCode
            this.stdout = stdout
            this.stderr = stderr
            this.exception = exception
        }
    }
}
