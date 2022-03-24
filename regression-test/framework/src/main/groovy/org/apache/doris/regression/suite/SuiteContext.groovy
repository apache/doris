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
import org.apache.doris.regression.Config
import org.apache.doris.regression.util.OutputUtils
import org.apache.doris.regression.util.Recorder
import groovy.util.logging.Slf4j

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ExecutorService

@Slf4j
@CompileStatic
class SuiteContext implements Closeable {
    public final File file
    private final Connection conn
    public final ThreadLocal<Connection> threadLocalConn = new ThreadLocal<>()
    public final Config config
    public final File dataPath
    public final File outputFile
    public final ThreadLocal<OutputUtils.OutputBlocksIterator> threadLocalOutputIterator = new ThreadLocal<>()
    public final ExecutorService executorService
    public final Recorder recorder
//    public final File tmpOutputPath
    private volatile OutputUtils.OutputBlocksWriter outputBlocksWriter

    SuiteContext(File file, Connection conn, ExecutorService executorService, Config config, Recorder recorder) {
        this.file = file
        this.conn = conn
        this.config = config
        this.executorService = executorService
        this.recorder = recorder

        def path = new File(config.suitePath).relativePath(file)
        def outputRelativePath = path.substring(0, path.lastIndexOf(".")) + ".out"
        this.outputFile = new File(new File(config.dataPath), outputRelativePath)
        this.dataPath = this.outputFile.getParentFile().getCanonicalFile()
//        def dataParentPath = new File(config.dataPath).parentFile.absolutePath
//        def tmpOutputPath = "${dataParentPath}/tmp_output/${outputRelativePath}".toString()
//        this.tmpOutputPath = new File(tmpOutputPath)
    }

    Connection getConnection() {
        def threadConn = threadLocalConn.get()
        if (threadConn != null) {
            return threadConn
        }
        return this.conn
    }

    public <T> T connect(String user, String password, String url, Closure<T> actionSupplier) {
        def originConnection = threadLocalConn.get()
        try {
            log.info("Create new connection for user '${user}'")
            return DriverManager.getConnection(url, user, password).withCloseable { newConn ->
                threadLocalConn.set(newConn)
                return actionSupplier.call()
            }
        } finally {
            log.info("Recover original connection")
            if (originConnection == null) {
                threadLocalConn.remove()
            } else {
                threadLocalConn.set(originConnection)
            }
        }
    }

    OutputUtils.OutputBlocksIterator getOutputIterator() {
        def outputIt = threadLocalOutputIterator.get()
        if (outputIt == null) {
            outputIt = OutputUtils.iterator(outputFile)
            threadLocalOutputIterator.set(outputIt)
        }
        return outputIt
    }

    OutputUtils.OutputBlocksWriter getOutputWriter(boolean deleteIfExist) {
        if (outputBlocksWriter != null) {
            return outputBlocksWriter
        }
        synchronized (this) {
            if (outputBlocksWriter != null) {
                return outputBlocksWriter
            } else if (outputFile.exists() && deleteIfExist) {
                log.info("Delete ${outputFile}".toString())
                outputFile.delete()
                log.info("Generate ${outputFile}".toString())
                outputFile.createNewFile()
                outputBlocksWriter = OutputUtils.writer(outputFile)
            } else if (!outputFile.exists()) {
                outputFile.parentFile.mkdirs()
                outputFile.createNewFile()
                log.info("Generate ${outputFile}".toString())
                outputBlocksWriter = OutputUtils.writer(outputFile)
            } else {
                log.info("Skip generate output file because exists: ${outputFile}".toString())
                outputBlocksWriter = new OutputUtils.OutputBlocksWriter(null)
            }
            return outputBlocksWriter
        }
    }

    void closeThreadLocal() {
        def outputIterator = threadLocalOutputIterator.get()
        if (outputIterator != null) {
            outputIterator.close()
            threadLocalOutputIterator.remove()
        }
    }

    @Override
    void close() {
        closeThreadLocal()

        if (outputBlocksWriter != null) {
            outputBlocksWriter.close()
        }

        try {
            conn.close()
        } catch (Throwable t) {
            log.warn("Close connection failed", t)
        }
    }
}
