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
import org.apache.doris.regression.util.CloseableIterator

import java.sql.Connection

@Slf4j
@CompileStatic
class SuiteContext implements Closeable {
    public final File file
    public final Connection conn
    public final Config config
    public final File dataPath
    public final File outputFile
    public final Recorder recorder
//    public final File tmpOutputPath
    public final CloseableIterator<Iterator<List<String>>> outputIterator
    private volatile OutputUtils.OutputBlocksWriter outputBlocksWriter

    SuiteContext(File file, Connection conn, Config config, Recorder recorder) {
        this.file = file
        this.conn = conn
        this.config = config
        this.recorder = recorder

        def path = new File(config.suitePath).relativePath(file)
        def outputRelativePath = path.substring(0, path.lastIndexOf(".")) + ".out"
        this.outputFile = new File(new File(config.dataPath), outputRelativePath)
        this.dataPath = this.outputFile.getParentFile().getCanonicalFile()
        if (!config.otherConfigs.getProperty("qt.generate.out", "false").toBoolean()
                && outputFile.exists()) {
            this.outputIterator = OutputUtils.iterator(outputFile)
        }
//        def dataParentPath = new File(config.dataPath).parentFile.absolutePath
//        def tmpOutputPath = "${dataParentPath}/tmp_output/${outputRelativePath}".toString()
//        this.tmpOutputPath = new File(tmpOutputPath)
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

    @Override
    void close() {
        if (outputIterator != null) {
            try {
                outputIterator.close()
            } catch (Throwable t) {
                log.warn("Close outputFile failed", t)
            }
        }

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
