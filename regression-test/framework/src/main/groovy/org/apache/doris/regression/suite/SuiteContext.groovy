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

import com.google.common.collect.Maps
import groovy.transform.CompileStatic
import org.apache.doris.regression.Config
import org.apache.doris.regression.util.OutputUtils
import groovy.util.logging.Slf4j

import java.lang.reflect.UndeclaredThrowableException
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ExecutorService
import java.util.function.Function

class ConnectionInfo {
    Connection conn
    String username
    String password
}

@Slf4j
@CompileStatic
class SuiteContext implements Closeable {
    public final File file
    public final String suiteName
    public final String group
    public final String dbName
    public final ThreadLocal<ConnectionInfo> threadLocalConn = new ThreadLocal<>()
    public final ThreadLocal<Connection> threadHiveDockerConn = new ThreadLocal<>()
    public final ThreadLocal<Connection> threadHiveRemoteConn = new ThreadLocal<>()
    private final ThreadLocal<Syncer> syncer = new ThreadLocal<>()
    public final Config config
    public final File dataPath
    public final File outputFile
    public final File realOutputFile
    public final ScriptContext scriptContext
    public final String flowName
    public final String flowId
    public final ThreadLocal<OutputUtils.OutputBlocksIterator> threadLocalOutputIterator = new ThreadLocal<>()
    public final ExecutorService suiteExecutors
    public final ExecutorService actionExecutors
    private volatile OutputUtils.OutputBlocksWriter outputBlocksWriter
    private volatile OutputUtils.OutputBlocksWriter realOutputBlocksWriter
    private long startTime
    private long finishTime
    private volatile Throwable throwable

    SuiteContext(File file, String suiteName, String group, ScriptContext scriptContext,
                 ExecutorService suiteExecutors, ExecutorService actionExecutors, Config config) {
        this.file = file
        this.suiteName = suiteName
        this.group = group
        this.config = config
        this.scriptContext = scriptContext

        String packageName = getPackageName()
        String className = getClassName()
        this.flowName = "${packageName}.${className}.${suiteName}"
        this.flowId = "${scriptContext.flowId}#${suiteName}"
        this.suiteExecutors = suiteExecutors
        this.actionExecutors = actionExecutors

        def path = new File(config.suitePath).relativePath(file)
        def realPath = new File(config.suitePath).relativePath(file)
        def outputRelativePath = path.substring(0, path.lastIndexOf(".")) + ".out"
        def realOutputRelativePath = path.substring(0, realPath.lastIndexOf(".")) + ".out"
        this.outputFile = new File(new File(config.dataPath), outputRelativePath)
        this.realOutputFile = new File(new File(config.realDataPath), realOutputRelativePath)
        this.dataPath = this.outputFile.getParentFile().getCanonicalFile()

        this.dbName = config.getDbNameByFile(file)
        // - flowName: tpcds_sf1.sql.q47.q47, flowId: tpcds_sf1/sql/q47.sql#q47
        log.info("flowName: ${flowName}, flowId: ${flowId}".toString())
    }

    String getPackageName() {
        String packageName = scriptContext.name
        log.info("packageName: ${packageName}".toString())
        int dirSplitPos = packageName.lastIndexOf(File.separator)
        if (dirSplitPos != -1) {
            packageName = packageName.substring(0, dirSplitPos)
        }
        packageName = packageName.replace(File.separator, ".")
        return packageName
    }

    String getClassName() {
        String scriptFileName = scriptContext.file.name
        log.info("scriptFileName: ${scriptFileName}".toString())
        int suffixPos = scriptFileName.lastIndexOf(".")
        String className = scriptFileName
        if (suffixPos != -1) {
            className = scriptFileName.substring(0, suffixPos)
        }
        return className
    }

    Syncer getSyncer(Suite suite) {
        Syncer syncerImpl = syncer.get()
        if (syncerImpl == null) {
            syncerImpl = new Syncer(suite, config)
            syncer.set(syncerImpl)
        }
        return syncerImpl
    }

    // compatible to context.conn
    Connection getConn() {
        return getConnection()
    }

    Connection getConnection() {
        def threadConnInfo = threadLocalConn.get()
        if (threadConnInfo == null) {
            threadConnInfo = new ConnectionInfo()
            threadConnInfo.conn = config.getConnectionByDbName(dbName)
            threadConnInfo.username = config.jdbcUser
            threadConnInfo.password = config.jdbcPassword
            threadLocalConn.set(threadConnInfo)
        }
        return threadConnInfo.conn
    }

    Connection getHiveDockerConnection(){
        def threadConn = threadHiveDockerConn.get()
        if (threadConn == null) {
            threadConn = getConnectionByHiveDockerConfig()
            threadHiveDockerConn.set(threadConn)
        }
        return threadConn
    }

    Connection getHiveRemoteConnection(){
        def threadConn = threadHiveRemoteConn.get()
        if (threadConn == null) {
            threadConn = getConnectionByHiveRemoteConfig()
            threadHiveRemoteConn.set(threadConn)
        }
        return threadConn
    }

    private String getJdbcNetInfo() {
        String subJdbc = config.jdbcUrl.substring(config.jdbcUrl.indexOf("://") + 3)
        return subJdbc.substring(0, subJdbc.indexOf("/"))
    }

    private Map<String, String> getSpec() {
        Map<String, String> spec = Maps.newHashMap()
        String[] jdbc = getJdbcNetInfo().split(":")
        spec.put("host", jdbc[0])
        spec.put("port", jdbc[1])
        spec.put("user", config.feSyncerUser)
        spec.put("password", config.feSyncerPassword)
        spec.put("cluster", "")

        return spec
    }

    Map<String, String> getSrcSpec() {
        Map<String, String> spec = getSpec()
        spec.put("thrift_port", config.feSourceThriftNetworkAddress.port.toString())
        spec.put("database", dbName)

        return spec
    }

    Map<String, String> getDestSpec() {
        Map<String, String> spec = getSpec()
        spec.put("thrift_port", config.feTargetThriftNetworkAddress.port.toString())
        spec.put("database", "TEST_" + dbName)

        return spec
    }

    Connection getConnectionByHiveDockerConfig() {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String hiveHost = config.otherConfigs.get("externalEnvIp")
        String hivePort = config.otherConfigs.get("hiveServerPort")
        String hiveJdbcUrl = "jdbc:hive2://${hiveHost}:${hivePort}/default"
        String hiveJdbcUser =  "hadoop"
        String hiveJdbcPassword = "hadoop"
        return DriverManager.getConnection(hiveJdbcUrl, hiveJdbcUser, hiveJdbcPassword)
    }

    Connection getConnectionByHiveRemoteConfig() {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String hiveHost = config.otherConfigs.get("extHiveHmsHost")
        String hivePort = config.otherConfigs.get("extHiveServerPort")
        String hiveJdbcUrl = "jdbc:hive2://${hiveHost}:${hivePort}/default"
        String hiveJdbcUser =  "hadoop"
        String hiveJdbcPassword = "hadoop"
        return DriverManager.getConnection(hiveJdbcUrl, hiveJdbcUser, hiveJdbcPassword)
    }

    Connection getTargetConnection(Suite suite) {
        def context = getSyncer(suite).context
        if (context.targetConnection == null) {
            context.targetConnection = config.getConnectionByDbName("TEST_" + dbName)
        }
        return context.targetConnection
    }

    public <T> T connect(String user, String password, String url, Closure<T> actionSupplier) {
        def originConnection = threadLocalConn.get()
        try {
            log.info("Create new connection for user '${user}'")
            return DriverManager.getConnection(url, user, password).withCloseable { newConn ->
                def newConnInfo = new ConnectionInfo()
                newConnInfo.conn = newConn
                newConnInfo.username = user
                newConnInfo.password = password
                threadLocalConn.set(newConnInfo)
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

    OutputUtils.OutputBlocksWriter getRealOutputWriter(boolean deleteIfExist) {
        if (realOutputBlocksWriter != null) {
            return realOutputBlocksWriter
        }
        synchronized (this) {
            if (realOutputBlocksWriter != null) {
                return realOutputBlocksWriter
            } else if (realOutputFile.exists() && deleteIfExist) {
                log.info("Delete ${realOutputFile}".toString())
                realOutputFile.delete()
                log.info("Generate ${realOutputFile}".toString())
                realOutputFile.createNewFile()
                realOutputBlocksWriter = OutputUtils.writer(realOutputFile)
            } else if (!realOutputFile.exists()) {
                realOutputFile.parentFile.mkdirs()
                realOutputFile.createNewFile()
                log.info("Generate ${realOutputFile}".toString())
                realOutputBlocksWriter = OutputUtils.writer(realOutputFile)
            } else {
                log.info("Skip generate output file because exists: ${realOutputFile}".toString())
                realOutputBlocksWriter = new OutputUtils.OutputBlocksWriter(null)
            }
            return realOutputBlocksWriter
        }
    }

    void closeThreadLocal() {
        def outputIterator = threadLocalOutputIterator.get()
        if (outputIterator != null) {
            threadLocalOutputIterator.remove()
            try {
                outputIterator.close()
            } catch (Throwable t) {
                log.warn("Close outputIterator failed", t)
            }
        }

        Syncer syncerImpl = syncer.get()
        if (syncerImpl != null) {
            syncer.remove()
            SyncerContext context = syncerImpl.getContext()
            if (context != null) {
                try {
                    syncerImpl.context.closeAllClients()
                    syncerImpl.context.closeConn()
                } catch (Throwable t) {
                    log.warn("Close syncer failed", t)
                }
            }
        }

        ConnectionInfo conn = threadLocalConn.get()
        if (conn != null) {
            threadLocalConn.remove()
            try {
                conn.conn.close()
            } catch (Throwable t) {
                log.warn("Close connection failed", t)
            }
        }
        
        Connection hive_docker_conn = threadHiveDockerConn.get()
        if (hive_docker_conn != null) {
            threadHiveDockerConn.remove()
            try {
                hive_docker_conn.close()
            } catch (Throwable t) {
                log.warn("Close connection failed", t)
            }
        }
        
        Connection hive_remote_conn = threadHiveRemoteConn.get()
        if (hive_remote_conn != null) {
            threadHiveRemoteConn.remove()
            try {
                hive_remote_conn.close()
            } catch (Throwable t) {
                log.warn("Close connection failed", t)
            }
        }
        
    }

    public <T> T start(Function<SuiteContext, T> func) {
        this.startTime = System.currentTimeMillis()
        scriptContext.eventListeners.each { it.onSuiteStarted(this) }

        this.withCloseable {suiteContext ->
            try {
                if (!config.dryRun) {
                    func.apply(suiteContext)
                }
            } catch (Throwable t) {
                if (t instanceof UndeclaredThrowableException) {
                    t = ((UndeclaredThrowableException) t).undeclaredThrowable
                }
                scriptContext.eventListeners.each { it.onSuiteFailed(this, t) }
                throwable = t
                null
            }
        }
    }

    @Override
    void close() {
        closeThreadLocal()

        if (outputBlocksWriter != null) {
            outputBlocksWriter.close()
        }

        if (realOutputBlocksWriter != null) {
            realOutputBlocksWriter.close()
        }

        this.finishTime = System.currentTimeMillis()
        long elapsed = finishTime - startTime
        scriptContext.eventListeners.each { it.onSuiteFinished(this, throwable == null, elapsed) }
    }
}
