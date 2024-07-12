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
    public final ThreadLocal<ConnectionInfo> threadArrowFlightSqlConn = new ThreadLocal<>()
    public final ThreadLocal<Connection> threadHive2DockerConn = new ThreadLocal<>()
    public final ThreadLocal<Connection> threadHive3DockerConn = new ThreadLocal<>()
    public final ThreadLocal<Connection> threadHiveRemoteConn = new ThreadLocal<>()
    public final ThreadLocal<Connection> threadDB2DockerConn = new ThreadLocal<>()
    private final ThreadLocal<Syncer> syncer = new ThreadLocal<>()
    public final Config config
    public final File dataPath
    public final File outputFile
    public final File realOutputFile
    public final ScriptContext scriptContext
    public final SuiteCluster cluster
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

    SuiteContext(File file, String suiteName, String group, ScriptContext scriptContext, SuiteCluster cluster,
                 ExecutorService suiteExecutors, ExecutorService actionExecutors, Config config) {
        this.file = file
        this.suiteName = suiteName
        this.group = group
        this.config = config
        this.scriptContext = scriptContext
        this.cluster = cluster

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

    boolean useArrowFlightSql() {
        if (group.contains("arrow_flight_sql") && config.groups.contains("arrow_flight_sql")) {
            return true
        }
        return false
    }

    // jdbc:mysql
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

    Connection getArrowFlightSqlConnection() {
        def threadConnInfo = threadArrowFlightSqlConn.get()
        if (threadConnInfo == null) {
            threadConnInfo = new ConnectionInfo()
            threadConnInfo.conn = config.getConnectionByArrowFlightSql(dbName)
            threadConnInfo.username = config.jdbcUser
            threadConnInfo.password = config.jdbcPassword
            threadArrowFlightSqlConn.set(threadConnInfo)
        }
        println("Use arrow flight sql connection")
        return threadConnInfo.conn
    }

    Connection getHiveDockerConnection(String hivePrefix) {
        if (hivePrefix == "hive2") {
            def threadConn = threadHive2DockerConn.get()
            if (threadConn == null) {
                threadConn = getConnectionByHiveDockerConfig(hivePrefix)
                threadHive2DockerConn.set(threadConn)
            }
            return threadConn
        } else if (hivePrefix == "hive3") {
            def threadConn = threadHive3DockerConn.get()
            if (threadConn == null) {
                threadConn = getConnectionByHiveDockerConfig(hivePrefix)
                threadHive3DockerConn.set(threadConn)
            }
            return threadConn
        }
    }

    Connection getHiveRemoteConnection() {
        def threadConn = threadHiveRemoteConn.get()
        if (threadConn == null) {
            threadConn = getConnectionByHiveRemoteConfig()
            threadHiveRemoteConn.set(threadConn)
        }
        return threadConn
    }

    Connection getDB2DockerConnection() {
        def threadConn = threadDB2DockerConn.get()
        if (threadConn == null) {
            threadConn = getConnectionByDB2DockerConfig()
            threadDB2DockerConn.set(threadConn)
        }
        return threadConn
    }

    private String getJdbcNetInfo() {
        String subJdbc = config.jdbcUrl.substring(config.jdbcUrl.indexOf("://") + 3)
        return subJdbc.substring(0, subJdbc.indexOf("/"))
    }

    private String getDownstreamJdbcNetInfo() {
        String subJdbc = config.ccrDownstreamUrl.substring(config.ccrDownstreamUrl.indexOf("://") + 3)
        return subJdbc.substring(0, subJdbc.indexOf("/"))
    }

    private Map<String, String> getSpec(String[] jdbc) {
        Map<String, String> spec = Maps.newHashMap()

        spec.put("host", jdbc[0])
        spec.put("port", jdbc[1])
        spec.put("user", config.feSyncerUser)
        spec.put("password", config.feSyncerPassword)
        spec.put("cluster", "")

        return spec
    }

    Map<String, String> getSrcSpec(String db = null) {
        if (db == null) {
            db = dbName
        }
        String[] jdbc = getJdbcNetInfo().split(":")
        Map<String, String> spec = getSpec(jdbc)
        spec.put("thrift_port", config.feSourceThriftNetworkAddress.port.toString())
        spec.put("database", db)

        return spec
    }

    Map<String, String> getDestSpec(String db = null) {
        if (db == null) {
            db = dbName
        }
        String[] jdbc = getDownstreamJdbcNetInfo().split(":")
        Map<String, String> spec = getSpec(jdbc)
        spec.put("thrift_port", config.feTargetThriftNetworkAddress.port.toString())
        spec.put("database", "TEST_" + db)

        return spec
    }

    Connection getConnectionByHiveDockerConfig(String hivePrefix) {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String hiveHost = config.otherConfigs.get("externalEnvIp")
        String hivePort = config.otherConfigs.get(hivePrefix + "ServerPort")
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

    Connection getConnectionByDB2DockerConfig() {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        String db2Host = config.otherConfigs.get("externalEnvIp")
        String db2Port = config.otherConfigs.get("db2_11_port")
        String db2JdbcUrl = "jdbc:db2://${db2Host}:${db2Port}/doris"
        String db2JdbcUser =  "db2inst1"
        String db2JdbcPassword = "123456"
        return DriverManager.getConnection(db2JdbcUrl, db2JdbcUser, db2JdbcPassword)
    }

    Connection getTargetConnection(Suite suite) {
        def context = getSyncer(suite).context
        if (context.targetConnection == null) {
            context.targetConnection = config.getDownstreamConnectionByDbName("TEST_" + dbName)
        }
        return context.targetConnection
    }

    InetSocketAddress getFeHttpAddress() {
        if (cluster.isRunning()) {
            def fe = cluster.getMasterFe()
            return new InetSocketAddress(fe.host, fe.httpPort)
        } else {
            return config.feHttpInetSocketAddress
        }
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

    public void reconnectFe() {
        ConnectionInfo connInfo = threadLocalConn.get()
        if (connInfo == null) {
            return
        }
        connectTo(connInfo.conn.getMetaData().getURL(), connInfo.username, connInfo.password);
    }

    public void connectTo(String url, String username, String password) {
        ConnectionInfo oldConn = threadLocalConn.get()
        if (oldConn != null) {
            threadLocalConn.remove()
            try {
                oldConn.conn.close()
            } catch (Throwable t) {
                log.warn("Close connection failed", t)
            }
        }

        def newConnInfo = new ConnectionInfo()
        newConnInfo.conn = DriverManager.getConnection(url, username, password)
        newConnInfo.username = username
        newConnInfo.password = password
        threadLocalConn.set(newConnInfo)
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

        ConnectionInfo arrow_flight_sql_conn = threadArrowFlightSqlConn.get()
        if (arrow_flight_sql_conn != null) {
            threadArrowFlightSqlConn.remove()
            try {
                arrow_flight_sql_conn.conn.close()
            } catch (Throwable t) {
                log.warn("Close connection failed", t)
            }
        }

        Connection hive2_docker_conn = threadHive2DockerConn.get()
        if (hive2_docker_conn != null) {
            threadHive2DockerConn.remove()
            try {
                hive2_docker_conn.close()
            } catch (Throwable t) {
                log.warn("Close connection failed", t)
            }
        }

        Connection hive3_docker_conn = threadHive3DockerConn.get()
        if (hive3_docker_conn != null) {
            threadHive3DockerConn.remove()
            try {
                hive3_docker_conn.close()
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
