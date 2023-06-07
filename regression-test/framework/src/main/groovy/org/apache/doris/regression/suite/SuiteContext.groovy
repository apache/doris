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
import groovy.util.logging.Slf4j
import org.apache.doris.thrift.BackendService
import org.apache.doris.thrift.FrontendService
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TTabletCommitInfo
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransportException

import java.lang.reflect.UndeclaredThrowableException
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ExecutorService
import java.util.function.Function

class FrontendClientImpl {
    private TSocket tSocket
    public FrontendService.Client client

    FrontendClientImpl(TNetworkAddress address) throws TTransportException {
        this.tSocket = new TSocket(address.hostname, address.port)
        this.client = new FrontendService.Client(new TBinaryProtocol(tSocket))
        this.tSocket.open()
    }

    void close() {
        this.tSocket.close()
    }
}

class BackendClientImpl {
    private TSocket tSocket

    public TNetworkAddress address
    public int httpPort
    public BackendService.Client client

    BackendClientImpl(TNetworkAddress address, int httpPort) throws TTransportException {
        this.address = address
        this.httpPort = httpPort
        this.tSocket = new TSocket(address.hostname, address.port)
        this.client = new BackendService.Client(new TBinaryProtocol(this.tSocket))
        this.tSocket.open()
    }

    String toString() {
        return "BackendClientImpl: {" + address.toString() + " }"
    }

    void close() {
        tSocket.close()
    }
}

class PartitionMeta {
    public long version
    public long indexId
    public TreeMap<Long, Long> tabletMeta

    PartitionMeta(long indexId, long version) {
        this.indexId = indexId
        this.version = version
        this.tabletMeta = new TreeMap<Long, Long>()
    }

    String toString() {
        return "PartitionMeta: { version: " + version.toString() + ", " + tabletMeta.toString() + " }"
    }

}

class SyncerContext {
    protected Connection targetConnection
    protected FrontendClientImpl sourceFrontendClient
    protected FrontendClientImpl targetFrontendClient

    protected Long sourceDbId
    protected HashMap<String, Long> sourceTableMap = new HashMap<String, Long>()
    protected TreeMap<Long, PartitionMeta> sourcePartitionMap = new TreeMap<Long, PartitionMeta>()
    protected Long targetDbId
    protected HashMap<String, Long> targetTableMap = new HashMap<String, Long>()
    protected TreeMap<Long, PartitionMeta> targetPartitionMap = new TreeMap<Long, PartitionMeta>()

    protected HashMap<Long, BackendClientImpl> sourceBackendClients = new HashMap<Long, BackendClientImpl>()
    protected HashMap<Long, BackendClientImpl> targetBackendClients = new HashMap<Long, BackendClientImpl>()

    public ArrayList<TTabletCommitInfo> commitInfos = new ArrayList<TTabletCommitInfo>()

    public String user
    public String passwd
    public String db
    public long txnId
    public long seq

    ArrayList<TTabletCommitInfo> resetCommitInfos() {
        def info = commitInfos
        commitInfos = new ArrayList<TTabletCommitInfo>()
        return info
    }

    ArrayList<TTabletCommitInfo> copyCommitInfos() {
        return new ArrayList<TTabletCommitInfo>(commitInfos)
    }

    void addCommitInfo(long tabletId, long backendId) {
        commitInfos.add(new TTabletCommitInfo(tabletId, backendId))
    }

    Boolean metaIsValid() {
        if (sourcePartitionMap.isEmpty() || targetPartitionMap.isEmpty()) {
            return false
        }

        if (sourcePartitionMap.size() != targetPartitionMap.size()) {
            return false
        }

        Iterator sourceIter = sourcePartitionMap.iterator()
        Iterator targetIter = targetPartitionMap.iterator()
        while (sourceIter.hasNext()) {
             if (sourceIter.next().value.tabletMeta.size() !=
                 targetIter.next().value.tabletMeta.size()) {
                 return false
             }
        }

        return true
    }

    void closeBackendClients() {
        if (!sourceBackendClients.isEmpty()) {
            for (BackendClientImpl client in sourceBackendClients.values()) {
                client.close()
            }
        }
        sourceBackendClients.clear()
        if (!targetBackendClients.isEmpty()) {
            for (BackendClientImpl client in targetBackendClients.values()) {
                client.close()
            }
        }
        targetBackendClients.clear()
    }

    void closeAllClients() {
        if (sourceFrontendClient != null) {
            sourceFrontendClient.close()
        }
        if (targetFrontendClient != null) {
            targetFrontendClient.close()
        }
    }
}


@Slf4j
@CompileStatic
class SuiteContext implements Closeable {
    public final File file
    public final String suiteName
    public final String group
    public final String dbName
    public final ThreadLocal<Connection> threadLocalConn = new ThreadLocal<>()
    private final ThreadLocal<SyncerContext> syncerContext = new ThreadLocal<>()
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

    SyncerContext getSyncerContext() {
        SyncerContext context = syncerContext.get()
        if (context == null) {
            context = new SyncerContext()
            context.user = config.feSyncerUser
            context.passwd = config.feSyncerPassword
            context.db = dbName
            context.seq = -1
            syncerContext.set(context)
        }
        return context
    }

    // compatible to context.conn
    Connection getConn() {
        return getConnection()
    }

    Connection getConnection() {
        def threadConn = threadLocalConn.get()
        if (threadConn == null) {
            threadConn = config.getConnectionByDbName(dbName)
            threadLocalConn.set(threadConn)
        }
        return threadConn
    }

    Connection getTargetConnection() {
        def context = getSyncerContext()
        if (context.targetConnection == null) {
            context.targetConnection = config.getTargetConnectionByDbName(dbName)
        }
        return context.targetConnection
    }

    FrontendClientImpl getSourceFrontClient() {
        log.info("Get source client ${config.feSourceThriftNetworkAddress}")
        SyncerContext context = getSyncerContext()
        if (context.sourceFrontendClient == null) {
            try {
                context.sourceFrontendClient = new FrontendClientImpl(config.feSourceThriftNetworkAddress)
            } catch (TTransportException e) {
                log.error("Create client error, Exception: ${e}")
            }
        }
        return context.sourceFrontendClient
    }

    FrontendClientImpl getTargetFrontClient() {
        log.info("Get target client ${config.feTargetThriftNetworkAddress}")
        SyncerContext context = getSyncerContext()
        if (context.targetFrontendClient == null) {
            try {
                context.targetFrontendClient = new FrontendClientImpl(config.feTargetThriftNetworkAddress)
            } catch (TTransportException e) {
                log.error("Create client error, Exception: ${e}")
            }
        }
        return context.targetFrontendClient
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

        SyncerContext context = syncerContext.get()
        if (context != null) {
            context.closeAllClients()
        }

        Connection conn = threadLocalConn.get()
        if (conn != null) {
            threadLocalConn.remove()
            try {
                conn.close()
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
