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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.qe.QueryDetail;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.SocketChannel;
import java.util.List;

// When one client connect in, we create a connect context for it.
// We store session information here. Meanwhile ConnectScheduler all
// connect with its connection id.
// Use `volatile` to make the reference change atomic.
public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);
    protected static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<ConnectContext>();

    // set this id before analyze
    protected volatile long stmtId;
    protected volatile long forwardedStmtId;

    protected volatile TUniqueId queryId;
    // id for this connection
    protected volatile int connectionId;
    // mysql net
    protected volatile MysqlChannel mysqlChannel;
    // state
    protected volatile QueryState state;
    protected volatile long returnRows;
    // the protocol capability which server say it can support
    protected volatile MysqlCapability serverCapability;
    // the protocol capability after server and client negotiate
    protected volatile MysqlCapability capability;
    // Indicate if this client is killed.
    protected volatile boolean isKilled;
    // Db
    protected volatile String currentDb = "";
    // cluster name
    protected volatile String clusterName = "";
    // username@host of current login user
    protected volatile String qualifiedUser;
    // username@host combination for the Doris account
    // that the server used to authenticate the current client.
    // In other word, currentUserIdentity is the entry that matched in Doris auth table.
    // This account determines user's access privileges.
    protected volatile UserIdentity currentUserIdentity;
    // Serializer used to pack MySQL packet.
    protected volatile MysqlSerializer serializer;
    // Variables belong to this session.
    protected volatile SessionVariable sessionVariable;
    // Scheduler this connection belongs to
    protected volatile ConnectScheduler connectScheduler;
    // Executor
    protected volatile StmtExecutor executor;
    // Command this connection is processing.
    protected volatile MysqlCommand command;
    // Timestamp in millisecond last command starts at
    protected volatile long startTime;
    // Cache thread info for this connection.
    protected volatile ThreadInfo threadInfo;

    // Catalog: put catalog here is convenient for unit test,
    // because catalog is singleton, hard to mock
    protected Catalog catalog;
    protected boolean isSend;

    protected AuditEventBuilder auditEventBuilder = new AuditEventBuilder();;

    protected String remoteIP;

    protected QueryDetail queryDetail;

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public static void remove() {
        threadLocalInfo.remove();
    }

    public void setIsSend(boolean isSend) {
        this.isSend = isSend;
    }

    public boolean isSend() {
        return this.isSend;
    }

    public ConnectContext() {
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        serializer = MysqlSerializer.newInstance();
        sessionVariable = VariableMgr.newSessionVariable();
        command = MysqlCommand.COM_SLEEP;
    }

    public ConnectContext(SocketChannel channel) {
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        mysqlChannel = new MysqlChannel(channel);
        serializer = MysqlSerializer.newInstance();
        sessionVariable = VariableMgr.newSessionVariable();
        command = MysqlCommand.COM_SLEEP;
        if (channel != null) {
            remoteIP = mysqlChannel.getRemoteIp();
        }
        queryDetail = null;
    }

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public long getForwardedStmtId() {
        return forwardedStmtId;
    }

    public void setForwardedStmtId(long forwardedStmtId) {
        this.forwardedStmtId = forwardedStmtId;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public void setQueryDetail(QueryDetail queryDetail) {
        this.queryDetail = queryDetail;
    }

    public QueryDetail getQueryDetail() {
        return queryDetail;
    }

    public AuditEventBuilder getAuditEventBuilder() {
        return auditEventBuilder;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public TResourceInfo toResourceCtx() {
        return new TResourceInfo(qualifiedUser, sessionVariable.getResourceGroup());
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public void setQualifiedUser(String qualifiedUser) {
        this.qualifiedUser = qualifiedUser;
    }

    // for USER() function
    public UserIdentity getUserIdentity() {
        return new UserIdentity(qualifiedUser, remoteIP);
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        this.currentUserIdentity = currentUserIdentity;
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public ConnectScheduler getConnectScheduler() {
        return connectScheduler;
    }

    public void setConnectScheduler(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
        returnRows = 0;
    }

    public void updateReturnRows(int returnRows) {
        this.returnRows += returnRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetReturnRows() {
        returnRows = 0;
    }

    public MysqlSerializer getSerializer() {
        return serializer;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public MysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public QueryState getState() {
        return state;
    }

    public void setState(QueryState state) {
        this.state = state;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public String getDatabase() {
        return currentDb;
    }

    public void setDatabase(String db) {
        currentDb = db;
    }

    public void setExecutor(StmtExecutor executor) {
        this.executor = executor;
    }

    public void cleanup() {
        mysqlChannel.close();
        threadLocalInfo.remove();
        returnRows = 0;
    }

    public boolean isKilled() {
        return isKilled;
    }

    // Set kill flag to true;
    public void setKilled() {
        isKilled = true;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public TUniqueId queryId() {
        return queryId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setCluster(String clusterName) {
        this.clusterName = clusterName;
    }

    // kill operation with no protect.
    public void kill(boolean killConnection) {
        LOG.warn("kill timeout query, {}, kill connection: {}",
                 getMysqlChannel().getRemoteHostPortString(), killConnection);

        if (killConnection) {
            isKilled = true;
            // Close channel to break connection with client
            getMysqlChannel().close();
        }
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel();
        }
    }

    public void checkTimeout(long now) {
        if (startTime <= 0) {
            return;
        }

        long delta = now - startTime;
        boolean killFlag = false;
        boolean killConnection = false;
        if (command == MysqlCommand.COM_SLEEP) {
            if (delta > sessionVariable.getWaitTimeoutS() * 1000) {
                // Need kill this connection.
                LOG.warn("kill wait timeout connection, remote: {}, wait timeout: {}",
                         getMysqlChannel().getRemoteHostPortString(), sessionVariable.getWaitTimeoutS());

                killFlag = true;
                killConnection = true;
            }
        } else {
            if (delta > sessionVariable.getQueryTimeoutS() * 1000) {
                LOG.warn("kill query timeout, remote: {}, query timeout: {}",
                         getMysqlChannel().getRemoteHostPortString(), sessionVariable.getQueryTimeoutS());

                // Only kill
                killFlag = true;
            }
        }
        if (killFlag) {
            kill(killConnection);
        }
    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo() {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        return threadInfo;
    }

    public class ThreadInfo {
        public List<String>  toRow(long nowMs) {
            List<String> row = Lists.newArrayList();
            row.add("" + connectionId);
            row.add(ClusterNamespace.getNameFromFullName(qualifiedUser));
            row.add(getMysqlChannel().getRemoteHostPortString());
            row.add(clusterName);
            row.add(ClusterNamespace.getNameFromFullName(currentDb));
            row.add(command.toString());
            row.add("" + (nowMs - startTime) / 1000);
            row.add("");
            row.add("");
            return row;
        }
    }
}
