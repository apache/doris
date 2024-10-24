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

package org.apache.doris.mysql;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.ProxyProtocolHandler.ProxyProtocolResult;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.MysqlConnectProcessor;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.ChannelListener;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;

/**
 * listener for accept mysql connections.
 */
public class AcceptListener implements ChannelListener<AcceptingChannel<StreamConnection>> {
    private static final Logger LOG = LogManager.getLogger(AcceptListener.class);
    private final ConnectScheduler connectScheduler;

    public AcceptListener(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    @Override
    public void handleEvent(AcceptingChannel<StreamConnection> channel) {
        try {
            StreamConnection connection = channel.accept();
            if (connection == null) {
                return;
            }
            connection.setOption(Options.KEEP_ALIVE, true);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connection established. remote={}", connection.getPeerAddress());
            }
            // connection has been established, so need to call context.cleanup()
            // if exception happens.
            ConnectContext context = new ConnectContext(connection);
            if (context.getSessionVariable().getQueryTimeoutS() <= 0) {
                LOG.warn("Connection query timeout is invalid: {}", context.getSessionVariable().getQueryTimeoutS());
            }
            context.setEnv(Env.getCurrentEnv());
            connectScheduler.submit(context);

            try {
                channel.getWorker().execute(() -> {
                    try {
                        // Set thread local info
                        context.setThreadLocalInfo();
                        context.setConnectScheduler(connectScheduler);

                        if (Config.enable_proxy_protocol) {
                            ProxyProtocolResult result = ProxyProtocolHandler.handle(context.getMysqlChannel());
                            Preconditions.checkNotNull(result);
                            if (!result.isUnknown) {
                                context.getMysqlChannel().setRemoteAddr(result.sourceIP, result.sourcePort);
                            }
                            // ignore the UNKNOWN, and just use IP from MySQL protocol.
                            // which is already set when creating MysqlChannel.
                        }

                        // authenticate check failed.
                        if (!MysqlProto.negotiate(context)) {
                            throw new AfterConnectedException("mysql negotiate failed");
                        }
                        if (connectScheduler.registerConnection(context)) {
                            MysqlProto.sendResponsePacket(context);
                            connection.setCloseListener(
                                    streamConnection -> connectScheduler.unregisterConnection(context));
                        } else {
                            context.getState().setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS,
                                    "Reach limit of connections");
                            MysqlProto.sendResponsePacket(context);
                            throw new AfterConnectedException("Reach limit of connections");
                        }
                        context.setStartTime();
                        int userQueryTimeout = context.getEnv().getAuth().getQueryTimeout(context.getQualifiedUser());
                        if (userQueryTimeout <= 0 && LOG.isDebugEnabled()) {
                            LOG.debug("Connection set query timeout to {}",
                                        context.getSessionVariable().getQueryTimeoutS());
                        }
                        context.setUserQueryTimeout(userQueryTimeout);
                        context.setUserInsertTimeout(
                                context.getEnv().getAuth().getInsertTimeout(context.getQualifiedUser()));
                        ConnectProcessor processor = new MysqlConnectProcessor(context);
                        context.startAcceptQuery(processor);
                    } catch (AfterConnectedException e) {
                        // do not need to print log for this kind of exception.
                        // just clean up the context;
                        context.cleanup();
                    } catch (Throwable e) {
                        // should be unexpected exception, so print warn log
                        if (context.getCurrentUserIdentity() != null) {
                            LOG.warn("connect processor exception because ", e);
                        } else if (e instanceof Error) {
                            LOG.error("connect processor exception because ", e);
                        } else {
                            // for unauthrorized access such lvs probe request,
                            // may cause exception, just log it in debug level
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("connect processor exception because ", e);
                            }
                        }
                        context.cleanup();
                    } finally {
                        ConnectContext.remove();
                    }
                });
            } catch (RejectedExecutionException e) {
                context.getState().setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS,
                        "Too many connections");
                MysqlProto.sendResponsePacket(context);
                context.cleanup();
            }
        } catch (IOException e) {
            LOG.warn("Connection accept failed.", e);
        }
    }

    // this exception is only used for some expected exception after connection established.
    // so that we can catch these kind of exceptions and close the channel without printing warning logs.
    private static class AfterConnectedException extends Exception {
        public AfterConnectedException(String msg) {
            super(msg);
        }
    }
}
