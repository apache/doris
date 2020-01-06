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
package org.apache.doris.mysql.nio;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.ChannelListener;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;

import java.io.IOException;

/**
 * listener for accept mysql connections.
 */
public class AcceptListener implements ChannelListener<AcceptingChannel<StreamConnection>> {
    private final Logger LOG = LogManager.getLogger(this.getClass());
    private ConnectScheduler connectScheduler;

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
            LOG.info("Connection established. remote={}", connection.getPeerAddress());
            NConnectContext context = new NConnectContext(connection);
            context.setCatalog(Catalog.getInstance());
            connectScheduler.submit(context);

            channel.getWorker().execute(() -> {
                try {
                    // Set thread local info
                    context.setThreadLocalInfo();
                    context.setConnectScheduler(connectScheduler);
                    // authenticate check failed.
                    if (!MysqlProto.negotiate(context)) {
                        return;
                    }
                    if (connectScheduler.registerConnection(context)) {
                        MysqlProto.sendResponsePacket(context);
                        connection.setCloseListener(streamConnection -> connectScheduler.unregisterConnection(context));
                    } else {
                        context.getState().setError("Reach limit of connections");
                        MysqlProto.sendResponsePacket(context);
                        return;
                    }
                    context.setStartTime();
                    ConnectProcessor processor = new ConnectProcessor(context);
                    context.startAcceptQuery(processor);
                } catch (Exception e) {
                    LOG.warn("connect processor exception because ", e);
                    context.cleanup();
                } finally {
                    ConnectContext.remove();
                }
            });
        } catch (IOException e) {
            LOG.warn("Connection accept failed.", e);
        }
    }
}
