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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

// MySQL protocol network service
public class MysqlServer {
    private static final Logger LOG = LogManager.getLogger(MysqlServer.class);

    protected int port;
    protected volatile boolean running;
    private ServerSocketChannel serverChannel = null;
    private ConnectScheduler scheduler = null;
    // used to accept connect request from client
    private ThreadPoolExecutor listener;
    private Future listenerFuture;

    public MysqlServer(int port, ConnectScheduler scheduler) {
        this.port = port;
        this.scheduler = scheduler;
    }

    protected MysqlServer() {
    }

    // start MySQL protocol service
    // return true if success, otherwise false
    public boolean start() {
        if (scheduler == null) {
            LOG.warn("scheduler is NULL.");
            return false;
        }

        // open server socket
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.socket().bind(new InetSocketAddress("0.0.0.0", port), 2048);
            serverChannel.configureBlocking(true);
        } catch (IOException e) {
            LOG.warn("Open MySQL network service failed.", e);
            return false;
        }

        // start accept thread
        listener = ThreadPoolManager.newDaemonCacheThreadPool(1, "MySQL-Protocol-Listener", true);
        running = true;
        listenerFuture = listener.submit(new Listener());

        return true;
    }

    public void stop() {
        if (running) {
            running = false;
            // close server channel, make accept throw exception
            try {
                serverChannel.close();
            } catch (IOException e) {
                LOG.warn("close server channel failed.", e);
            }
        }
    }

    public void join() {
        try {
            listenerFuture.get();
        } catch (Exception e) {
            // just return
            LOG.warn("Join MySQL server exception.", e);
        }
    }

    private class Listener implements Runnable {
        @Override
        public void run() {
            while (running && serverChannel.isOpen()) {
                SocketChannel clientChannel;
                try {
                    clientChannel = serverChannel.accept();
                    if (clientChannel == null) {
                        continue;
                    }
                    // submit this context to scheduler
                    ConnectContext context = new ConnectContext(clientChannel);
                    // Set catalog here.
                    context.setCatalog(Catalog.getCurrentCatalog());
                    if (!scheduler.submit(context)) {
                        LOG.warn("Submit one connect request failed. Client=" + clientChannel.toString());
                        // clear up context
                        context.cleanup();
                    }
                } catch (IOException e) {
                    // ClosedChannelException
                    // AsynchronousCloseException
                    // ClosedByInterruptException
                    // Other IOException, for example "to many open files" ...
                    LOG.warn("Query server encounter exception.", e);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                       // Do nothing
                    }
                } catch (Throwable e) {
                    // NotYetBoundException
                    // SecurityException
                    LOG.warn("Query server failed when calling accept.", e);
                }
            }
        }
    }

    public ConnectScheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(ConnectScheduler scheduler) {
        this.scheduler = scheduler;
    }

}
