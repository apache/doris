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

package org.apache.doris.http;

import org.apache.doris.common.Config;
import org.apache.doris.http.action.BackendAction;
import org.apache.doris.http.action.HaAction;
import org.apache.doris.http.action.HelpAction;
import org.apache.doris.http.action.IndexAction;
import org.apache.doris.http.action.LogAction;
import org.apache.doris.http.action.QueryAction;
import org.apache.doris.http.action.QueryProfileAction;
import org.apache.doris.http.action.SessionAction;
import org.apache.doris.http.action.StaticResourceAction;
import org.apache.doris.http.action.SystemAction;
import org.apache.doris.http.action.VariableAction;
import org.apache.doris.http.common.DorisHttpPostObjectAggregator;
import org.apache.doris.http.meta.ColocateMetaService;
import org.apache.doris.http.meta.MetaService.CheckAction;
import org.apache.doris.http.meta.MetaService.DumpAction;
import org.apache.doris.http.meta.MetaService.ImageAction;
import org.apache.doris.http.meta.MetaService.InfoAction;
import org.apache.doris.http.meta.MetaService.JournalIdAction;
import org.apache.doris.http.meta.MetaService.PutAction;
import org.apache.doris.http.meta.MetaService.RoleAction;
import org.apache.doris.http.meta.MetaService.VersionAction;
import org.apache.doris.http.rest.BootstrapFinishAction;
import org.apache.doris.http.rest.CancelStreamLoad;
import org.apache.doris.http.rest.CheckDecommissionAction;
import org.apache.doris.http.rest.ConnectionAction;
import org.apache.doris.http.rest.GetDdlStmtAction;
import org.apache.doris.http.rest.GetLoadInfoAction;
import org.apache.doris.http.rest.GetLogFileAction;
import org.apache.doris.http.rest.GetSmallFileAction;
import org.apache.doris.http.rest.GetStreamLoadState;
import org.apache.doris.http.rest.HealthAction;
import org.apache.doris.http.rest.LoadAction;
import org.apache.doris.http.rest.MetaReplayerCheckAction;
import org.apache.doris.http.rest.MetricsAction;
import org.apache.doris.http.rest.MigrationAction;
import org.apache.doris.http.rest.MultiAbort;
import org.apache.doris.http.rest.MultiCommit;
import org.apache.doris.http.rest.MultiDesc;
import org.apache.doris.http.rest.MultiList;
import org.apache.doris.http.rest.MultiStart;
import org.apache.doris.http.rest.MultiUnload;
import org.apache.doris.http.rest.ProfileAction;
import org.apache.doris.http.rest.QueryDetailAction;
import org.apache.doris.http.rest.RowCountAction;
import org.apache.doris.http.rest.SetConfigAction;
import org.apache.doris.http.rest.ShowDataAction;
import org.apache.doris.http.rest.ShowMetaInfoAction;
import org.apache.doris.http.rest.ShowProcAction;
import org.apache.doris.http.rest.ShowRuntimeInfoAction;
import org.apache.doris.http.rest.StorageTypeCheckAction;
import org.apache.doris.http.rest.TableQueryPlanAction;
import org.apache.doris.http.rest.TableRowCountAction;
import org.apache.doris.http.rest.TableSchemaAction;
import org.apache.doris.master.MetaHelper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HttpServer {
    private static final Logger LOG = LogManager.getLogger(HttpServer.class);
    private int port;
    private ActionController controller;

    private Thread serverThread;

    private AtomicBoolean isStarted = new AtomicBoolean(false);

    public HttpServer(int port) {
        this.port = port;
        controller = new ActionController();
    }

    public void setup() throws IllegalArgException {
        registerActions();
    }

    private void registerActions() throws IllegalArgException {
        // add rest action
        LoadAction.registerAction(controller);
        GetLoadInfoAction.registerAction(controller);
        SetConfigAction.registerAction(controller);
        GetDdlStmtAction.registerAction(controller);
        MigrationAction.registerAction(controller);
        StorageTypeCheckAction.registerAction(controller);
        CancelStreamLoad.registerAction(controller);
        GetStreamLoadState.registerAction(controller);

        // add web action
        IndexAction.registerAction(controller);
        SystemAction.registerAction(controller);
        BackendAction.registerAction(controller);
        LogAction.registerAction(controller);
        QueryAction.registerAction(controller);
        QueryProfileAction.registerAction(controller);
        SessionAction.registerAction(controller);
        VariableAction.registerAction(controller);
        HelpAction.registerAction(controller);
        StaticResourceAction.registerAction(controller);
        HaAction.registerAction(controller);

        // Add multi action
        MultiStart.registerAction(controller);
        MultiDesc.registerAction(controller);
        MultiCommit.registerAction(controller);
        MultiUnload.registerAction(controller);
        MultiAbort.registerAction(controller);
        MultiList.registerAction(controller);

        // rest action
        HealthAction.registerAction(controller);
        MetricsAction.registerAction(controller);
        ShowMetaInfoAction.registerAction(controller);
        ShowProcAction.registerAction(controller);
        ShowRuntimeInfoAction.registerAction(controller);
        GetLogFileAction.registerAction(controller);
        GetSmallFileAction.registerAction(controller);
        RowCountAction.registerAction(controller);
        CheckDecommissionAction.registerAction(controller);
        MetaReplayerCheckAction.registerAction(controller);
        ColocateMetaService.BucketSeqAction.registerAction(controller);
        ColocateMetaService.ColocateMetaAction.registerAction(controller);
        ColocateMetaService.MarkGroupStableAction.registerAction(controller);
        ProfileAction.registerAction(controller);
        QueryDetailAction.registerAction(controller);
        ConnectionAction.registerAction(controller);
        ShowDataAction.registerAction(controller);

        // meta service action
        File imageDir = MetaHelper.getMasterImageDir();
        ImageAction.registerAction(controller, imageDir);
        InfoAction.registerAction(controller, imageDir);
        VersionAction.registerAction(controller, imageDir);
        PutAction.registerAction(controller, imageDir);
        JournalIdAction.registerAction(controller, imageDir);
        CheckAction.registerAction(controller, imageDir);
        DumpAction.registerAction(controller, imageDir);
        RoleAction.registerAction(controller, imageDir);

        // external usage
        TableRowCountAction.registerAction(controller);
        TableSchemaAction.registerAction(controller);
        TableQueryPlanAction.registerAction(controller);

        BootstrapFinishAction.registerAction(controller);
    }

    public void start() {
        serverThread = new Thread(new HttpServerThread(), "FE Http Server");
        serverThread.start();
    }

    protected class PaloHttpServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new HttpServerCodec());
            ch.pipeline().addLast(new DorisHttpPostObjectAggregator(100 * 65536));
            ch.pipeline().addLast(new ChunkedWriteHandler());
            ch.pipeline().addLast(new HttpServerHandler(controller));
        }
    }

    ServerBootstrap serverBootstrap;

    private class HttpServerThread implements Runnable {
        @Override
        public void run() {
            // Configure the server.
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                serverBootstrap = new ServerBootstrap();
                serverBootstrap.option(ChannelOption.SO_BACKLOG, Config.http_backlog_num);
                // reused address and port to avoid bind already exception
                serverBootstrap.option(ChannelOption.SO_REUSEADDR, true);
                serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
                serverBootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new PaloHttpServerInitializer());
                Channel ch = serverBootstrap.bind(port).sync().channel();

                isStarted.set(true);
                LOG.info("HttpServer started with port {}", port);
                // block until server is closed
                ch.closeFuture().sync();
            } catch (Exception e) {
                LOG.error("Fail to start FE query http server[port: " + port + "] ", e);
                System.exit(-1);
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }
    }

    // used for test, release bound port
    public void shutDown() {
        if (serverBootstrap != null) {
            Future future = serverBootstrap.config().group().shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
            try {
                future.get();
                isStarted.set(false);
                LOG.info("HttpServer was closed completely");
            } catch (Throwable e) {
                LOG.warn("Exception happened when close HttpServer", e);
            }
            serverBootstrap = null;
        }
    }

    public boolean isStarted() {
        return isStarted.get();
    }

    public static void main(String[] args) throws Exception {
        HttpServer httpServer = new HttpServer(8080);
        httpServer.setup();
        System.out.println("before start http server.");
        httpServer.start();
        System.out.println("after start http server.");

        while (true) {
            Thread.sleep(2000);
        }
    }
}
