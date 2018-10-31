// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http;

import com.baidu.palo.http.action.BackendAction;
import com.baidu.palo.http.action.HaAction;
import com.baidu.palo.http.action.HelpAction;
import com.baidu.palo.http.action.IndexAction;
import com.baidu.palo.http.action.LogAction;
import com.baidu.palo.http.action.QueryAction;
import com.baidu.palo.http.action.QueryProfileAction;
import com.baidu.palo.http.action.SessionAction;
import com.baidu.palo.http.action.StaticResourceAction;
import com.baidu.palo.http.action.SystemAction;
import com.baidu.palo.http.action.VariableAction;
import com.baidu.palo.http.meta.MetaService.CheckAction;
import com.baidu.palo.http.meta.MetaService.DumpAction;
import com.baidu.palo.http.meta.MetaService.ImageAction;
import com.baidu.palo.http.meta.MetaService.InfoAction;
import com.baidu.palo.http.meta.MetaService.JournalIdAction;
import com.baidu.palo.http.meta.MetaService.PutAction;
import com.baidu.palo.http.meta.MetaService.RoleAction;
import com.baidu.palo.http.meta.MetaService.VersionAction;
import com.baidu.palo.http.rest.BootstrapFinishAction;
import com.baidu.palo.http.rest.CancelStreamLoad;
import com.baidu.palo.http.rest.CheckDecommissionAction;
import com.baidu.palo.http.rest.GetDdlStmtAction;
import com.baidu.palo.http.rest.GetLoadInfoAction;
import com.baidu.palo.http.rest.GetLogFileAction;
import com.baidu.palo.http.rest.GetStreamLoadState;
import com.baidu.palo.http.rest.HealthAction;
import com.baidu.palo.http.rest.LoadAction;
import com.baidu.palo.http.rest.MetaReplayerCheckAction;
import com.baidu.palo.http.rest.MetricsAction;
import com.baidu.palo.http.rest.MigrationAction;
import com.baidu.palo.http.rest.MultiAbort;
import com.baidu.palo.http.rest.MultiCommit;
import com.baidu.palo.http.rest.MultiDesc;
import com.baidu.palo.http.rest.MultiList;
import com.baidu.palo.http.rest.MultiStart;
import com.baidu.palo.http.rest.MultiUnload;
import com.baidu.palo.http.rest.RowCountAction;
import com.baidu.palo.http.rest.SetConfigAction;
import com.baidu.palo.http.rest.ShowMetaInfoAction;
import com.baidu.palo.http.rest.ShowProcAction;
import com.baidu.palo.http.rest.ShowRuntimeInfoAction;
import com.baidu.palo.http.rest.StorageTypeCheckAction;
import com.baidu.palo.master.MetaHelper;
import com.baidu.palo.qe.QeService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

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
    private static final int BACKLOG_NUM = 128;
    private QeService qeService = null;
    private int port;
    private ActionController controller;

    private Thread serverThread;

    public HttpServer(QeService qeService, int port) {
        this.qeService = qeService;
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
        RowCountAction.registerAction(controller);
        CheckDecommissionAction.registerAction(controller);
        MetaReplayerCheckAction.registerAction(controller);

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

        BootstrapFinishAction.registerAction(controller);
    }

    public void start() {
        serverThread = new Thread(new HttpServerThread(), "FE Http Server");
        serverThread.start();
    }

    protected class PaloHttpServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast("codec", new HttpServerCodec());
            // ch.pipeline().addLast("compressor", new HttpContentCompressor());
            ch.pipeline().addLast(new ChunkedWriteHandler());
            ch.pipeline().addLast(new HttpServerHandler(controller, qeService));
        }
    }

    private class HttpServerThread implements Runnable {
        @Override
        public void run() {
            // Configure the server.
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.option(ChannelOption.SO_BACKLOG, BACKLOG_NUM);
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new PaloHttpServerInitializer());
                Channel ch = b.bind(port).sync().channel();
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

    public static void main(String[] args) throws Exception {
        QeService qeService = new QeService(9030);
        HttpServer httpServer = new HttpServer(qeService, 8080);
        httpServer.setup();
        System.out.println("before start http server.");
        httpServer.start();
        System.out.println("after start http server.");

        while (true) {
            Thread.sleep(2000);
        }
    }
}
