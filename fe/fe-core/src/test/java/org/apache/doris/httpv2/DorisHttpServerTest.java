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

package org.apache.doris.httpv2;

import org.apache.doris.common.FeConstants;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.web.embedded.jetty.JettyWebServer;
import org.springframework.boot.web.server.WebServer;
import org.springframework.context.ConfigurableApplicationContext;

public class DorisHttpServerTest {
    private HttpServer httpServer;

    private final int maxHttpRequestHeaderSize = 10 * 1024 * 1024 + 1;
    private final int maxHttpPostSize = 100 * 1024 * 1024 + 1;
    private final int threadSelectors = 1;
    private final int threadAcceptors = 2;
    private final int minThreads = 2;
    private final int maxThreads = 5;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        httpServer = new HttpServer();
        httpServer.setMaxHttpPostSize(maxHttpPostSize);
        httpServer.setMaxHttpHeaderSize(maxHttpRequestHeaderSize);
        httpServer.setAcceptors(threadAcceptors);
        httpServer.setSelectors(threadSelectors);
        httpServer.setMinThreads(minThreads);
        httpServer.setMaxThreads(maxThreads);
        httpServer.start();
    }

    @Test
    public void testWebContainerProperties() {
        ConfigurableApplicationContext context = httpServer.getApplicationContext();
        Assert.assertTrue(context instanceof WebServerApplicationContext);

        WebServerApplicationContext webContext = (WebServerApplicationContext) context;
        WebServer webServer = webContext.getWebServer();
        Assert.assertTrue(webServer instanceof JettyWebServer);

        // max request header size
        JettyWebServer jettyServer = (JettyWebServer) webServer;
        Connector[] connectors = jettyServer.getServer().getConnectors();
        Assert.assertNotNull(connectors);
        Assert.assertTrue(connectors.length > 0);
        Assert.assertTrue(connectors[0] instanceof ServerConnector);

        ServerConnector serverConnector = (ServerConnector) connectors[0];
        HttpConfiguration httpConfig = serverConnector
                .getConnectionFactory(HttpConnectionFactory.class)
                .getHttpConfiguration();
        Assert.assertEquals(maxHttpRequestHeaderSize, httpConfig.getRequestHeaderSize());

        // max post size
        Handler handler = jettyServer.getServer().getHandler();
        ServletContextHandler servletContextHandler = findServletContextHandler(handler);
        Assert.assertNotNull(servletContextHandler);
        Assert.assertEquals(maxHttpPostSize, servletContextHandler.getMaxFormContentSize());

        // threads
        Assert.assertEquals(threadAcceptors, serverConnector.getAcceptors());
        Assert.assertEquals(threadSelectors, serverConnector.getSelectorManager().getSelectorCount());
        Assert.assertTrue(serverConnector.getExecutor() instanceof ThreadPool.SizedThreadPool);

        ThreadPool.SizedThreadPool threadPool = (ThreadPool.SizedThreadPool) serverConnector.getExecutor();
        Assert.assertEquals(minThreads, threadPool.getMinThreads());
        Assert.assertEquals(maxThreads, threadPool.getMaxThreads());
    }

    private static ServletContextHandler findServletContextHandler(Handler handler) {
        if (handler == null) {
            return null;
        }
        if (handler instanceof ServletContextHandler && handler.isStarted()) {
            return (ServletContextHandler) handler;
        }
        if (handler instanceof ContextHandlerCollection) {
            for (Handler child : ((ContextHandlerCollection) handler).getHandlers()) {
                ServletContextHandler contextHandler = findServletContextHandler(child);
                if (contextHandler != null) {
                    return contextHandler;
                }
            }
        }
        if (handler instanceof Handler.Wrapper) {
            return findServletContextHandler(((Handler.Wrapper) handler).getHandler());
        }
        return null;
    }

    @After
    public void cleanup() {
        if (httpServer != null) {
            try {
                httpServer.shutdown();
            } catch (Exception e) {
                httpServer = null;
            }
        }
    }
}
