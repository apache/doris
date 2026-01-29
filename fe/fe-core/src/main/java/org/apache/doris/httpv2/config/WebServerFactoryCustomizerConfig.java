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

package org.apache.doris.httpv2.config;

import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.springframework.boot.web.embedded.jetty.ConfigurableJettyWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class WebServerFactoryCustomizerConfig implements WebServerFactoryCustomizer<ConfigurableJettyWebServerFactory> {
    private static final Logger LOG = LogManager.getLogger(WebServerFactoryCustomizerConfig.class);

    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {

        ((JettyServletWebServerFactory) factory).addServerCustomizers(server -> {
            WebAppContext context = server.getDescendant(WebAppContext.class);
            if (context != null) {
                try {
                    JettyWebSocketServletContainerInitializer.configure(context, null);
                } catch (Exception e) {
                    LOG.error("Failed to initialize WebSocket support", e);
                    throw new RuntimeException("Failed to initialize WebSocket support", e);
                }
            }
        });

        if (Config.enable_https) {
            ((JettyServletWebServerFactory) factory).setConfigurations(
                    Collections.singletonList(new HttpToHttpsJettyConfig())
            );

            factory.addServerCustomizers(server -> {
                if (server.getConnectors() != null && server.getConnectors().length > 0) {
                    ServerConnector existingConnector = (ServerConnector) server.getConnectors()[0];
                    HttpConnectionFactory httpFactory =
                            existingConnector.getConnectionFactory(HttpConnectionFactory.class);
                    if (httpFactory != null) {
                        HttpConfiguration httpConfig = httpFactory.getHttpConfiguration();
                        httpConfig.setSecurePort(Config.https_port);
                        httpConfig.setSecureScheme("https");
                    }
                }
            });
        }
    }
}
