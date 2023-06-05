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
    @Override
    public void customize(ConfigurableJettyWebServerFactory factory) {
        if (Config.enable_https) {
            ((JettyServletWebServerFactory) factory).setConfigurations(
                    Collections.singleton(new HttpToHttpsJettyConfig())
            );

            factory.addServerCustomizers(
                    server -> {
                        HttpConfiguration httpConfiguration = new HttpConfiguration();
                        httpConfiguration.setSecurePort(Config.https_port);
                        httpConfiguration.setSecureScheme("https");

                        ServerConnector connector = new ServerConnector(server);
                        connector.addConnectionFactory(new HttpConnectionFactory(httpConfiguration));
                        connector.setPort(Config.http_port);

                        server.addConnector(connector);
                    }
            );
        }
    }
}
