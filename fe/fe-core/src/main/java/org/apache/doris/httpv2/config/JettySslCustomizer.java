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
import org.apache.doris.common.util.CertificateManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.security.KeyStore;
import java.util.Collections;

@Component
public class JettySslCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {
    private static final Logger LOG = LogManager.getLogger(JettySslCustomizer.class);

    @Override
    public void customize(JettyServletWebServerFactory factory) {
        if (Config.enable_tls) {
            factory.addServerCustomizers(server -> {
                try {
                    createSslConnector(server);
                } catch (Exception e) {
                    LOG.error("Failed to create SSL connector", e);
                    throw new RuntimeException("SSL configuration failed", e);
                }
            });
        } else if (Config.enable_https) {
            factory.setConfigurations(
                    Collections.singleton(new HttpToHttpsJettyConfig())
            );

            factory.addServerCustomizers(server -> {
                HttpConfiguration httpConfiguration = new HttpConfiguration();
                httpConfiguration.setSecurePort(Config.https_port);
                httpConfiguration.setSecureScheme("https");

                ServerConnector connector = new ServerConnector(server);
                connector.addConnectionFactory(new HttpConnectionFactory(httpConfiguration));
                connector.setPort(Config.http_port);
                server.addConnector(connector);
            });
        }
    }

    private void createSslConnector(Server server) {
        try {
            SslContextFactory.Server sslContextFactory = createSslContextFactory();

            // Create HTTP configuration
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setSecureScheme("https");
            httpConfig.setSecurePort(Config.http_port);

            // Create SSL connection factory
            SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(
                    sslContextFactory,
                    "http/1.1"
            );

            // Create HTTP connection factory
            HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);

            // Create SSL server connector
            ServerConnector sslConnector = new ServerConnector(
                    server,
                    sslConnectionFactory,
                    httpConnectionFactory
            );
            sslConnector.setPort(Config.http_port);

            // Replace all existing connectors with SSL connector
            server.setConnectors(new Connector[]{sslConnector});

            LOG.info("Created SSL connector on port {} with PEM certificates", Config.http_port);
        } catch (Exception e) {
            LOG.error("Failed to create SSL connector", e);
            throw new RuntimeException("SSL connector creation failed", e);
        }
    }

    private void configureSslConnector(Server server) {
        try {
            Field sslContextFactoryField = SslConnectionFactory.class.getDeclaredField("_sslContextFactory");
            sslContextFactoryField.setAccessible(true);

            SslContextFactory.Server newContextFactory = createSslContextFactory();
            newContextFactory.start();

            for (Connector connector : server.getConnectors()) {
                if (!(connector instanceof ServerConnector)) {
                    continue;
                }

                ServerConnector sc = (ServerConnector) connector;
                SslConnectionFactory ssl = sc.getConnectionFactory(SslConnectionFactory.class);
                if (ssl == null) {
                    continue;
                }

                SslContextFactory.Server oldContextFactory =
                        (SslContextFactory.Server) sslContextFactoryField.get(ssl);

                sslContextFactoryField.set(ssl, newContextFactory);
                LOG.info("Switched SSL context for connector on port {}", sc.getLocalPort());

                if (oldContextFactory == null) {
                    continue;
                }
                new Thread(() -> {
                    try {
                        waitForConnectionsToDrain(sc, 100_000 /* ms */);
                        oldContextFactory.stop();
                        LOG.info("Stopped old SSL context factory after draining");
                    } catch (Exception e) {
                        LOG.warn("Failed to stop old SSL context factory", e);
                    }
                }, "ssl-context-drainer-" + sc.getLocalPort()).start();
            }
        } catch (Exception e) {
            LOG.error("Failed to init SSL connector", e);
        }
    }

    private void waitForConnectionsToDrain(ServerConnector connector, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (connector.getConnectedEndPoints().isEmpty()) {
                break;
            }
            Thread.sleep(2000);
        }
    }

    public static SslContextFactory.Server createSslContextFactory() {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

        try {
            KeyStore ks = CertificateManager.loadKeyStore(
                    Config.tls_certificate_path,
                    Config.tls_private_key_path,
                    Config.tls_private_key_password.toCharArray()
            );

            sslContextFactory.setKeyStore(ks);
            sslContextFactory.setKeyStorePassword(Config.tls_private_key_password);

            KeyStore ts = CertificateManager.loadTrustStore(Config.tls_ca_certificate_path);
            sslContextFactory.setTrustStore(ts);
            // Client authentication settings
            if (Config.tls_verify_mode.equals("verify_fail_if_no_peer_cert")) {
                sslContextFactory.setNeedClientAuth(true);
            } else if (Config.tls_verify_mode.equals("verify_peer")) {
                sslContextFactory.setWantClientAuth(true);
            } else {
                sslContextFactory.setWantClientAuth(false);
                sslContextFactory.setNeedClientAuth(false);
            }

            sslContextFactory.setSniRequired(false);
            sslContextFactory.setExcludeCipherSuites(
                    "SSL_RSA_WITH_DES_CBC_SHA",
                    "SSL_DHE_RSA_WITH_DES_CBC_SHA",
                    "SSL_DHE_DSS_WITH_DES_CBC_SHA",
                    "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
                    "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA",
                    "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
                    "SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA"
            );

            LOG.info("SSL Context Factory configured successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PEM SSL context factory", e);
        }

        return sslContextFactory;
    }
}
