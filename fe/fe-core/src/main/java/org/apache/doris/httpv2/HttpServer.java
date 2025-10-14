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

import org.apache.doris.DorisFE;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.CertificateManager;
import org.apache.doris.httpv2.config.JettySslCustomizer;
import org.apache.doris.httpv2.config.SpringLog4j2Config;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SpringBootApplication
@EnableConfigurationProperties
@ServletComponentScan
public class HttpServer extends SpringBootServletInitializer {
    private static final Logger LOG = LogManager.getLogger(HttpServer.class);
    private ConfigurableApplicationContext applicationContext;
    private int port;
    private int httpsPort;
    private int acceptors;
    private int selectors;
    private int maxHttpPostSize;
    private int workers;

    private String keyStorePath;
    private String keyStorePassword;
    private String keyStoreType;
    private String keyStoreAlias;
    private boolean enableHttps;

    private int minThreads;
    private int maxThreads;
    private int maxHttpHeaderSize;


    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
            r -> {
                Thread t = new Thread(r, "HttpServer-Cert-Monitor");
                t.setDaemon(true);
                return t;
            });
    private final AtomicReference<Object> webServerRef = new AtomicReference<>();

    // max retiredSslContextFactories size
    private final int maxRetainedSslContexts = 3;
    // store the replaced ssl context factory, for delay release safe
    private final ConcurrentLinkedQueue<SslContextFactory.Server> retiredSslContextFactories =
            new ConcurrentLinkedQueue<>();

    private final CertificateManager.FileWatcherState certWatcher = new CertificateManager.FileWatcherState();
    private final CertificateManager.FileWatcherState keyWatcher = new CertificateManager.FileWatcherState();
    private final CertificateManager.FileWatcherState caWatcher = new CertificateManager.FileWatcherState();

    private volatile boolean monitoring = false;

    public int getMaxHttpHeaderSize() {
        return maxHttpHeaderSize;
    }

    public void setMaxHttpHeaderSize(int maxHttpHeaderSize) {
        this.maxHttpHeaderSize = maxHttpHeaderSize;
    }

    public int getMinThreads() {
        return minThreads;
    }

    public void setMinThreads(int minThreads) {
        this.minThreads = minThreads;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public void setAcceptors(int acceptors) {
        this.acceptors = acceptors;
    }

    public void setSelectors(int selectors) {
        this.selectors = selectors;
    }

    public void setMaxHttpPostSize(int maxHttpPostSize) {
        this.maxHttpPostSize = maxHttpPostSize;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setHttpsPort(int httpsPort) {
        this.httpsPort = httpsPort;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public void setKeyStoreAlias(String keyStoreAlias) {
        this.keyStoreAlias = keyStoreAlias;
    }

    public void setEnableHttps(boolean enableHttps) {
        this.enableHttps = enableHttps;
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(HttpServer.class);
    }

    public void start() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("server.port", port);
        if (enableHttps) {
            // ssl config
            properties.put("server.ssl.key-store", keyStorePath);
            properties.put("server.ssl.key-store-password", keyStorePassword);
            properties.put("server.ssl.key-store-type", keyStoreType);
            properties.put("server.ssl.keyalias", keyStoreAlias);
            properties.put("server.http.port", port);
            properties.put("server.port", httpsPort);
            properties.put("server.ssl.enabled", true);
        }
        if (FrontendOptions.isBindIPV6()) {
            properties.put("server.address", "::0");
        } else {
            properties.put("server.address", "0.0.0.0");
        }
        properties.put("server.servlet.context-path", "/");
        properties.put("spring.resources.static-locations", "classpath:/static");
        properties.put("spring.http.encoding.charset", "UTF-8");
        properties.put("spring.http.encoding.enabled", true);
        properties.put("spring.http.encoding.force", true);
        // enable jetty config
        properties.put("server.jetty.acceptors", this.acceptors);
        properties.put("server.jetty.max-http-post-size", this.maxHttpPostSize);
        properties.put("server.jetty.selectors", this.selectors);
        properties.put("server.jetty.threadPool.maxThreads", this.maxThreads);
        properties.put("server.jetty.threadPool.minThreads", this.minThreads);
        properties.put("server.max-http-header-size", this.maxHttpHeaderSize);
        // Worker thread pool is not set by default, set according to your needs
        if (this.workers > 0) {
            properties.put("server.jetty.workers", this.workers);
        }
        // This is to disable the spring-boot-devtools restart feature.
        // To avoid some unexpected behavior.
        System.setProperty("spring.devtools.restart.enabled", "false");
        // Value of `DORIS_HOME_DIR` is null in unit test.
        if (DorisFE.DORIS_HOME_DIR != null) {
            System.setProperty("spring.http.multipart.location", DorisFE.DORIS_HOME_DIR);
        }
        System.setProperty("spring.banner.image.location", "doris-logo.png");
        if (FeConstants.runningUnitTest) {
            // this is currently only used for unit test
            properties.put("logging.config", getClass().getClassLoader().getResource("log4j2.xml").getPath());
        } else {
            properties.put("logging.config", Config.custom_config_dir + "/" + SpringLog4j2Config.SPRING_LOG_XML_FILE);
        }
        // Disable automatic shutdown hook registration
        // This prevents Spring Boot from responding to SIGTERM automatically
        // allowing the main process (DorisFE) to control when the HTTP server shuts down
        this.applicationContext = new SpringApplicationBuilder()
                .sources(HttpServer.class)
                .properties(properties)
                .registerShutdownHook(false)
                .listeners((ApplicationListener<ApplicationReadyEvent>) event -> {
                    if (Config.enable_tls) {
                        // Start custom certificate monitoring for Jetty
                        try {
                            ServletWebServerApplicationContext context =
                                    (ServletWebServerApplicationContext) event.getApplicationContext();
                            WebServer webServer = context.getWebServer();
                            setWebServer(webServer);
                            startMonitoringCertificates();
                            LOG.info("Certificate changes will be detected and hot-reloaded automatically");
                        } catch (Exception e) {
                            LOG.error("Falling back to manual restart for certificate updates");
                        }
                    }
                })
                .run(new String[]{});
    }

    /**
     * Explicitly shutdown the HTTP server.
     * This method should be called by the main process (DorisFE) after its graceful shutdown is complete.
     */
    public void shutdown() {
        if (applicationContext != null) {
            LOG.info("Shutting down HTTP server gracefully...");
            applicationContext.close();
            LOG.info("HTTP server shutdown complete");
        }
    }

    public void setWebServer(Object webServer) {
        webServerRef.set(webServer);
    }

    public void startMonitoringCertificates() {
        if (monitoring || !Config.enable_tls) {
            return;
        }

        monitoring = true;
        initLastModifiedTimes();

        // Check certificate files
        scheduler.scheduleWithFixedDelay(
                this::reloadCertificateIfNeeded,
                Config.tls_cert_refresh_interval_seconds,
                Config.tls_cert_refresh_interval_seconds,
                TimeUnit.SECONDS);
        LOG.info("Started certificate monitoring for HTTPS server");
    }

    public void stopMonitoringCertificates() {
        monitoring = false;
        scheduler.shutdown();
        releaseAllRetiredSslContextFactories();
        LOG.info("Stopped certificate monitoring");
    }

    // init cert file modify time
    private void initLastModifiedTimes() {
        try {
            CertificateManager.checkCertificateFile(new File(Config.tls_certificate_path), certWatcher,
                    "certificate", null);

            CertificateManager.checkCertificateFile(new File(Config.tls_private_key_path), keyWatcher,
                    "private key", null);

            CertificateManager.checkCertificateFile(new File(Config.tls_ca_certificate_path), caWatcher,
                    "CA certificate", null);
        } catch (Exception e) {
            LOG.warn("Failed to initialize certificate last modified times", e);
        }
    }

    private void reloadCertificateIfNeeded() {
        try {
            // the checkNum is use for weather we 'can' do reload
            // if ca change checkNum |= 1 << 2
            // if key change checkNum |= 1 << 1
            // if cert change checkNum |= 1
            // then we can determine the current situation based on the value of checkNum
            // if ca file changed, then cert and key also need change, the num is 7
            // if user don't want change key file, only ca and cert change ,the num is 5
            // if key changed, then cert also need change, the num is 3
            // if only cert changed, the num is 1
            // therefore, we can conclude that when checkNum is odd, the certificate needs to be reloaded
            // this approach can eliminate some instances of user error (compare bool value)
            // it may also be used for finer-grained control in the future
            CertificateManager.FileWatcherState.Snapshot caSnapshot = caWatcher.snapshot();
            CertificateManager.FileWatcherState.Snapshot keySnapshot = keyWatcher.snapshot();
            CertificateManager.FileWatcherState.Snapshot certSnapshot = certWatcher.snapshot();

            int checkNum = 0;

            if (CertificateManager.checkCertificateFile(
                    new File(Config.tls_ca_certificate_path), caWatcher,
                    "CA certificate", () -> !monitoring)) {
                checkNum |= 1 << 2;
            }

            if (CertificateManager.checkCertificateFile(
                    new File(Config.tls_private_key_path), keyWatcher,
                    "private key", () -> !monitoring)) {
                checkNum |= 1 << 1;
            }

            if (CertificateManager.checkCertificateFile(
                    new File(Config.tls_certificate_path), certWatcher,
                    "certificate", () -> !monitoring)) {
                checkNum |= 1;
            }

            if ((checkNum & 1) == 1) {
                boolean reloaded = reloadCertificates();
                if (!reloaded) {
                    caSnapshot.restore(caWatcher);
                    keySnapshot.restore(keyWatcher);
                    certSnapshot.restore(certWatcher);
                    LOG.warn("Certificate reload failed, will retry on next check");
                }
            }
        } catch (Exception e) {
            LOG.error("Error checking certificate files", e);
        }
    }

    private boolean reloadCertificates() {
        try {
            Object webServer = webServerRef.get();

            if (webServer == null) {
                LOG.warn("WebServer not set, cannot reload certificates");
                return false;
            }

            LOG.info("Reloading HTTPS certificates...");

            // Use reflection to check if it's a JettyWebServer
            if (webServer.getClass().getName().equals("org.springframework.boot.web.embedded.jetty.JettyWebServer")) {
                return reloadJettyCertificates(webServer);
            } else {
                LOG.warn("Certificate reload not supported for non-Jetty web servers");
                return false;
            }

        } catch (Exception e) {
            LOG.error("Failed to reload certificates", e);
            return false;
        }
    }

    private boolean reloadJettyCertificates(Object jettyWebServer) throws Exception {
        Field serverField = jettyWebServer.getClass().getDeclaredField("server");
        serverField.setAccessible(true);
        Server server = (Server) serverField.get(jettyWebServer);

        if (server == null) {
            LOG.warn("Jetty server is null, cannot reload certificates");
            return false;
        }

        boolean replaced = false;
        Connector[] connectors = server.getConnectors();
        // only one connector by default
        for (Connector connector : connectors) {
            if (connector instanceof ServerConnector) {
                ServerConnector serverConnector = (ServerConnector) connector;

                if (serverConnector.getDefaultConnectionFactory() != null
                        && serverConnector.getConnectionFactory("ssl") != null) {

                    if (replaceConnectorSslContext(serverConnector)) {
                        replaced = true;
                    }
                }
            }
        }
        if (!replaced) {
            LOG.warn("No SSL connectors were reloaded");
        }
        return replaced;
    }

    private boolean replaceConnectorSslContext(ServerConnector connector) throws Exception {
        LOG.info("Replacing SSL context for connector on port {}", connector.getLocalPort());

        SslContextFactory.Server newSslContextFactory;
        try {
            newSslContextFactory = JettySslCustomizer.createSslContextFactory();
        } catch (Exception e) {
            LOG.error("Failed to create SSL context factory via reflection", e);
            return false;
        }
        newSslContextFactory.start();
        boolean replaced = false;
        synchronized (connector) {
            SslConnectionFactory sslConnectionFactory = connector.getConnectionFactory(SslConnectionFactory.class);
            if (sslConnectionFactory == null) {
                LOG.warn("Connector on port {} does not use SSL, skipping replacement", connector.getLocalPort());
                return false;
            }

            Field sslContextFactoryField = SslConnectionFactory.class.getDeclaredField("_sslContextFactory");
            sslContextFactoryField.setAccessible(true);

            SslContextFactory.Server oldSslContextFactory =
                    (SslContextFactory.Server) sslContextFactoryField.get(sslConnectionFactory);

            sslContextFactoryField.set(sslConnectionFactory, newSslContextFactory);
            LOG.info("Replaced SSL context factory with new certificates for port {}", connector.getLocalPort());
            replaced = true;

            if (oldSslContextFactory != null) {
                retiredSslContextFactories.offer(oldSslContextFactory);
                enforceRetiredPoolLimit();
            }
        }
        return replaced;
    }

    // release oldest factory on queue
    private void enforceRetiredPoolLimit() {
        while (retiredSslContextFactories.size() > maxRetainedSslContexts) {
            releaseSslContextFactory(retiredSslContextFactories.poll());
        }
    }

    // release all factory
    private void releaseAllRetiredSslContextFactories() {
        SslContextFactory.Server retired;
        while ((retired = retiredSslContextFactories.poll()) != null) {
            releaseSslContextFactory(retired);
        }
    }

    private void releaseSslContextFactory(SslContextFactory.Server retired) {
        if (retired == null) {
            return;
        }
        try {
            if (retired.isRunning()) {
                retired.stop();
            }
            LOG.info("Released retired SSL context factory");
        } catch (Exception e) {
            LOG.warn("Failed to release retired SSL context factory", e);
        }
    }
}
