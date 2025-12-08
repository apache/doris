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

import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.util.JdbcUtils
import org.slf4j.Logger

import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContextBuilder

import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory

import javax.net.ssl.*
import java.security.KeyStore
import java.io.FileInputStream
import java.util.Scanner
import org.apache.http.ssl.SSLContexts

// make sure PluginQueryTimeoutDebugger quit gracefully
class PluginQueryTimeoutDebuggerHolder {
    static final PluginQueryTimeoutDebugger staticResource = new PluginQueryTimeoutDebugger()

    static {
        Runtime.runtime.addShutdownHook {
            staticResource?.stopWorker()
        }
    }
}

PluginQueryTimeoutDebugger.jdbcUrl = context.config.jdbcUrl
PluginQueryTimeoutDebugger.jdbcUser = context.config.jdbcUser
PluginQueryTimeoutDebugger.jdbcPassword = context.config.jdbcPassword
PluginQueryTimeoutDebugger.skip = !context.config.excludeDockerTest
PluginQueryTimeoutDebugger.logger = logger
PluginQueryTimeoutDebuggerHolder.staticResource.startWorker()
PluginQueryTimeoutDebugger.enableTLS = context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true") ?: false
PluginQueryTimeoutDebugger.tlsVerifyMode = context.config.otherConfigs.get("tlsVerifyMode")?.toString()?.toLowerCase() ?: "none"
PluginQueryTimeoutDebugger.trustStorePath = context.config.otherConfigs.get("trustStorePath")?.toString()
PluginQueryTimeoutDebugger.trustStorePassword = context.config.otherConfigs.get("trustStorePassword")?.toString() ?: ""
PluginQueryTimeoutDebugger.trustStoreType = context.config.otherConfigs.get("trustStoreType")?.toString() ?: 'PKCS12'
PluginQueryTimeoutDebugger.keyStorePath = context.config.otherConfigs.get("keyStorePath")?.toString()
PluginQueryTimeoutDebugger.keyStorePassword = context.config.otherConfigs.get("keyStorePassword")?.toString() ?: ""
PluginQueryTimeoutDebugger.keyStoreType = context.config.otherConfigs.get("keyStoreType")?.toString() ?: 'PKCS12'

/**
 * print pipeline tasks every 1 minutes to help debugging query timeout.
 * be list refreshed every 5 minutes.
 */
class PluginQueryTimeoutDebugger {
    static private final String HostColumnName = "Host"
    static private final String PortColumnName = "HttpPort"
    static private final int HTTP_TIMEOUT = 5000
    static private final long BACKEND_REFRESH_INTERVAL = 5 * 60 * 1000

    static public String jdbcUrl
    static public String jdbcUser
    static public String jdbcPassword
    static public Logger logger

    static public Boolean skip
    
    static public Boolean enableTLS
    static public String tlsVerifyMode
    static public String trustStorePath
    static public String trustStorePassword
    static public String trustStoreType
    static public String keyStorePath
    static public String keyStorePassword
    static public String keyStoreType

    private ScheduledExecutorService scheduler
    private List<String> backendUrls = []
    private long lastBackendRefreshTime = 0

    // catch all exceptions in timer function.
    private void startWorker() {
        if (skip) {
            logger.info("docker case skip this plugin")
            return
        }
        if (scheduler?.isShutdown() == false) {
            logger.warn("worker already started")
            return
        }

        scheduler = Executors.newSingleThreadScheduledExecutor { r ->
            Thread thread = new Thread(r)
            thread.setName("query-timeout-debugger-thread")
            thread.setDaemon(true)
            return thread
        }
        scheduler.scheduleAtFixedRate({
            try {
                work()
            } catch (Exception e) {
                logger.warn("work exception: ${e.getMessage()}", e)
            }
        }, 0, 1, TimeUnit.MINUTES)

        logger.info("worker started with scheduler")
    }

    private void stopWorker() {
        logger.info("stop worker")
        if (scheduler != null) {
            scheduler.shutdown()
            if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
                logger.warn("worker scheduler forced to stop")
            }
        }
    }

    private void work() {
        initBackendsUrls()
        for (String url : backendUrls) {    
            logger.info("${url} pipeline tasks: ${curl(url)}")
        }
    }

    void initBackendsUrls() {
        // refreshed every BACKEND_REFRESH_INTERVAL.
        long now = System.currentTimeMillis()
        if (!backendUrls.isEmpty() && (now - lastBackendRefreshTime < BACKEND_REFRESH_INTERVAL)) {
            return
        }
        lastBackendRefreshTime = now

        List<String> urls = []
        DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword).withCloseable { conn ->
            def (result, meta) = JdbcUtils.executeToList(conn, "show backends")
            int hostIndex = -1
            int portIndex = -1

            for (int i = 0; i < meta.getColumnCount(); i++) {
                if (meta.getColumnLabel(i+1) == HostColumnName) {
                    hostIndex = i
                } else if (meta.getColumnLabel(i+1) == PortColumnName) {
                    portIndex = i
                }
            }

            if (hostIndex != -1 && portIndex != -1) {
                for (int i = 0; i < result.size(); i++) {
                    urls.add(String.format("http://%s:%s/api/running_pipeline_tasks/180", result.get(i).get(hostIndex), result.get(i).get(portIndex)))
                }
                backendUrls = urls
            }
        }

        logger.info("backends: ${backendUrls}")
    }

    String curl(String urlStr) {
        HttpURLConnection connection = null

        try {
            if (enableTLS) {
                if (urlStr.startsWith("http://")) {
                    urlStr = urlStr.replace("http://", "https://")
                }

                def sslContextBuilder = SSLContext.getInstance("TLSv1.2")

                KeyManager[] keyManagers = null
                TrustManager[] trustManagers = null

                // 客户端证书
                if (keyStorePath) {
                    KeyStore keyStore = KeyStore.getInstance(keyStoreType)
                    keyStore.load(new FileInputStream(keyStorePath), keyStorePassword.toCharArray())
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                    kmf.init(keyStore, keyStorePassword.toCharArray())
                    keyManagers = kmf.getKeyManagers()
                }

                // 服务端信任证书
                if (tlsVerifyMode == "none") {
                    trustManagers = [ [ checkClientTrusted:{ a,b->}, checkServerTrusted:{ a,b->}, getAcceptedIssuers:{ [] as java.security.cert.X509Certificate[] } ] as X509TrustManager ]
                } else if (trustStorePath) {
                    KeyStore trustStore = KeyStore.getInstance(trustStoreType)
                    trustStore.load(new FileInputStream(trustStorePath), trustStorePassword.toCharArray())
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                    tmf.init(trustStore)
                    trustManagers = tmf.getTrustManagers()
                }

                sslContextBuilder.init(keyManagers, trustManagers, new java.security.SecureRandom())
                SSLContext sslContext = sslContextBuilder

                def sslSocketFactory = sslContext.getSocketFactory()
                connection = (HttpsURLConnection) new URL(urlStr).openConnection()
                connection.setSSLSocketFactory(sslSocketFactory)

                // 如果 tlsVerifyMode == none，跳过主机名验证
                if (tlsVerifyMode == "none") {
                    (connection as HttpsURLConnection).setHostnameVerifier({ h, s -> true } as HostnameVerifier)
                }
            } else {
                connection = (HttpURLConnection) new URL(urlStr).openConnection()
            }

            connection.setRequestMethod("GET")
            connection.setConnectTimeout(10000)
            connection.setReadTimeout(30000)

            int responseCode = connection.getResponseCode()
            boolean success = (200..<300).contains(responseCode)

            def stream = success ? connection.getInputStream() : connection.getErrorStream()
            def responseBody = stream ? new Scanner(stream).useDelimiter("\\A").next() : ""
            int exitcode = success ? 0 : -1
            def err = success ? "" : "HTTP ${responseCode}"

            return [exitcode, responseBody, err]

        } catch (Exception e) {
            logger.error("mycurl TLS request failed: ${e.message}", e)
            return [-1, "", e.message]
        } finally {
            connection?.disconnect()
        }
    }
}