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
PluginQueryTimeoutDebugger.logger = logger
PluginQueryTimeoutDebuggerHolder.staticResource.startWorker()

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

    private ScheduledExecutorService scheduler
    private List<String> backendUrls = []
    private long lastBackendRefreshTime = 0

    // catch all exceptions in timer function.
    private void startWorker() {
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
            URL url = new URL(urlStr)
            connection = url.openConnection() as HttpURLConnection
            connection.requestMethod = "GET"
            connection.connectTimeout = HTTP_TIMEOUT
            connection.readTimeout = HTTP_TIMEOUT
            
            if (connection.responseCode == 200) {
                return connection.inputStream.text
            } else {
                throw new Exception("curl ${urlStr} failed, code: ${connection.responseCode}")
            }
        } finally {
            connection?.disconnect()
        }
    }
}
