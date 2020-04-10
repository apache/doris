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

package org.apache.doris.plugin.audit;

import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private StringBuilder auditBuffer = new StringBuilder();
    private long lastLoadTime = 0;

    private BlockingQueue<StringBuilder> batchQueue = new LinkedBlockingDeque<StringBuilder>(1);
    private DorisStreamLoader streamLoader;
    private Thread loadThread;

    private AuditLoaderConf conf;
    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            this.lastLoadTime = System.currentTimeMillis();

            loadConfig(ctx);

            this.streamLoader = new DorisStreamLoader(conf);
            this.loadThread = new Thread(new LoadWorker(this.streamLoader), "audit loader thread");
            this.loadThread.start();

            isInit = true;
        }
    }

    private void loadConfig(PluginContext ctx) throws PluginException {
        Path pluginPath = FileSystems.getDefault().getPath(ctx.getPluginPath());
        if (!Files.exists(pluginPath)) {
            throw new PluginException("plugin path does not exist: " + pluginPath);
        }

        Path confFile = pluginPath.resolve("plugin.conf");
        if (!Files.exists(confFile)) {
            throw new PluginException("plugin conf file does not exist: " + confFile);
        }

        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(confFile)) {
            props.load(stream);
        } catch (IOException e) {
            throw new PluginException(e.getMessage());
        }
        final Map<String, String> properties = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(Function.identity(), props::getProperty));

        conf = new AuditLoaderConf();
        conf.init(properties);
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                LOG.debug("encounter exception when closing the audit loader", e);
            }
        }
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    public void exec(AuditEvent event) {
        assembleAudit(event);
        loadIfNecessary();
    }

    private void assembleAudit(AuditEvent event) {
        auditBuffer.append(event.queryId).append("\t");
        auditBuffer.append(longToTimeString(event.timestamp)).append("\t");
        auditBuffer.append(event.clientIp).append("\t");
        auditBuffer.append(event.user).append("\t");
        auditBuffer.append(event.db).append("\t");
        auditBuffer.append(event.state).append("\t");
        auditBuffer.append(event.queryTime).append("\t");
        auditBuffer.append(event.scanBytes).append("\t");
        auditBuffer.append(event.scanRows).append("\t");
        auditBuffer.append(event.returnRows).append("\t");
        auditBuffer.append(event.stmtId).append("\t");
        auditBuffer.append(event.isQuery ? 1 : 0).append("\t");
        auditBuffer.append(event.feIp).append("\t");
        // trim the query to avoid too long
        int maxLen = Math.min(2048, event.stmt.length());
        String stmt = event.stmt.substring(0, maxLen).replace("\t", " ");
        LOG.debug("receive audit event with stmt: {}", stmt);
        auditBuffer.append(stmt).append("\n");
    }

    private void loadIfNecessary() {
        if (auditBuffer.length() < conf.maxBatchSize && System.currentTimeMillis() - lastLoadTime < conf.maxBatchIntervalSec * 1000) {
            return;
        }

        lastLoadTime = System.currentTimeMillis();
        // begin to load
        try {
            if (!batchQueue.isEmpty()) {
                // TODO(cmy): if queue is not empty, which means the last batch is not processed.
                // In order to ensure that the system can run normally, here we directly
                // discard the current batch. If this problem occurs frequently,
                // improvement can be considered.
                throw new PluginException("The previous batch is not processed, and the current batch is discarded.");
            }

            batchQueue.put(this.auditBuffer);
        } catch (Exception e) {
            LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
        } finally {
            // make a new string builder to receive following events.
            this.auditBuffer = new StringBuilder();
        }

        return;
    }

    public static class AuditLoaderConf {
        public static final String PROP_MAX_BATCH_SIZE = "max_batch_size";
        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";
        public static final String PROP_FRONTEND_HOST_PORT = "frontend_host_port";
        public static final String PROP_USER = "user";
        public static final String PROP_PASSWORD = "password";
        public static final String PROP_DATABASE = "database";
        public static final String PROP_TABLE = "table";

        public long maxBatchSize = 50 * 1024 * 1024;
        public long maxBatchIntervalSec = 60;
        public String frontendHostPort = "127.0.0.1:9030";
        public String user = "root";
        public String password = "";
        public String database = "__doris_audit_db";
        public String table = "__doris_audit_tbl";

        public void init(Map<String, String> properties) throws PluginException {
            try {
                if (properties.containsKey(PROP_MAX_BATCH_SIZE)) {
                    maxBatchSize = Long.valueOf(properties.get(PROP_MAX_BATCH_SIZE));
                }
                if (properties.containsKey(PROP_MAX_BATCH_INTERVAL_SEC)) {
                    maxBatchIntervalSec = Long.valueOf(properties.get(PROP_MAX_BATCH_INTERVAL_SEC));
                }
                if (properties.containsKey(PROP_FRONTEND_HOST_PORT)) {
                    frontendHostPort = properties.get(PROP_FRONTEND_HOST_PORT);
                }
                if (properties.containsKey(PROP_USER)) {
                    user = properties.get(PROP_USER);
                }
                if (properties.containsKey(PROP_PASSWORD)) {
                    password = properties.get(PROP_PASSWORD);
                }
                if (properties.containsKey(PROP_DATABASE)) {
                    database = properties.get(PROP_DATABASE);
                }
                if (properties.containsKey(PROP_TABLE)) {
                    table = properties.get(PROP_TABLE);
                }
            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }
    }

    private class LoadWorker implements Runnable {
        private DorisStreamLoader loader;

        public LoadWorker(DorisStreamLoader loader) {
            this.loader = loader;
        }

        public void run() {
            while (!isClosed) {
                try {
                    StringBuilder batch = batchQueue.poll(5, TimeUnit.SECONDS);
                    if (batch == null) {
                        continue;
                    }

                    DorisStreamLoader.LoadResponse response = loader.loadBatch(batch);
                    LOG.debug("audit loader response: {}", response);
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when loading current audit batch", e);
                    continue;
                }
            }
        }
    }

    public static synchronized String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return "1900-01-01 00:00:00";
        }
        return DATETIME_FORMAT.format(new Date(timeStamp));
    }
}
