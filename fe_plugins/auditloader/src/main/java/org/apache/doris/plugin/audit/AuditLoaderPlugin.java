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

import org.apache.doris.common.Config;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private StringBuilder auditLogBuffer = new StringBuilder();
    private StringBuilder slowLogBuffer = new StringBuilder();
    private long lastLoadTimeAuditLog = 0;
    private long lastLoadTimeSlowLog = 0;

    private BlockingQueue<AuditEvent> auditEventQueue;
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
            this.lastLoadTimeAuditLog = System.currentTimeMillis();
            this.lastLoadTimeSlowLog = System.currentTimeMillis();

            loadConfig(ctx, info.getProperties());

            this.auditEventQueue = Queues.newLinkedBlockingDeque(conf.maxQueueSize);
            this.streamLoader = new DorisStreamLoader(conf);
            this.loadThread = new Thread(new LoadWorker(this.streamLoader), "audit loader thread");
            this.loadThread.start();

            isInit = true;
        }
    }

    private void loadConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
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

        for (Map.Entry<String, String> entry : pluginInfoProperties.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        final Map<String, String> properties = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(Function.identity(), props::getProperty));
        conf = new AuditLoaderConf();
        conf.init(properties);
        conf.feIdentity = ctx.getFeIdentity();
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
        try {
            auditEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            LOG.debug("encounter exception when putting current audit batch, discard current audit event", e);
        }
    }

    private void assembleAudit(AuditEvent event) {
        if (conf.enableSlowLog && event.queryTime > Config.qe_slow_log_ms) {
            fillLogBuffer(event, slowLogBuffer);
        }
        fillLogBuffer(event, auditLogBuffer);
    }

    private void fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        logBuffer.append(event.queryId).append("\t");
        logBuffer.append(longToTimeString(event.timestamp)).append("\t");
        logBuffer.append(event.clientIp).append("\t");
        logBuffer.append(event.user).append("\t");
        logBuffer.append(event.catalog).append("\t");
        logBuffer.append(event.db).append("\t");
        logBuffer.append(event.state).append("\t");
        logBuffer.append(event.errorCode).append("\t");
        logBuffer.append(event.errorMessage).append("\t");
        logBuffer.append(event.queryTime).append("\t");
        logBuffer.append(event.scanBytes).append("\t");
        logBuffer.append(event.scanRows).append("\t");
        logBuffer.append(event.returnRows).append("\t");
        logBuffer.append(event.stmtId).append("\t");
        logBuffer.append(event.isQuery ? 1 : 0).append("\t");
        logBuffer.append(event.feIp).append("\t");
        logBuffer.append(event.cpuTimeMs).append("\t");
        logBuffer.append(event.sqlHash).append("\t");
        logBuffer.append(event.sqlDigest).append("\t");
        logBuffer.append(event.peakMemoryBytes).append("\t");
        // trim the query to avoid too long
        // use `getBytes().length` to get real byte length
        String stmt = truncateByBytes(event.stmt).replace("\n", " ").replace("\t", " ");
        LOG.debug("receive audit event with stmt: {}", stmt);
        logBuffer.append(stmt).append("\n");
    }

    private String truncateByBytes(String str) {
        int maxLen = Math.min(conf.max_stmt_length, str.getBytes().length);
        if (maxLen >= str.getBytes().length) {
            return str;
        }
        Charset utf8Charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = utf8Charset.newDecoder();
        byte[] sb = str.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(sb, 0, maxLen);
        CharBuffer charBuffer = CharBuffer.allocate(maxLen);
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(buffer, charBuffer, true);
        decoder.flush(charBuffer);
        return new String(charBuffer.array(), 0, charBuffer.position());
    }

    private void loadIfNecessary(DorisStreamLoader loader, boolean slowLog) {
        StringBuilder logBuffer = slowLog ? slowLogBuffer : auditLogBuffer;
        long lastLoadTime = slowLog ? lastLoadTimeSlowLog : lastLoadTimeAuditLog;
        long currentTime = System.currentTimeMillis();
        
        if (logBuffer.length() >= conf.maxBatchSize || currentTime - lastLoadTime >= conf.maxBatchIntervalSec * 1000) {         
            // begin to load
            try {
                DorisStreamLoader.LoadResponse response = loader.loadBatch(logBuffer, slowLog);
                LOG.debug("audit loader response: {}", response);
            } catch (Exception e) {
                LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
            } finally {
                // make a new string builder to receive following events.
                resetLogBufferAndLastLoadTime(currentTime, slowLog);
            }
        }

        return;
    }

    private void resetLogBufferAndLastLoadTime(long currentTime, boolean slowLog) {
        if (slowLog) {
            this.slowLogBuffer = new StringBuilder();
            lastLoadTimeSlowLog = currentTime;
        } else {
            this.auditLogBuffer = new StringBuilder();
            lastLoadTimeAuditLog = currentTime;
        }

        return;
    }

    public static class AuditLoaderConf {
        public static final String PROP_MAX_BATCH_SIZE = "max_batch_size";
        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";
        public static final String PROP_MAX_QUEUE_SIZE = "max_queue_size";
        public static final String PROP_FRONTEND_HOST_PORT = "frontend_host_port";
        public static final String PROP_USER = "user";
        public static final String PROP_PASSWORD = "password";
        public static final String PROP_DATABASE = "database";
        public static final String PROP_TABLE = "table";
        public static final String PROP_AUDIT_LOG_TABLE = "audit_log_table";
        public static final String PROP_SLOW_LOG_TABLE = "slow_log_table";
        public static final String PROP_ENABLE_SLOW_LOG = "enable_slow_log";
        // the max stmt length to be loaded in audit table.
        public static final String MAX_STMT_LENGTH = "max_stmt_length";

        public long maxBatchSize = 50 * 1024 * 1024;
        public long maxBatchIntervalSec = 60;
        public int maxQueueSize = 1000;
        public String frontendHostPort = "127.0.0.1:8030";
        public String user = "root";
        public String password = "";
        public String database = "doris_audit_db__";
        public String auditLogTable = "doris_audit_log_tbl__";
        public String slowLogTable = "doris_slow_log_tbl__";
        public boolean enableSlowLog = false;
        // the identity of FE which run this plugin
        public String feIdentity = "";
        public int max_stmt_length = 4096;

        public void init(Map<String, String> properties) throws PluginException {
            try {
                if (properties.containsKey(PROP_MAX_BATCH_SIZE)) {
                    maxBatchSize = Long.valueOf(properties.get(PROP_MAX_BATCH_SIZE));
                }
                if (properties.containsKey(PROP_MAX_BATCH_INTERVAL_SEC)) {
                    maxBatchIntervalSec = Long.valueOf(properties.get(PROP_MAX_BATCH_INTERVAL_SEC));
                }
                if (properties.containsKey(PROP_MAX_QUEUE_SIZE)) {
                    maxQueueSize = Integer.valueOf(properties.get(PROP_MAX_QUEUE_SIZE));
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
                // If plugin.conf is not changed, the audit logs are imported to previous table
                if (properties.containsKey(PROP_TABLE)) {
                    auditLogTable = properties.get(PROP_TABLE);
                }
                if (properties.containsKey(PROP_AUDIT_LOG_TABLE)) {
                    auditLogTable = properties.get(PROP_AUDIT_LOG_TABLE);
                }
                if (properties.containsKey(PROP_SLOW_LOG_TABLE)) {
                    slowLogTable = properties.get(PROP_SLOW_LOG_TABLE);
                }
                if (properties.containsKey(PROP_ENABLE_SLOW_LOG)) {
                    enableSlowLog = Boolean.valueOf(properties.get(PROP_ENABLE_SLOW_LOG));
                }
                if (properties.containsKey(MAX_STMT_LENGTH)) {
                    max_stmt_length = Integer.parseInt(properties.get(MAX_STMT_LENGTH));
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
                    AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleAudit(event);
                        // process slow audit logs
                        if (conf.enableSlowLog) {
                            loadIfNecessary(loader, true);
                        }
                        // process all audit logs
                        loadIfNecessary(loader, false);
                    }
                } catch (InterruptedException ie) {
                    LOG.debug("encounter exception when loading current audit batch", ie);
                } catch (Exception e) {
                    LOG.error("run audit logger error:", e);
                }
            }
        }
    }

    public static String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return "1900-01-01 00:00:00";
        }
        return DATETIME_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneId.systemDefault()));
    }
}
