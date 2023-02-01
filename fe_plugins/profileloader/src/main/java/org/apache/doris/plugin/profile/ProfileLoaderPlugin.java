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

package org.apache.doris.plugin.profile;


import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.ProfileEvent;
import org.apache.doris.plugin.ProfilePlugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;

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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * This plugin will load profile log to specified doris table at specified interval
 */
public class ProfileLoaderPlugin extends Plugin implements ProfilePlugin {
    private final static Logger LOG = LogManager.getLogger(ProfileLoaderPlugin.class);

    private static final String COLUMN_SEPARATOR = new String(new byte[]{0x01});
    private static final String LINE_DELIMITER = new String(new byte[]{0x00});

    private BlockingQueue<ProfileEvent> profileEventQueue;
    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;
    private volatile Thread loadThread;

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            ProfileLoaderConf conf = buildConfig(ctx, info.getProperties());
            this.profileEventQueue = Queues.newLinkedBlockingDeque(conf.maxQueueSize);
            this.loadThread = new Thread(new LoadWorker(conf), "profile loader thread");
            this.loadThread.start();

            isInit = true;
        }
    }

    private ProfileLoaderConf buildConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
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

        Map<String, String> properties = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(Function.identity(), props::getProperty));
        return new ProfileLoaderConf(properties, ctx.getFeIdentity());
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

    public void exec(ProfileEvent event) {
        try {
            profileEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            LOG.debug("encounter exception when putting current audit batch, discard current audit event", e);
        }
    }

    public static class ProfileLoaderConf {

        public ProfileLoaderConf(Map<String, String> properties, String feIdentity) throws PluginException {
            try {
                this.feIdentity = feIdentity;

                maxBatchSize = properties.containsKey(PROP_MAX_BATCH_SIZE) ?
                        Long.valueOf(properties.get(PROP_MAX_BATCH_SIZE)) : DEFAULT_MAX_BATCH_SIZE;

                maxBatchIntervalSec = properties.containsKey(PROP_MAX_BATCH_INTERVAL_SEC) ?
                        Long.valueOf(properties.get(PROP_MAX_BATCH_INTERVAL_SEC)) : DEFAULT_MAX_BATCH_INTERVAL_SEC;

                maxQueueSize = properties.containsKey(PROP_MAX_QUEUE_SIZE) ?
                        Integer.valueOf(properties.get(PROP_MAX_QUEUE_SIZE)) : DEFAULT_MAX_QUEUE_SIZE;

                frontendHostPort = properties.containsKey(PROP_FRONTEND_HOST_PORT) ?
                        properties.get(PROP_FRONTEND_HOST_PORT) : DEFAULT_FRONTEND_HOST_PORT;

                user = properties.containsKey(PROP_USER) ? properties.get(PROP_USER) : DEFAULT_USER;

                password = properties.containsKey(PROP_PASSWORD) ? properties.get(PROP_PASSWORD) : DEFAULT_PASSWORD;

                database = properties.containsKey(PROP_DATABASE) ? properties.get(PROP_DATABASE) : DEFAULT_DATABASE;

                profileLogTable = properties.containsKey(PROP_PROFILE_LOG_TABLE) ?
                        properties.get(PROP_PROFILE_LOG_TABLE) : DEFAULT_PROFILE_LOG_TABLE;

                maxStmtLength = properties.containsKey(PROP_MAX_STMT_LENGTH) ?
                        Integer.parseInt(properties.get(PROP_MAX_STMT_LENGTH)) : DEFAULT_MAX_STMT_LENGTH;

                maxProfileLength = properties.containsKey(PROP_MAX_PROFILE_LENGTH) ?
                        Integer.parseInt(properties.get(PROP_MAX_PROFILE_LENGTH)) : DEFAULT_MAX_PROFILE_LENGTH;

                skipStmt = properties.containsKey(PROP_SKIP_STMT) ?
                        Boolean.parseBoolean(properties.get(PROP_SKIP_STMT)) :
                        DEFAULT_SKIP_STMT;

                skipTypes = properties.containsKey(PROP_SKIP_TYPES) ?
                        ImmutableSet.copyOf(properties.get(PROP_SKIP_TYPES).trim().split(",")) :
                        DEFAULT_SKIP_TYPES;

                slowQueryMs = properties.containsKey(PROP_SLOW_QUERY_MS) ?
                        Integer.parseInt(properties.get(PROP_SLOW_QUERY_MS)) : DEFAULT_SLOW_QUERY_MS;

                slowLoadMs = properties.containsKey(PROP_SLOW_LOAD_MS) ?
                        Integer.parseInt(properties.get(PROP_SLOW_LOAD_MS)) : DEFAULT_SLOW_LOAD_MS;

                slowInsertMs = properties.containsKey(PROP_SLOW_INSERT_MS) ?
                        Integer.parseInt(properties.get(PROP_SLOW_INSERT_MS)) : DEFAULT_SLOW_INSERT_MS;

                slowExportMs = properties.containsKey(PROP_SLOW_EXPORT_MS) ?
                        Integer.parseInt(properties.get(PROP_SLOW_EXPORT_MS)) : DEFAULT_SLOW_EXPORT_MS;

            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }

        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";
        public static final String PROP_FRONTEND_HOST_PORT = "frontend_host_port";
        // the max profile length to be loaded in profile table.
        public static final String PROP_MAX_PROFILE_LENGTH = "max_profile_length";
        public static final String PROP_PROFILE_LOG_TABLE = "profile_log_table";
        // the max stmt length to be loaded in profile table.
        public static final String PROP_MAX_STMT_LENGTH = "max_stmt_length";
        public static final String PROP_MAX_BATCH_SIZE = "max_batch_size";
        public static final String PROP_MAX_QUEUE_SIZE = "max_queue_size";
        public static final String PROP_SLOW_EXPORT_MS = "slow_export_ms";
        public static final String PROP_SLOW_INSERT_MS = "slow_insert_ms";
        public static final String PROP_SLOW_QUERY_MS = "slow_query_ms";
        public static final String PROP_SLOW_LOAD_MS = "slow_load_ms";
        public static final String PROP_SKIP_TYPES = "skip_types";
        public static final String PROP_SKIP_STMT = "skip_stmt";
        public static final String PROP_PASSWORD = "password";
        public static final String PROP_DATABASE = "database";
        public static final String PROP_USER = "user";

        public static final Set<String> DEFAULT_SKIP_TYPES = Collections.EMPTY_SET;

        public static final String DEFAULT_PROFILE_LOG_TABLE = "doris_profile_log_tbl__";
        public static final String DEFAULT_FRONTEND_HOST_PORT = "127.0.0.1:8030";
        public static final String DEFAULT_DATABASE = "doris_audit_db__";
        public static final String DEFAULT_USER = "root";
        public static final String DEFAULT_PASSWORD = "";

        public static final long DEFAULT_MAX_BATCH_SIZE = 50 * 1024 * 1024;
        public static final long DEFAULT_MAX_BATCH_INTERVAL_SEC = 60;

        public static final int DEFAULT_MAX_PROFILE_LENGTH = 81920;
        public static final int DEFAULT_SLOW_EXPORT_MS = 1800000;
        public static final int DEFAULT_SLOW_INSERT_MS = 1800000;
        public static final int DEFAULT_SLOW_LOAD_MS = 1800000;
        public static final int DEFAULT_MAX_STMT_LENGTH = 4096;
        public static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
        public static final int DEFAULT_SLOW_QUERY_MS = 15000;

        public static final boolean DEFAULT_SKIP_STMT = true;

        public final long maxBatchSize;
        public final long maxBatchIntervalSec;
        public final int maxQueueSize;
        public final String frontendHostPort;
        public final String user;
        public final String password;
        public final String database;
        public final String profileLogTable;
        // the identity of FE which run this plugin
        public final String feIdentity;
        public final boolean skipStmt;
        public final Set<String> skipTypes;
        public final int maxStmtLength;
        public final int maxProfileLength;
        public final int slowQueryMs;
        public final int slowLoadMs;
        public final int slowInsertMs;
        public final int slowExportMs;

    }

    private class LoadWorker implements Runnable {
        private final DorisStreamLoader loader;
        private final ProfileLoaderConf conf;
        private volatile long lastLoadTimeProfileLog;

        public LoadWorker(ProfileLoaderConf conf) {
            this.conf = conf;
            this.loader = new DorisStreamLoader(conf);
            this.lastLoadTimeProfileLog = System.currentTimeMillis();
        }

        public void run() {
            StringBuilder profileLogBuffer = new StringBuilder();
            while (!isClosed) {
                try {
                    ProfileEvent event = profileEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {

                        if (!filterEvent(event)) {
                            continue;
                        }

                        fillLogBuffer(event, profileLogBuffer);

                        if (loadIfNecessary(profileLogBuffer)) {
                            profileLogBuffer = new StringBuilder();
                        }

                    }
                } catch (InterruptedException ie) {
                    LOG.debug("Encounter exception when loading current profile batch", ie);
                } catch (Exception e) {
                    LOG.error("LoadWorker run failed for profile loader, error:", e);
                }
            }
        }

        private boolean filterEvent(ProfileEvent event) {
            if (conf.skipTypes.contains(event.profileType)) {
                return false;
            }

            switch (event.profileType) {
                case "Query":
                    return event.totalTimeMs >= conf.slowQueryMs;
                case "Load":
                    return event.totalTimeMs >= conf.slowLoadMs;
                case "Insert":
                    return event.totalTimeMs >= conf.slowInsertMs;
                case "Export":
                    return event.totalTimeMs >= conf.slowExportMs;
                default:
                    // just save all profiles if not in these known types
                    return true;
            }

        }

        private boolean loadIfNecessary(StringBuilder logBuffer) {
            long currentTime = System.currentTimeMillis();

            if (logBuffer.length() >= conf.maxBatchSize
                    || currentTime - lastLoadTimeProfileLog >= conf.maxBatchIntervalSec * 1000) {
                // begin to load
                try {
                    DorisStreamLoader.LoadResponse response = loader.loadBatch(logBuffer.toString().getBytes());
                    LOG.info("Profile loader response: {}", response);
                } catch (Exception e) {
                    LOG.warn("Encounter exception when putting current profile batch, discard current batch", e);
                } finally {
                    // make a new string builder to receive following events.
                    lastLoadTimeProfileLog = currentTime;
                }
                return true;
            }

            return false;
        }

        private void fillLogBuffer(ProfileEvent event, StringBuilder logBuffer) {
            logBuffer.append(event.jobId).append(COLUMN_SEPARATOR);
            logBuffer.append(event.queryId).append(COLUMN_SEPARATOR);
            logBuffer.append(event.user).append(COLUMN_SEPARATOR);
            logBuffer.append(event.defaultDb).append(COLUMN_SEPARATOR);
            logBuffer.append(event.profileType).append(COLUMN_SEPARATOR);
            logBuffer.append(event.startTime).append(COLUMN_SEPARATOR);
            logBuffer.append(event.endTime).append(COLUMN_SEPARATOR);
            logBuffer.append(event.totalTime).append(COLUMN_SEPARATOR);
            logBuffer.append(event.totalTimeMs).append(COLUMN_SEPARATOR);
            logBuffer.append(event.queryState).append(COLUMN_SEPARATOR);
            logBuffer.append(event.traceId).append(COLUMN_SEPARATOR);
            // trim the stmt to avoid too long
            // use `getBytes().length` to get real byte length
            String stmt = conf.skipStmt ? "N/A" : truncateByBytes(event.stmt, conf.maxStmtLength);
            logBuffer.append(stmt).append(COLUMN_SEPARATOR);
            // trim the profile to avoid too long
            // use `getBytes().length` to get real byte length
            String profile = truncateByBytes(event.profile, conf.maxProfileLength);
            logBuffer.append(profile).append(LINE_DELIMITER);
        }

        private String truncateByBytes(String str, int length) {
            int maxLen = Math.min(length, str.getBytes().length);
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

    }

}
