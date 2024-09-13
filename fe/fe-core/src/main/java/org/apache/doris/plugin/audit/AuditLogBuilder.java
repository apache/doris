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

import org.apache.doris.common.AuditLog;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditField;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

// A builtin Audit plugin, registered when FE start.
// it will receive "AFTER_QUERY" AuditEvent and print it as a log in fe.audit.log
public class AuditLogBuilder extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLogBuilder.class);

    private final PluginInfo pluginInfo;

    private static final String[] LOAD_ANNONATION_NAMES = {"JobId", "Label", "LoadType", "Db", "TableList",
        "FilePathList", "BrokerUser", "Timestamp", "LoadStartTime", "LoadFinishTime", "ScanRows",
        "ScanBytes", "FileNumber"};

    private final Set<String> loadAnnotationSet;

    private static final String[] STREAM_LOAD_ANNONATION_NAMES = {"Label", "Db", "Table", "User", "ClientIp",
            "Status", "Message", "Url", "TotalRows", "LoadedRows", "FilteredRows", "UnselectedRows",
            "LoadBytes", "StartTime", "FinishTime"};

    private final Set<String> streamLoadAnnotationSet;

    public AuditLogBuilder() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLogBuilder", PluginType.AUDIT,
                "builtin audit logger", DigitalVersion.fromString("0.12.0"),
                DigitalVersion.fromString("1.8.31"), AuditLogBuilder.class.getName(), null, null);
        loadAnnotationSet = Sets.newHashSet(LOAD_ANNONATION_NAMES);
        streamLoadAnnotationSet = Sets.newHashSet(STREAM_LOAD_ANNONATION_NAMES);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public boolean eventFilter(EventType type) {
        return type == EventType.AFTER_QUERY || type == EventType.LOAD_SUCCEED || type == EventType.STREAM_LOAD_FINISH;
    }

    @Override
    public void exec(AuditEvent event) {
        try {
            switch (event.type) {
                case AFTER_QUERY:
                    auditQueryLog(event);
                    break;
                case LOAD_SUCCEED:
                    auditLoadLog(event);
                    break;
                case STREAM_LOAD_FINISH:
                    auditStreamLoadLog(event);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("failed to process audit event", e);
            }
        }
    }

    private void auditQueryLog(AuditEvent event) throws IllegalAccessException {
        StringBuilder sb = new StringBuilder();
        long queryTime = 0;
        // get each field with annotation "AuditField" in AuditEvent
        // and assemble them into a string.
        Field[] fields = event.getClass().getFields();
        for (Field f : fields) {
            AuditField af = f.getAnnotation(AuditField.class);
            if (af == null) {
                continue;
            }

            if (af.value().equals("Timestamp")) {
                continue;
            }

            if (af.value().equals("Time(ms)")) {
                queryTime = (long) f.get(event);
            }
            sb.append("|").append(af.value()).append("=").append(String.valueOf(f.get(event)));
        }

        String auditLog = sb.toString();
        AuditLog.getQueryAudit().log(auditLog);
        // slow query
        if (queryTime > Config.qe_slow_log_ms) {
            AuditLog.getSlowAudit().log(auditLog);
        }
    }

    private void auditLoadLog(AuditEvent event) throws IllegalAccessException {
        Field[] fields = event.getClass().getFields();
        Map<String, String> annotationToFieldValueMap = Maps.newHashMap();
        for (Field f : fields) {
            AuditField af = f.getAnnotation(AuditField.class);
            if (af == null || !loadAnnotationSet.contains(af.value())) {
                continue;
            }
            annotationToFieldValueMap.put(af.value(), String.valueOf(f.get(event)));
        }
        StringBuilder sb = new StringBuilder();
        for (String annotation : LOAD_ANNONATION_NAMES) {
            sb.append("|").append(annotation).append("=").append(annotationToFieldValueMap.get(annotation));
        }
        String auditLog = sb.toString();
        AuditLog.getLoadAudit().log(auditLog);
    }

    private void auditStreamLoadLog(AuditEvent event) throws IllegalAccessException {
        Field[] fields = event.getClass().getFields();
        Map<String, String> annotationToFieldValueMap = Maps.newHashMap();
        for (Field f : fields) {
            AuditField af = f.getAnnotation(AuditField.class);
            if (af == null || !streamLoadAnnotationSet.contains(af.value())) {
                continue;
            }
            annotationToFieldValueMap.put(af.value(), String.valueOf(f.get(event)));
        }
        StringBuilder sb = new StringBuilder();
        for (String annotation : STREAM_LOAD_ANNONATION_NAMES) {
            sb.append("|").append(annotation).append("=").append(annotationToFieldValueMap.get(annotation));
        }
        String auditLog = sb.toString();
        AuditLog.getStreamLoadAudit().log(auditLog);
    }
}
