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

package org.apache.doris.qe;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;

// A builtin Audit plugin, registered when FE start.
// it will receive "AFTER_QUERY" AuditEventy and print it as a log in fe.audit.log
public class AuditLogBuilder extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLogBuilder.class);

    private PluginInfo pluginInfo;

    public AuditLogBuilder() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLogBuilder", PluginType.AUDIT,
                "builtin audit logger", DigitalVersion.fromString("0.12.0"), 
                DigitalVersion.fromString("1.8.31"), AuditLogBuilder.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public boolean eventFilter(EventType type) {
        return type == EventType.AFTER_QUERY;
    }

    @Override
    public void exec(AuditEvent event) {
        try {
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

                if (af.value().equals("Time")) {
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
        } catch (Exception e) {
            LOG.debug("failed to process audit event", e);
        }
    }
}
