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

import java.util.List;

import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.plugin.PluginType;

public class Auditor {

    private PluginMgr pluginMgr;

    public Auditor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void notifyQueryEvent(ConnectContext ctx, String query, short eventMasks) {
        AuditEvent event = AuditEvent.createQueryEvent(ctx, query);
        notifyEvent(event, AuditEvent.AUDIT_QUERY, eventMasks);
    }

    public void notifyEvent(AuditEvent event, short eventType, short eventMasks) {
        event.setEventTypeAndMasks(eventType, eventMasks);

        List<Plugin> pluginList = pluginMgr.getPluginList(PluginType.AUDIT);

        pluginList.parallelStream().forEach(plugin ->
        {
            AuditPlugin a = (AuditPlugin) plugin;
            if (a.eventFilter(eventType, eventMasks)) {
                a.exec(event);
            }
        });
    }
}
