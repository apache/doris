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

package plugin;

import java.io.IOException;

import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AuditPluginDemo extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(AuditPluginDemo.class);

    private final static short EVENT_MASKS = AuditEvent.AUDIT_QUERY_START | AuditEvent.AUDIT_QUERY_END;

    @Override
    public void init(PluginInfo info, PluginContext ctx) {
        super.init(info, ctx);
        LOG.info("this is audit plugin demo init");
    }

    @Override
    public void close() throws IOException {
        super.close();
        LOG.info("this is audit plugin demo close");
    }

    @Override
    public boolean eventFilter(short type, short masks) {
        return type == AuditEvent.AUDIT_QUERY && (masks & EVENT_MASKS) != 0;
    }

    @Override
    public void exec(AuditEvent event) {
        LOG.info("audit demo plugin log: event={} masks={} user={}, ip={}, query={}", event.getEventType(),
                event.getEventMasks(),
                event.getUser(),
                event.getIp(),
                event.getQuery());
    }
}
