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

import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class AuditPluginDemo extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(AuditPluginDemo.class);

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);
        LOG.info("this is audit plugin demo init");
    }

    @Override
    public void close() throws IOException {
        super.close();
        LOG.info("this is audit plugin demo close");
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    public void exec(AuditEvent event) {
        LOG.info("audit demo plugin log exec");
    }
}
