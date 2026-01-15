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

package org.apache.doris.nereids.lineage;

import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.plugin.lineage.AbstractLineagePlugin;

import java.util.List;

public class LineageEventProcessor {

    private final PluginMgr pluginMgr;
    private List<Plugin> lineagePlugins;

    public LineageEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void submitLineageEvent(LineageEvent lineageEvent) {
    }

    public void handleLineageEvent() {
        // Get lineage event from queue
        LineageEvent lineageEvent = new LineageEvent();
        for (Plugin plugin : lineagePlugins) {
            AbstractLineagePlugin lineagePlugin = (AbstractLineagePlugin) plugin;
            if (lineagePlugin.eventFilter()) {
                LineageInfo lineageInfo = LineageInfoExtractor.extractLineageInfo(lineageEvent);
                lineagePlugin.exec(lineageInfo);
            }
        }
    }
}

