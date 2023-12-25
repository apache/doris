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

package org.apache.doris.plugin;

import org.apache.doris.nereids.parser.Dialect;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableListMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import javax.validation.constraints.NotNull;

/**
 * Sql dialect adapter manager, use this manager to refresh dialect plugins periodically.
 */
public class DialectConverterPluginMgr {

    private static final Logger LOG = LogManager.getLogger(DialectConverterPluginMgr.class);

    private static final int REFRESH_INTERVAL_MS = 10000;

    private final PluginMgr pluginMgr;
    // should not be null
    private volatile @NotNull ImmutableListMultimap<Dialect, DialectConverterPlugin> sqlDialectPluginMap;

    public DialectConverterPluginMgr(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    @SuppressWarnings("BusyWait")
    public void init() {
        collectDialectConverterPlugins();
        Thread refreshThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(REFRESH_INTERVAL_MS);
                    collectDialectConverterPlugins();
                } catch (Throwable e) {
                    LOG.warn("Refresh sql dialect plugins failed ", e);
                }
            }
        }, "sqlDialectPluginRefresher");
        refreshThread.setDaemon(true);
        refreshThread.start();
    }

    private void collectDialectConverterPlugins() {
        List<Plugin> plugins = this.pluginMgr.getActivePluginList(PluginInfo.PluginType.DIALECT);
        ImmutableListMultimap.Builder<Dialect, DialectConverterPlugin> builder = ImmutableListMultimap.builder();
        for (Plugin plugin : plugins) {
            Preconditions.checkArgument(plugin instanceof DialectConverterPlugin);
            DialectConverterPlugin dialectConverterPlugin = (DialectConverterPlugin) plugin;
            for (Dialect dialect : dialectConverterPlugin.acceptDialects()) {
                builder.put(dialect, dialectConverterPlugin);
            }
        }
        sqlDialectPluginMap = builder.build();
    }

    /**
     * Use ImmutableListMultimap here, so we can not determine the order of plugins
     * TODO: maybe we can add a order property and return plugins by order
     */
    public List<DialectConverterPlugin> getDialectConverterPlugins(@NotNull Dialect dialect) {
        return sqlDialectPluginMap.get(dialect);
    }

}
