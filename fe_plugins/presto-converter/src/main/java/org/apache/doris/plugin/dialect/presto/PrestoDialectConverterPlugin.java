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

package org.apache.doris.plugin.dialect.presto;

import com.google.common.base.Preconditions;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.plugin.DialectConverterPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class PrestoDialectConverterPlugin extends Plugin implements DialectConverterPlugin {

    private volatile boolean isInit = false;
    private volatile boolean isClosed = false;
    private volatile String targetURL = null;

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            loadConfig(ctx, info.getProperties());
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
        targetURL = properties.get("target_url");
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
    }

    @Override
    public ImmutableSet<Dialect> acceptDialects() {
        return ImmutableSet.of(Dialect.PRESTO);
    }

    @Override
    public @Nullable String convertSql(String originSql, SessionVariable sessionVariable) {
        Preconditions.checkNotNull(targetURL);
        return SQLDialectUtils.convertSql(targetURL, originSql);
    }
}
