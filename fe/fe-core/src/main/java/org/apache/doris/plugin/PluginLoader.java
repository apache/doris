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

import org.apache.doris.common.UserException;

import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Objects;

public abstract class PluginLoader {

    public enum PluginStatus {
        INSTALLING,
        INSTALLED,
        UNINSTALLING,
        UNINSTALLED,
        ERROR;

        private String msg;

        public void setMsg(String msg) {
            this.msg = msg;
        }

        @Override
        public String toString() {
            if (Strings.isNullOrEmpty(msg)) {
                return this.name();
            } else {
                return this.name() + ": " + msg;
            }
        }
    }

    // the root dir of Frontend plugin, should always be Config.plugin_dir
    protected final Path pluginDir;
    // source of plugin, eg, a remote download link of zip file, or a local zip file.
    protected String source;

    protected Plugin plugin;

    protected PluginInfo pluginInfo;

    protected PluginContext pluginContext;

    protected PluginStatus status;

    protected PluginLoader(String path, String source) {
        this.pluginDir = FileSystems.getDefault().getPath(path);
        this.source = source;
        this.plugin = null;
        this.pluginInfo = null;
        this.pluginContext = new PluginContext();
    }

    protected PluginLoader(String path, PluginInfo info) {
        this.pluginDir = FileSystems.getDefault().getPath(path);
        this.source = info.getSource();
        this.plugin = null;
        this.pluginInfo = info;
        this.pluginContext = new PluginContext();
    }

    public abstract void install() throws UserException, IOException;

    public abstract void uninstall() throws IOException, UserException;

    public boolean isDynamicPlugin() {
        return false;
    }

    public PluginInfo getPluginInfo() throws IOException, UserException {
        return pluginInfo;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    public void setStatus(PluginStatus status) {
        setStatus(status, null);
    }

    public void setStatus(PluginStatus status, String msg) {
        this.status = status;
        this.status.setMsg(msg);
    }

    public PluginStatus getStatus() {
        return status;
    }

    public void pluginInstallValid() throws UserException {

    }

    public void pluginUninstallValid() throws UserException {
        // check plugin flags
        if (plugin != null && (plugin.flags() & Plugin.PLUGIN_NOT_DYNAMIC_UNINSTALL) > 0) {
            throw new UserException("plugin " + pluginInfo + " not allow dynamic uninstall");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PluginLoader that = (PluginLoader) o;
        return pluginInfo.equals(that.pluginInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginInfo);
    }
}
