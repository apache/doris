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

import org.apache.doris.common.Config;
import org.apache.doris.service.FrontendOptions;

public class PluginContext {

    // the dir path where the plugin's files saved
    private String pluginPath;
    // the identity of the FE which load this plugin. Some plugins need to know which FE is running with it.
    // currently, we just use FE ip and editlog port
    private String feIdentity;

    public PluginContext() {
        this.feIdentity = FrontendOptions.getLocalHostAddress() + "_" + Config.edit_log_port;
    }

    protected void setPluginPath(String pluginPath) {
        this.pluginPath = pluginPath;
    }

    public String getPluginPath() {
        return pluginPath;
    }

    public String getFeIdentity() {
        return feIdentity;
    }
}
