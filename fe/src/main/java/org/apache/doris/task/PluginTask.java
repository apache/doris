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

package org.apache.doris.task;

import java.util.List;

import org.apache.doris.thrift.TInstallPluginReq;
import org.apache.doris.thrift.TPluginMetaInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUninstallPluginReq;

public class PluginTask extends AgentTask {

    private List<TPluginMetaInfo> plugins;

    public PluginTask(long backendId, TTaskType taskType, long signature, List<TPluginMetaInfo> pluginList) {
        super(null, backendId, taskType, -1, -1, -1, -1, -1, signature);

        this.plugins = pluginList;
    }

    public TInstallPluginReq toInstallThrift() {

        TInstallPluginReq req = new TInstallPluginReq();
        req.plugin_infos = plugins;

        return req;
    }

    public TUninstallPluginReq toUninstallThrift() {
        TUninstallPluginReq req = new TUninstallPluginReq();
        req.plugin_infos = plugins;

        return req;
    }
}
