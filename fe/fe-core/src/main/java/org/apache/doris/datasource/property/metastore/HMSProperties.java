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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.Config;
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.property.ConnectorProperty;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;

import java.util.Map;

@Slf4j
public class HMSProperties extends AbstractHMSProperties {

    private HMSBaseProperties hmsBaseProperties;

    @ConnectorProperty(names = {"hive.enable_hms_events_incremental_sync"},
            required = false,
            description = "Whether to enable incremental sync of hms events.")
    private boolean hmsEventsIncrementalSyncEnabledInput = Config.enable_hms_events_incremental_sync;

    @ConnectorProperty(names = {"hive.hms_events_batch_size_per_rpc"},
            required = false,
            description = "The batch size of hms events per rpc.")
    private int hmsEventisBatchSizePerRpcInput = Config.hms_events_batch_size_per_rpc;

    public HMSProperties(Map<String, String> origProps) {
        super(Type.HMS, origProps);
    }

    @Override
    protected String getResourceConfigPropName() {
        return "hive.conf.resources";
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        initRefreshParams();
        hmsBaseProperties = HMSBaseProperties.of(origProps);
        this.hiveConf = hmsBaseProperties.getHiveConf();
        this.executionAuthenticator = new HadoopExecutionAuthenticator(hmsBaseProperties.getHmsAuthenticator());
    }


    private void initRefreshParams() {
        this.hmsEventsIncrementalSyncEnabled = BooleanUtils.toBoolean(hmsEventsIncrementalSyncEnabledInput);
        this.hmsEventsBatchSizePerRpc = hmsEventisBatchSizePerRpcInput;
    }

}
