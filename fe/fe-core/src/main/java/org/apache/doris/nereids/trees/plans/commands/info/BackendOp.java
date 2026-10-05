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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;

/**
 * BackendOp
 */
public abstract class BackendOp extends AlterSystemOp {
    public static final String MUTLI_TAG_DISABLED_MSG = "Not support multi tags for Backend now. "
            + "You can set 'enable_multi_tags=true' in fe.conf to enable this feature.";
    public static final String NEED_LOCATION_TAG_MSG
            = "Backend must have location type tag. Eg: 'tag.location' = 'xxx'.";
    protected List<String> params;

    protected List<HostInfo> hostInfos;

    protected List<String> ids;

    protected BackendOp(List<String> params) {
        super(AlterOpType.ALTER_OTHER);
        this.params = params;
        this.ids = Lists.newArrayList();
        this.hostInfos = Lists.newArrayList();
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        for (String param : params) {
            if (!param.contains(":")) {
                ids.add(param);
            } else {
                HostInfo hostInfo = SystemInfoService.getHostAndPort(param);
                this.hostInfos.add(hostInfo);
            }

        }
        Preconditions.checkState(!this.hostInfos.isEmpty() || !this.ids.isEmpty(),
                "hostInfos or ids can not be empty");
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException("Not support getProperties for BackendOp");
    }

    public List<HostInfo> getHostInfos() {
        return hostInfos;
    }

    public List<String> getIds() {
        return ids;
    }
}
