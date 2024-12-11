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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * rebalance disk commands' base class
 */
public abstract class AbstractRebalanceDiskCommand extends Command implements NoForward {
    protected AbstractRebalanceDiskCommand(PlanType type) {
        super(type);
    }

    protected List<Backend> getNeedRebalanceDiskBackends(List<String> backends) throws AnalysisException {
        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        List<Backend> needRebalanceDiskBackends = Lists.newArrayList();
        return parseRebalanceDiskBackends(backends, backendsInfo, needRebalanceDiskBackends);
    }

    private List<Backend> parseRebalanceDiskBackends(List<String> backends, ImmutableMap<Long, Backend> backendsInfo,
            List<Backend> needRebalanceDiskBackends) {
        if (backends == null) {
            needRebalanceDiskBackends.addAll(backendsInfo.values());
        } else {
            Map<String, Long> backendsID = new HashMap<>();
            for (Backend backend : backendsInfo.values()) {
                backendsID.put(
                        NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHeartbeatPort()),
                        backend.getId());
            }
            for (String be : backends) {
                if (backendsID.containsKey(be)) {
                    needRebalanceDiskBackends.add(backendsInfo.get(backendsID.get(be)));
                    backendsID.remove(be);
                }
            }
        }
        return needRebalanceDiskBackends;
    }

}
