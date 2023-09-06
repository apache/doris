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

package org.apache.doris.qe;

import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.proto.Types;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import java.util.List;

public class QueryCancelWorker extends MasterDaemon {
    private SystemInfoService systemInfoService;

    public QueryCancelWorker(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    @Override
    protected void runAfterCatalogReady() {
        List<Backend> allBackends = systemInfoService.getAllBackends();

        for (Coordinator co : QeProcessorImpl.INSTANCE.getAllCoordinators()) {
            if (co.shouldCancel(allBackends)) {
                // TODO(zhiqiang): We need more clear cancel message, so that user can figure out what happened
                //  by searching log.
                co.cancel(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
            }
        }
    }
}
