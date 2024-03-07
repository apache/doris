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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.qe.QeProcessorImpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkloadActionCancelQuery implements WorkloadAction {

    private static final Logger LOG = LogManager.getLogger(WorkloadActionCancelQuery.class);

    @Override
    public void exec(WorkloadQueryInfo queryInfo) {
        if (queryInfo.context != null && !queryInfo.context.isKilled()
                && queryInfo.tUniqueId != null
                && QeProcessorImpl.INSTANCE.getCoordinator(queryInfo.tUniqueId) != null) {
            LOG.info("cancel query {} triggered by query schedule policy.", queryInfo.queryId);
            queryInfo.context.cancelQuery();
        }
    }

    public static WorkloadActionCancelQuery createWorkloadAction() {
        return new WorkloadActionCancelQuery();
    }

    @Override
    public WorkloadActionType getWorkloadActionType() {
        return WorkloadActionType.CANCEL_QUERY;
    }
}
