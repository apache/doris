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

package org.apache.doris.cloud.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class CloudRoutineLoadManager extends RoutineLoadManager {
    private static final Logger LOG = LogManager.getLogger(CloudRoutineLoadManager.class);

    @Override
    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob, String dbName, String tableName)
                    throws UserException {
        if (!Strings.isNullOrEmpty(ConnectContext.get().getCloudCluster())) {
            routineLoadJob.setCloudCluster(ConnectContext.get().getCloudCluster());
        } else {
            throw new UserException("cloud cluster is empty, please specify cloud cluster");
        }
        super.addRoutineLoadJob(routineLoadJob, dbName, tableName);
    }

    @Override
    protected List<Long> getAvailableBackendIds(long jobId) throws LoadException {
        RoutineLoadJob routineLoadJob = getJob(jobId);
        String cloudClusterId = routineLoadJob.getCloudClusterId();
        if (Strings.isNullOrEmpty(cloudClusterId)) {
            LOG.warn("cluster id is empty");
            throw new LoadException("cluster id is empty");
        }

        return ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getBackendsByClusterId(cloudClusterId)
                .stream()
                .filter(Backend::isAlive)
                .map(Backend::getId)
                .collect(Collectors.toList());
    }
}
