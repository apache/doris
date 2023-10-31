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

package org.apache.doris.scheduler.executor;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.thrift.TUniqueId;

import lombok.Getter;

import java.util.UUID;

@Getter
public abstract class AbstractJobExecutor<T, C> implements JobExecutor<T, C> {

    protected ConnectContext createContext(Job job) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(ClusterNamespace.getClusterNameFromFullName(job.getDbName()));
        ctx.setDatabase(job.getDbName());
        ctx.setQualifiedUser(job.getUser());
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(job.getUser(), "%"));
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        return ctx;
    }

    protected String generateTaskId() {
        return UUID.randomUUID().toString();
    }

    protected TUniqueId generateQueryId(String taskIdString) {
        UUID taskId = UUID.fromString(taskIdString);
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }
}
