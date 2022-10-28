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

package org.apache.doris.planner.external;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class BackendPolicy {
    private static final Logger LOG = LogManager.getLogger(BackendPolicy.class);
    private final List<Backend> backends = Lists.newArrayList();

    private int nextBe = 0;

    public void init() throws UserException {
        Set<Tag> tags = Sets.newHashSet();
        if (ConnectContext.get() != null && ConnectContext.get().getCurrentUserIdentity() != null) {
            String qualifiedUser = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
            tags = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
            if (tags == UserProperty.INVALID_RESOURCE_TAGS) {
                throw new UserException("No valid resource tag for user: " + qualifiedUser);
            }
        } else {
            LOG.debug("user info in ExternalFileScanNode should not be null, add log to observer");
        }

        // scan node is used for query
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .needLoadAvailable()
                .addTags(tags)
                .preferComputeNode()
                .assignCandidateNum(Config.backend_num_for_federation)
                .build();
        backends.addAll(policy.getCandidateBackends(Env.getCurrentSystemInfo().getIdToBackend().values()));
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
        }
    }

    public Backend getNextBe() {
        Backend selectedBackend = backends.get(nextBe++);
        nextBe = nextBe % backends.size();
        return selectedBackend;
    }

    public int numBackends() {
        return backends.size();
    }
}
