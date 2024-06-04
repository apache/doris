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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Multimap;

import java.util.Iterator;

public class CloudTablet extends Tablet {

    public CloudTablet() {
        super();
    }

    public CloudTablet(long tabletId) {
        super(tabletId);
    }

    @Override
    public Replica getReplicaByBackendId(long backendId) {
        if (!replicas.isEmpty()) {
            return replicas.get(0);
        }
        return null;
    }

    private Multimap<Long, Long> backendPathMapReprocess(Multimap<Long, Long> pathMap) throws UserException {
        if (pathMap.containsKey(-1L)) {
            pathMap.removeAll(-1L);
            if (pathMap.isEmpty()) {
                throw new UserException(InternalErrorCode.META_NOT_FOUND_ERR,
                        SystemInfoService.NOT_USING_VALID_CLUSTER_MSG);
            }
        }

        return pathMap;
    }

    @Override
    public Multimap<Long, Long> getNormalReplicaBackendPathMap() throws UserException {
        Multimap<Long, Long> pathMap = super.getNormalReplicaBackendPathMap();
        return backendPathMapReprocess(pathMap);
    }

    public Multimap<Long, Long> getNormalReplicaBackendPathMapCloud(String beEndpoint) throws UserException {
        Multimap<Long, Long> pathMap = super.getNormalReplicaBackendPathMapCloud(beEndpoint);
        return backendPathMapReprocess(pathMap);
    }

    @Override
    protected boolean isLatestReplicaAndDeleteOld(Replica newReplica) {
        boolean delete = false;
        boolean hasBackend = false;
        long version = newReplica.getVersion();
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            hasBackend = true;
            Replica replica = iterator.next();
            if (replica.getVersion() <= version) {
                iterator.remove();
                delete = true;
            }
        }

        return delete || !hasBackend;
    }

    public void addReplica(Replica replica, boolean isRestore) {
        if (isLatestReplicaAndDeleteOld(replica)) {
            replicas.add(replica);
            if (!isRestore) {
                Env.getCurrentInvertedIndex().addReplica(id, replica);
            }
        }
    }

}
