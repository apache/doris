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

package org.apache.doris.stack.component;

import org.apache.doris.stack.dao.ManagerDatabaseRepository;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.exception.NoPermissionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DatabuildComponent {

    @Autowired
    private ManagerDatabaseRepository databaseRepository;

    /**
     * Check the consistency between the user manager metadata and the cluster.
     * If it is correct, return the database data information
     * @param dbId
     * @param clusterId
     * @throws Exception
     */
    public ManagerDatabaseEntity checkClusterDatabase(int dbId, int clusterId) throws Exception {
        ManagerDatabaseEntity database = databaseRepository.findById(dbId).get();
        log.debug("Check database {} belong to cluster {}.", dbId, clusterId);
        if (database.getClusterId() != clusterId) {
            log.error("The database {} not belong to cluster {}.", dbId, clusterId);
            throw new NoPermissionException();
        }
        return database;
    }
}
