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

package org.apache.doris.alter;

import org.apache.doris.catalog.Replica;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class CooldownHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CooldownHandler.class);

    // tabletId->replicaId, it is used to hold replica which is used to upload segment to remote storage.
    private Map<Long, Long> uploadingReplicaMap = new HashMap<>();

    private static volatile CooldownHandler INSTANCE = null;

    public static CooldownHandler getInstance() {
        if (INSTANCE == null) {
            synchronized (CooldownHandler.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CooldownHandler();
                }
            }
        }
        return INSTANCE;
    }

    public void handleSyncCooldownType(Map<Long, Replica> replicaCooldownMap) {

    }
    private void syncCooldownConf() {

    }

    @Override
    protected void runAfterCatalogReady() {

    }
}
