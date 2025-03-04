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

package org.apache.doris.nereids.stats;

import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.HboPlanInfoProvider;

import java.util.concurrent.ExecutorService;

public class HboPlanStatisticsManager extends MasterDaemon {
    private static volatile HboPlanStatisticsManager INSTANCE = null;
    private HboPlanStatisticsProvider HboPlanStatisticsProvider;
    private HboPlanInfoProvider HboPlanInfoProvider;
    private final ExecutorService hboExecutor;

    HboPlanStatisticsManager()
    {
        super("hbo-manager", 1 * 1000);
        HboPlanStatisticsProvider = new MemoryHboPlanStatisticsProvider();
        hboExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                20, 100, "hbo-thread-pool", true);
        HboPlanInfoProvider = new HboPlanInfoProvider();
        //planCanonicalInfoProvider = new CachingPlanCanonicalInfoProvider(HboPlanStatisticsProvider, newObjectMapper, metadata);
    }

    public static HboPlanStatisticsManager getInstance() {
        // TODO: should with current session and configuration info and will be used in runtime stats collection
        // and plan matching during stats calculator
        if (INSTANCE == null) {
            synchronized (HboPlanStatisticsManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HboPlanStatisticsManager();
                    INSTANCE.start();
                }
            }
        }
        return INSTANCE;
    }

    public HboPlanStatisticsProvider getHboPlanStatisticsProvider() {
        return HboPlanStatisticsProvider;
    }

    public HboPlanInfoProvider getHboPlanInfoProvider() {
        return HboPlanInfoProvider;
    }

    @Override
    protected void runAfterCatalogReady() {}

}