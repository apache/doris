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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.qe.VariableMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Periodically refreshes GLOBAL baseline state from the shared internal table on each non-checkpoint FE. The
 * interval follows the default session variable and is clamped to at least one second. The manager also performs
 * leadership-gated repairs and mutation settlement during refresh. SESSION stores are not refreshed here.
 */
public class BaselineRefreshDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(BaselineRefreshDaemon.class);

    private static final BaselineRefreshDaemon INSTANCE = new BaselineRefreshDaemon();

    private BaselineRefreshDaemon() {
        super("BaselineRefreshDaemon",
                VariableMgr.getDefaultSessionVariable().getSpmBaselineRefreshIntervalSeconds() * 1000L);
    }

    public static BaselineRefreshDaemon getInstance() {
        return INSTANCE;
    }

    @Override
    protected void runAfterCatalogReady() {
        // Re-read the interval every cycle so SET GLOBAL takes effect within one old
        // interval. Clamp to >= 1s so a misconfiguration cannot spin the thread.
        long intervalMs = Math.max(1L,
                VariableMgr.getDefaultSessionVariable().getSpmBaselineRefreshIntervalSeconds())
                * 1000L;
        setInterval(intervalMs);
        if (Env.isCheckpointThread()) {
            return;
        }
        try {
            BaselineManager.getInstance().refreshFromInternalTable();
        } catch (Throwable t) {
            LOG.warn("SPM baseline refresh cycle failed", t);
        }
    }
}
