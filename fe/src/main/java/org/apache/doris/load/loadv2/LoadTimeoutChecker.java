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

package org.apache.doris.load.loadv2;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LoadTimeoutChecker is performed to cancel the timeout job.
 * The job which is not finished, not cancelled, not isCommitting will be checked.
 * The standard of timeout is CurrentTS > (CreateTs + timeoutSeconds * 1000).
 */
public class LoadTimeoutChecker extends Daemon {
    private static final Logger LOG = LogManager.getLogger(LoadTimeoutChecker.class);

    private LoadManager loadManager;

    public LoadTimeoutChecker(LoadManager loadManager) {
        super("Load job timeout checker", Config.load_checker_interval_second * 1000);
        this.loadManager = loadManager;
    }

    @Override
    protected void runOneCycle() {
        try {
            loadManager.processTimeoutJobs();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of LoadJobScheduler with error message {}", e.getMessage(), e);
        }
    }
}
