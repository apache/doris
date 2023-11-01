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

package org.apache.doris.common.profile;

import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * ExecutionProfile is used to collect profile of a complete query plan(including query or load).
 * Need to call addToProfileAsChild() to add it to the root profile.
 * It has the following structure:
 *  Execution Profile:
 *      Fragment 0:
 *          Instance 0:
 *          ...
 *      Fragment 1:
 *          Instance 0:
 *          ...
 *      ...
 *      LoadChannels:  // only for load job
 */
public class ExecutionProfile {
    private static final Logger LOG = LogManager.getLogger(ExecutionProfile.class);

    // The root profile of this execution task
    private RuntimeProfile executionProfile;
    // Profiles for each fragment. And the InstanceProfile is the child of fragment profile.
    // Which will be added to fragment profile when calling Coordinator::sendFragment()
    private List<RuntimeProfile> fragmentProfiles;
    // Profile for load channels. Only for load job.
    private RuntimeProfile loadChannelProfile;
    // A countdown latch to mark the completion of each instance.
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;

    private int waitCount = 0;

    private TUniqueId queryId;

    public ExecutionProfile(TUniqueId queryId, int fragmentNum) {
        executionProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        executionProfile.addChild(fragmentsProfile);
        fragmentProfiles = Lists.newArrayList();
        for (int i = 0; i < fragmentNum; i++) {
            fragmentProfiles.add(new RuntimeProfile("Fragment " + i));
            fragmentsProfile.addChild(fragmentProfiles.get(i));
        }
        loadChannelProfile = new RuntimeProfile("LoadChannels");
        executionProfile.addChild(loadChannelProfile);
        this.queryId = queryId;
    }

    public RuntimeProfile getExecutionProfile() {
        return executionProfile;
    }

    public RuntimeProfile getLoadChannelProfile() {
        return loadChannelProfile;
    }

    public void addToProfileAsChild(RuntimeProfile rootProfile) {
        rootProfile.addChild(executionProfile);
    }

    public void markInstances(Set<TUniqueId> instanceIds) {
        profileDoneSignal = new MarkedCountDownLatch<>(instanceIds.size());
        for (TUniqueId instanceId : instanceIds) {
            profileDoneSignal.addMark(instanceId, -1L /* value is meaningless */);
        }
    }

    public void update(long startTime, boolean isFinished) {
        if (startTime > 0) {
            executionProfile.getCounterTotalTime().setValue(TUnit.TIME_MS, TimeUtils.getElapsedTimeMs(startTime));
        }
        // Wait for all backends to finish reporting when writing profile last time.
        if (isFinished && profileDoneSignal != null) {
            try {
                profileDoneSignal.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                LOG.warn("signal await error", e1);
            }
        }

        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            fragmentProfile.sortChildren();
        }
    }

    public void onCancel() {
        if (profileDoneSignal != null) {
            // count down to zero to notify all objects waiting for this
            profileDoneSignal.countDownToZero(new Status());
            LOG.info("Query {} unfinished instance: {}", DebugUtil.printId(queryId),  profileDoneSignal.getLeftMarks()
                    .stream().map(e -> DebugUtil.printId(e.getKey())).toArray());
        }
    }

    public void markOneInstanceDone(TUniqueId fragmentInstanceId) {
        if (profileDoneSignal != null) {
            if (profileDoneSignal.markedCountDown(fragmentInstanceId, -1L)) {
                LOG.info("Mark instance {} done succeed", DebugUtil.printId(fragmentInstanceId));
            } else {
                LOG.warn("Mark instance {} done failed", DebugUtil.printId(fragmentInstanceId));
            }
        }
    }

    public boolean awaitAllInstancesDone(long waitTimeS) throws InterruptedException {
        if (profileDoneSignal == null) {
            return true;
        }

        waitCount++;

        for (Entry<TUniqueId, Long> entry : profileDoneSignal.getLeftMarks()) {
            if (waitCount > 2) {
                LOG.info("Query {} waiting instance {}, waitCount: {}",
                        DebugUtil.printId(queryId), DebugUtil.printId(entry.getKey()), waitCount);
            }
        }

        return profileDoneSignal.await(waitTimeS, TimeUnit.SECONDS);
    }

    public boolean isAllInstancesDone() {
        if (profileDoneSignal == null) {
            return true;
        }
        return profileDoneSignal.getCount() == 0;
    }

    public void addInstanceProfile(int instanceIdx, RuntimeProfile instanceProfile) {
        Preconditions.checkArgument(instanceIdx < fragmentProfiles.size(),
                instanceIdx + " vs. " + fragmentProfiles.size());
        fragmentProfiles.get(instanceIdx).addChild(instanceProfile);
    }
}
