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
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    // use for old pipeline
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;

    // A countdown latch to mark the completion of each fragment. use for pipelineX
    // fragmentId -> dummy value
    private MarkedCountDownLatch<Integer, Long> profileFragmentDoneSignal;

    // use to merge profile from multi be
    private List<Map<TNetworkAddress, List<RuntimeProfile>>> multiBeProfile = null;

    public ExecutionProfile(TUniqueId queryId, int fragmentNum) {
        executionProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        executionProfile.addChild(fragmentsProfile);
        fragmentProfiles = Lists.newArrayList();
        multiBeProfile = Lists.newArrayList();
        for (int i = 0; i < fragmentNum; i++) {
            fragmentProfiles.add(new RuntimeProfile("Fragment " + i));
            fragmentsProfile.addChild(fragmentProfiles.get(i));
            multiBeProfile.add(new ConcurrentHashMap<TNetworkAddress, List<RuntimeProfile>>());
        }
        loadChannelProfile = new RuntimeProfile("LoadChannels");
        executionProfile.addChild(loadChannelProfile);
    }

    public void addMultiBeProfileByPipelineX(int profileFragmentId, TNetworkAddress address,
            List<RuntimeProfile> taskProfile) {
        multiBeProfile.get(profileFragmentId).put(address, taskProfile);
    }

    private List<List<RuntimeProfile>> getMultiBeProfile(int profileFragmentId) {
        Map<TNetworkAddress, List<RuntimeProfile>> multiPipeline = multiBeProfile.get(profileFragmentId);
        List<List<RuntimeProfile>> allPipelines = Lists.newArrayList();
        int pipelineSize = 0;
        for (List<RuntimeProfile> profiles : multiPipeline.values()) {
            pipelineSize = profiles.size();
            break;
        }
        for (int pipelineIdx = 0; pipelineIdx < pipelineSize; pipelineIdx++) {
            List<RuntimeProfile> allPipelineTask = new ArrayList<RuntimeProfile>();
            for (List<RuntimeProfile> pipelines : multiPipeline.values()) {
                RuntimeProfile pipeline = pipelines.get(pipelineIdx);
                for (Pair<RuntimeProfile, Boolean> runtimeProfile : pipeline.getChildList()) {
                    allPipelineTask.add(runtimeProfile.first);
                }
            }
            allPipelines.add(allPipelineTask);
        }
        return allPipelines;
    }

    private RuntimeProfile getPipelineXAggregatedProfile(Map<Integer, String> planNodeMap) {
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        for (int i = 0; i < fragmentProfiles.size(); ++i) {
            RuntimeProfile newFragmentProfile = new RuntimeProfile("Fragment " + i);
            fragmentsProfile.addChild(newFragmentProfile);
            List<List<RuntimeProfile>> allPipelines = getMultiBeProfile(i);
            int pipelineIdx = 0;
            for (List<RuntimeProfile> allPipelineTask : allPipelines) {
                RuntimeProfile mergedpipelineProfile = new RuntimeProfile(
                        "Pipeline : " + pipelineIdx + "(instance_num="
                                + allPipelineTask.size() + ")",
                        allPipelineTask.get(0).nodeId());
                RuntimeProfile.mergeProfiles(allPipelineTask, mergedpipelineProfile, planNodeMap);
                newFragmentProfile.addChild(mergedpipelineProfile);
                pipelineIdx++;
            }
        }
        return fragmentsProfile;
    }

    private RuntimeProfile getNonPipelineXAggregatedProfile(Map<Integer, String> planNodeMap) {
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        for (int i = 0; i < fragmentProfiles.size(); ++i) {
            RuntimeProfile oldFragmentProfile = fragmentProfiles.get(i);
            RuntimeProfile newFragmentProfile = new RuntimeProfile("Fragment " + i);
            fragmentsProfile.addChild(newFragmentProfile);
            List<RuntimeProfile> allInstanceProfiles = new ArrayList<RuntimeProfile>();
            for (Pair<RuntimeProfile, Boolean> runtimeProfile : oldFragmentProfile.getChildList()) {
                allInstanceProfiles.add(runtimeProfile.first);
            }
            RuntimeProfile mergedInstanceProfile = new RuntimeProfile("Instance" + "(instance_num="
                    + allInstanceProfiles.size() + ")", allInstanceProfiles.get(0).nodeId());
            newFragmentProfile.addChild(mergedInstanceProfile);
            RuntimeProfile.mergeProfiles(allInstanceProfiles, mergedInstanceProfile, planNodeMap);
        }
        return fragmentsProfile;
    }

    public RuntimeProfile getAggregatedFragmentsProfile(Map<Integer, String> planNodeMap) {
        if (enablePipelineX()) {
            /*
             * Fragment 0
             * ---Pipeline 0
             * ------pipelineTask 0
             * ------pipelineTask 0
             * ------pipelineTask 0
             * ---Pipeline 1
             * ------pipelineTask 1
             * ---Pipeline 2
             * ------pipelineTask 2
             * ------pipelineTask 2
             * Fragment 1
             * ---Pipeline 0
             * ------......
             * ---Pipeline 1
             * ------......
             * ---Pipeline 2
             * ------......
             * ......
             */
            return getPipelineXAggregatedProfile(planNodeMap);
        } else {
            /*
             * Fragment 0
             * ---Instance 0
             * ------pipelineTask 0
             * ------pipelineTask 1
             * ------pipelineTask 2
             * ---Instance 1
             * ------pipelineTask 0
             * ------pipelineTask 1
             * ------pipelineTask 2
             * ---Instance 2
             * ------pipelineTask 0
             * ------pipelineTask 1
             * ------pipelineTask 2
             * Fragment 1
             * ---Instance 0
             * ---Instance 1
             * ---Instance 2
             * ......
             */
            return getNonPipelineXAggregatedProfile(planNodeMap);
        }
    }

    public RuntimeProfile getExecutionProfile() {
        return executionProfile;
    }

    public RuntimeProfile getLoadChannelProfile() {
        return loadChannelProfile;
    }

    public List<RuntimeProfile> getFragmentProfiles() {
        return fragmentProfiles;
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

    private boolean enablePipelineX() {
        return profileFragmentDoneSignal != null;
    }

    public void markFragments(int fragments) {
        profileFragmentDoneSignal = new MarkedCountDownLatch<>(fragments);
        for (int fragmentId = 0; fragmentId < fragments; fragmentId++) {
            profileFragmentDoneSignal.addMark(fragmentId, -1L /* value is meaningless */);
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

        if (isFinished && profileFragmentDoneSignal != null) {
            try {
                profileFragmentDoneSignal.await(2, TimeUnit.SECONDS);
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
        }
        if (profileFragmentDoneSignal != null) {
            profileFragmentDoneSignal.countDownToZero(new Status());
        }
    }

    public void markOneInstanceDone(TUniqueId fragmentInstanceId) {
        if (profileDoneSignal != null) {
            if (!profileDoneSignal.markedCountDown(fragmentInstanceId, -1L)) {
                LOG.warn("Mark instance {} done failed", DebugUtil.printId(fragmentInstanceId));
            }
        }
    }

    public void markOneFragmentDone(int fragmentId) {
        if (profileFragmentDoneSignal != null) {
            if (!profileFragmentDoneSignal.markedCountDown(fragmentId, -1L)) {
                LOG.warn("Mark fragment {} done failed", fragmentId);
            }
        }
    }

    public boolean awaitAllInstancesDone(long waitTimeS) throws InterruptedException {
        if (profileDoneSignal == null) {
            return true;
        }
        return profileDoneSignal.await(waitTimeS, TimeUnit.SECONDS);
    }

    public boolean awaitAllFragmentsDone(long waitTimeS) throws InterruptedException {
        if (profileFragmentDoneSignal == null) {
            return true;
        }
        return profileFragmentDoneSignal.await(waitTimeS, TimeUnit.SECONDS);
    }

    public boolean isAllInstancesDone() {
        if (profileDoneSignal == null) {
            return true;
        }
        return profileDoneSignal.getCount() == 0;
    }

    public boolean isAllFragmentsDone() {
        if (profileFragmentDoneSignal == null) {
            return true;
        }
        return profileFragmentDoneSignal.getCount() == 0;
    }

    public void addInstanceProfile(int fragmentId, RuntimeProfile instanceProfile) {
        Preconditions.checkArgument(fragmentId < fragmentProfiles.size(),
                fragmentId + " vs. " + fragmentProfiles.size());
        fragmentProfiles.get(fragmentId).addChild(instanceProfile);
    }
}
