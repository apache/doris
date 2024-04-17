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

import org.apache.commons.io.FileUtils;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

/**
 * Profile is a class to record the execution time of a query. It has the
 * following structure: root profile: // summary of this profile, such as start
 * time, end time, query id, etc. [SummaryProfile] // each execution profile is
 * a complete execution of a query, a job may contain multiple queries.
 * [List<ExecutionProfile>].
 * There maybe multi execution profiles for one job, for example broker load job.
 * It will create one execution profile for every single load task.
 *
 * SummaryProfile: Summary: Execution Summary:
 *
 *
 * ExecutionProfile1: Fragment 0: Fragment 1: ...
 * ExecutionProfile2: Fragment 0: Fragment 1: ...
 *
 * ExecutionProfile: Fragment 0: Fragment 1: ...
 * And also summary profile contains plan information, but execution profile is for
 * be execution time.
 * StmtExecutor(Profile) ---> Coordinator(ExecutionProfile)
 */
public class Profile {
    private static final Logger LOG = LogManager.getLogger(Profile.class);
    private static final int MergedProfileLevel = 1;
    private static final String PROFILE_META_FILE_NAME = "profile_meta";
    private static final String EXECUTION_PROFILE_FILE_NAME = "execution_profile";
    public static final String SEPERATOR = "_";

    // For broker load, it has its specific name, for other load/query, the it will be 
    // the same witch profile id in the SummaryProfile.
    private String id = "";
    private boolean isPipelineX = true;
    // summaryProfile will be serialized to disk as JSON, and we can recover it from disk
    // recover of SummaryProfile is important, because it contains the meta information of the profile
    // we need it to construct memory index for profile retrieving.
    private SummaryProfile summaryProfile = new SummaryProfile();
    private List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    // profileStoragePath will only be assigned when:
    // 1. profile is stored to disk
    // 2. or profile is loaded from disk
    private String profileStorageDir = "";
    // isFinished means the coordinator or stmtexecutor is finished.
    // does not mean the profile report has finished, since the report is async.
    private boolean isFinished = false;
    // when coordinator finishes, it will mark finish time.
    // we will wait for about 5 seconds to see if all profiles have been reported.
    // if not, we will store the profile to disk, and release the memory,
    // futher report will be ignored.
    // why MIN_VALUE? So that we can use PriorityQueue to sort profile by finish time.
    private long queryFinishTimestamp = Long.MIN_VALUE;
    private long profileStoreTimestamp = 0;
    private Map<Integer, String> planNodeMap = Maps.newHashMap();
    private int profileLevel = 3;

    // Need default constructor for read from disk
    public Profile() {}

    public Profile(boolean isEnable, int profileLevel, boolean isPipelineX) {
        this.isPipelineX = isPipelineX;
        this.summaryProfile = new SummaryProfile();
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isFinished = !isEnable;
        this.profileLevel = profileLevel;
    }

    // For load task, the profile contains many execution profiles
    public void addExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            LOG.warn("try to set a null excecution profile, it is abnormal", new Exception());
            return;
        }
        if (this.isPipelineX) {
            executionProfile.setPipelineX();
        }
        executionProfile.setSummaryProfile(summaryProfile);
        this.executionProfiles.add(executionProfile);
    }

    public List<ExecutionProfile> getExecutionProfiles() {
        return this.executionProfiles;
    }

    public long getProfileStoreTimestamp() {
        return this.profileStoreTimestamp;
    }

    public long getQueryFinishTimestamp() {
        return this.queryFinishTimestamp;
    }


    public void setId(String id) {
        this.id = id;
    }

    // This API will also add the profile to ProfileManager, so that we could get the profile from ProfileManager.
    // isFinished ONLY means the cooridnator or stmtexecutor is finished.
    public synchronized void updateSummary(long startTime, Map<String, String> summaryInfo, boolean isFinished,
            Planner planner) {
        try {
            if (this.isFinished) {
                return;
            }
            summaryProfile.update(summaryInfo);
            this.setId(summaryProfile.getProfileId());
            this.isFinished = isFinished;

            for (ExecutionProfile executionProfile : executionProfiles) {
                // Tell execution profile the start time
                executionProfile.update(startTime, isFinished);
            }
            // Nerids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            ProfileManager.getInstance().pushProfile(this);
        } catch (Throwable t) {
            LOG.warn("update profile failed", t);
            throw t;
        }
    }

    // For UT
    public void setSummaryProfile(SummaryProfile summaryProfile) {
        this.summaryProfile = summaryProfile;
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }

    public String getProfileByLevel() {
        StringBuilder builder = new StringBuilder();

        summaryProfile.prettyPrint(builder);

        getExecutionProfileContent(builder);

        return builder.toString();
    }

    // If this Profile has already been stored to disk(by checking profileStoragePath),
    // then we will read the profile from disk.
    public void getExecutionProfileContent(StringBuilder builder) {
        if (builder == null) {
            builder = new StringBuilder();
        }

        if (profileHasBeenStoredToDisk()) {
            LOG.info("Profile of query {} has been stored to disk, reading it from disk",
                        this.summaryProfile.getProfileId());
            try {
                File executionFile = new File(profileStorageDir, EXECUTION_PROFILE_FILE_NAME);
                String executionTxt = FileUtils.readFileToString(executionFile, "UTF-8");
                builder.append(executionTxt);
            } catch (Exception e) {
                LOG.error("read execution profile failed, profile storage path {}", profileStorageDir, e);
                builder.append("read execution profile from" + profileStorageDir + " failed");
            }
            return;
        }

        // Only generate merged profile for select, insert into select.
        // Not support broker load now.
        if (this.profileLevel == MergedProfileLevel && this.executionProfiles.size() == 1) {
            try {
                builder.append("\n MergedProfile \n");
                this.executionProfiles.get(0).getAggregatedFragmentsProfile(planNodeMap).prettyPrint(builder, "     ");
            } catch (Throwable aggProfileException) {
                LOG.warn("build merged simple profile failed", aggProfileException);
                builder.append("build merged simple profile failed");
            }
        }
        try {
            for (ExecutionProfile executionProfile : executionProfiles) {
                builder.append("\n");
                executionProfile.getRoot().prettyPrint(builder, "");
            }
        } catch (Throwable aggProfileException) {
            LOG.warn("build profile failed", aggProfileException);
            builder.append("build  profile failed");
        }
    }

    private RuntimeProfile composeRootProfile() {

        RuntimeProfile rootProfile = new RuntimeProfile(id);
        rootProfile.setIsPipelineX(isPipelineX);
        rootProfile.addChild(summaryProfile.getSummary());
        rootProfile.addChild(summaryProfile.getExecutionSummary());
        for (ExecutionProfile executionProfile : executionProfiles) {
            rootProfile.addChild(executionProfile.getRoot());
        }
        rootProfile.computeTimeInProfile();
        return rootProfile;
    }

    public String getProfileBrief() {
        RuntimeProfile rootProfile = composeRootProfile();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(rootProfile.toBrief());
    }

    public void releaseExecutionProfile() {
        this.executionProfiles.clear();
    }

    public boolean shouldStoreToDisk() {
        if (profileHasBeenStoredToDisk()) {
            return false;
        }

        if (!isFinished) {
            return false;
        }

        // below is the case where query has finished

        boolean hasReportingProfile = false;

        for (ExecutionProfile executionProfile : executionProfiles) {
            if (!executionProfile.isCompleted()) {
                hasReportingProfile = true;
            }
        }

        if (!hasReportingProfile) {
            // query finished and no flying profile, most ideal case, we can store profile to disk.
            return true;
        }

        if (this.queryFinishTimestamp == Long.MIN_VALUE) {
            LOG.warn("Logical error,  query {} has finished, but queryFinishTimestamp is 0,", id);
            return false;
        }

        if (System.currentTimeMillis() - this.queryFinishTimestamp > 5000) {
            LOG.info("Profile of query {} should be stored to disk without waiting for incoming profile,"
                    + " since it has been waiting for {} ms, query finished time: {}",
                    id, System.currentTimeMillis() - this.queryFinishTimestamp, this.queryFinishTimestamp);            
            return true;
        }

        // query finished, wait a while for reporting profile
        return false;
    }

    public String getProfileStorageDir() {
        return this.profileStorageDir;
    }

    public synchronized boolean profileHasBeenStoredToDisk() {
        return !Strings.isNullOrEmpty(profileStorageDir);
    }

    // Profile IO threads races with Coordinator threads.
    public synchronized void markisFinished(long queryFinishTime) {
        try {
            if (this.profileHasBeenStoredToDisk()) {
                LOG.error("Logical error, profile {} has already been stored to disk", this.id);
                return;
            }
    
            this.executionProfiles.forEach(
                executionProfile -> {
                executionProfile.setQueryFinishTime(queryFinishTime);
                } 
            );

            this.isFinished = true;
            this.queryFinishTimestamp = System.currentTimeMillis();
        } catch (Throwable t) {
            LOG.warn("mark query finished failed", t);
            throw t;
        } finally {
            LOG.info("Mark query {} finished", this.id);
        }   
    }

    static public String[] parseProfileDirName(String profileDirName) {
        String [] idAndTimestamp = profileDirName.split(SEPERATOR);
        if (idAndTimestamp.length != 2) {
            return null;
        }
        TUniqueId thriftId = Util.parseTUniqueIdFromString(idAndTimestamp[0]);
        if (thriftId == null) {
            return null;
        }
        return idAndTimestamp;
    }

    public static Profile read(String path) {
        File profileDir = new File(path);
        if (!profileDir.exists()) {
            LOG.warn("profile directory {} does not exist", profileDir.getAbsolutePath());
            return null;
        }

        String profileFile = profileDir.getName();
        // Profile could be a load task with multiple queries, so we call it id.
        String [] idAndTimestamp = parseProfileDirName(profileFile);
        if (idAndTimestamp == null) {
            LOG.warn("profile directory {} is not a valid profile directory", profileDir.getAbsolutePath());
            return null;
        }

        LOG.info("Reading profile from {}", path);
        
        File profileMetaFile = new File(profileDir, PROFILE_META_FILE_NAME);
        String metaJson = "";

        try {
            metaJson = FileUtils.readFileToString(profileMetaFile, "UTF-8");

            if (Strings.isNullOrEmpty(metaJson)) {
                LOG.warn("meta profile is empty");
                return null;
            }
        } catch (Exception e) {
            LOG.error("read profile failed", e);
            return null;
        }

        Profile res = new Profile();
        byte[] byteArray = metaJson.getBytes();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        DataInputStream dataInput = new DataInputStream(byteArrayInputStream);

        try {
            res.summaryProfile = SummaryProfile.read(dataInput);
        } catch (Exception e) {
            LOG.error("read summary profile failed", e);
            return null;
        }

        res.setId(res.summaryProfile.getProfileId());
        res.profileStorageDir = path;
        res.isFinished = true;

        return res;
    }

    public void store(String systemProfileStorageDir) {
        if (Strings.isNullOrEmpty(id)) {
            LOG.warn("store profile failed, name is empty");
            return;
        }

        if (!Strings.isNullOrEmpty(profileStorageDir)) {
            LOG.error("Logical error, profile {} has already been stored to disk", id);
            return;
        }

        final String profileId = this.summaryProfile.getProfileId();
        // List directories, remove if name exists
        // profile dir name has format like: id_timestamp
        final long tmpProfileStoreTime = System.currentTimeMillis() / 1000;
        this.profileStoreTimestamp = tmpProfileStoreTime;

        // queryID_timestamp
        String tmpProfileStorageDir = systemProfileStorageDir +
                File.separator + profileId + SEPERATOR + String.valueOf(tmpProfileStoreTime);

        File profileDir = new File(tmpProfileStorageDir);
        if (profileDir.exists()) {
            LOG.warn("profile directory {} already exists, remove it", profileDir.getAbsolutePath());
            profileDir.delete();
        }

        // create query_id directory
        if (!profileDir.mkdir()) {
            LOG.warn("create profile directory {} failed", profileDir.getAbsolutePath());
            return;
        }

        // store mata of profile as json
        final String profileMetaFilePath = tmpProfileStorageDir + File.separator + PROFILE_META_FILE_NAME;

        try {
            FileOutputStream fileWriter = new FileOutputStream(profileMetaFilePath);
            DataOutputStream dataOutputStream = new DataOutputStream(fileWriter);
            this.summaryProfile.write(dataOutputStream);
            fileWriter.close();
        } catch (Exception e) {
            LOG.error("write {} summary profile failed", id, e);
            return;
        }
        
        // store execution profiles as string
        StringBuilder build = new StringBuilder();
        getExecutionProfileContent(build);
        String executionProfileString = build.toString();
    
        final String executionProfileFilePath = tmpProfileStorageDir + File.separator +
            EXECUTION_PROFILE_FILE_NAME;

        try {
            FileWriter executionProfileWriter = new FileWriter(executionProfileFilePath);
            executionProfileWriter.write(executionProfileString);
            executionProfileWriter.close();
        } catch (Exception e) {
            LOG.error("write {} execution profile failed", id, e);
            return;
        }

        this.profileStorageDir = tmpProfileStorageDir;
        LOG.info("Store profile for id: {}, path {}",  this.summaryProfile.getProfileId(), this.profileStorageDir);
    }

    public void remove() {
        if (Strings.isNullOrEmpty(getProfileStorageDir())) {
            LOG.warn("remove profile failed, profileStorageDir is empty");
            return;
        }

        File profileDir = new File(getProfileStorageDir());
        if (!profileDir.exists()) {
            LOG.warn("profile directory {} does not exist", profileDir.getAbsolutePath());
            return;
        }

        LOG.info("remove profile: {}", getProfileStorageDir());

        try {
            FileUtils.deleteDirectory(profileDir);
        } catch (Exception e) {
            LOG.error("remove profile failed", e);
        }
    }
}
