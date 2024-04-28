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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.Util;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
    // profile file name format: time_id
    private static final String SEPERATOR = "_";

    // id will be assgined to id of SummaryProfile.
    // For broker load, its SummaryPRofile id is a string representation of a long integer,
    // for others, it is queryID
    private String id = "";
    private boolean isPipelineX = true;
    // summaryProfile will be serialized to disk as JSON, and we can recover it from disk
    // recover of SummaryProfile is important, because it contains the meta information of the profile
    // we need it to construct memory index for profile retrieving.
    private SummaryProfile summaryProfile = new SummaryProfile();
    // executionProfiles will be stored to disk as string, when geting profile content, we will read
    // from disk directly.
    private List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    // profileStoragePath will only be assigned when:
    // 1. profile is stored to disk
    // 2. or profile is loaded from disk
    private String profileStoragePath = "";
    // isFinished means the coordinator or stmtexecutor is finished.
    // does not mean the profile report has finished, since the report is async.
    private boolean isFinished = false;
    // when coordinator finishes, it will mark finish time.
    // we will wait for about 5 seconds to see if all profiles have been reported.
    // if not, we will store the profile to disk, and release the memory,
    // futher report will be ignored.
    // why MIN_VALUE? So that we can use PriorityQueue to sort profile by finish time.
    private long queryFinishTimestamp = Long.MIN_VALUE;
    private long profileStoreTimestamp = Long.MIN_VALUE;
    private Map<Integer, String> planNodeMap = Maps.newHashMap();
    private int profileLevel = 3;
    private long autoProfileDurationMs = 500;

    // Need default constructor for read from disk
    public Profile() {}

    public Profile(boolean isEnable, int profileLevel, boolean isPipelineX, long autoProfileDurationMs) {
        this.isPipelineX = isPipelineX;
        this.summaryProfile = new SummaryProfile();
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isFinished = !isEnable;
        this.profileLevel = profileLevel;
        this.autoProfileDurationMs = autoProfileDurationMs;
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

    // This API will also add the profile to ProfileManager, so that we could get the profile from ProfileManager.
    // isFinished ONLY means the coordinator or stmtexecutor is finished.
    public synchronized void updateSummary(long startTime, Map<String, String> summaryInfo, boolean isFinished,
            Planner planner) {
        try {
            if (this.isFinished) {
                return;
            }
            summaryProfile.update(summaryInfo);
            this.setId(summaryProfile.getProfileId());

            for (ExecutionProfile executionProfile : executionProfiles) {
                // Tell execution profile the start time
                executionProfile.update(startTime, isFinished);
            }
            // Nerids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            // TODO: should not change isFinished here, in principle, the modification of isFinished should only
            // made by method markisFinished.
            // State change should be carefully designed, the absense of unified behaviour is
            // a potential risk of bug.
            this.isFinished = isFinished;
            if (isFinished) {
                this.queryFinishTimestamp = System.currentTimeMillis();
            }
        } catch (Throwable t) {
            LOG.warn("update profile {} failed", id, t);
            throw t;
        }
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }

    public String getProfileByLevel() {
        StringBuilder builder = new StringBuilder();
        // add summary to builder
        summaryProfile.prettyPrint(builder);
        // read execution profile from disk or generate it from memory (during query execution)
        getExecutionProfileContent(builder);

        return builder.toString();
    }

    // Read file if profile has been stored to disk.
    public void getExecutionProfileContent(StringBuilder builder) {
        if (builder == null) {
            builder = new StringBuilder();
        }

        if (profileHasBeenStoredToDisk()) {
            LOG.info("Profile {} has been stored to disk, reading it from disk", id);

            FileInputStream fileInputStream = null;

            try {
                fileInputStream = createPorfileFileInputStream(profileStoragePath);
                if (fileInputStream == null) {
                    builder.append("Failed to read execution profile from " + profileStoragePath);
                    return;
                }

                DataInput dataInput = new DataInputStream(fileInputStream);
                // skip summary profile
                Text.readString(dataInput);
                builder.append(Text.readString(dataInput));
                return;
            } catch (Exception e) {
                LOG.error("An error occurred while reading execution profile from disk, profile storage path: {}",
                        profileStoragePath, e);
                builder.append("Failed to read execution profile from " + profileStoragePath);
            } finally {
                if (fileInputStream != null) {
                    try {
                        fileInputStream.close();
                    } catch (Exception e) {
                        LOG.warn("Close profile {} failed", profileStoragePath, e);
                    }
                }
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

    // check if the profile file is valid and create a file input stream
    // user need to close the file stream.
    private static FileInputStream createPorfileFileInputStream(String path) {
        File profileFile = new File(path);
        if (!profileFile.isFile()) {
            LOG.warn("Profile storage path {} is invalid, its not a file.", profileFile.getAbsolutePath());
            return null;
        }

        String[] parts = path.split(File.separator);
        if (parts.length < 1) {
            LOG.warn("Profile storage path {} is invalid", profileFile.getAbsolutePath());
            return null;
        }
        // Profile could be a load task with multiple queries, so we call it id.
        if (parseProfileFileName(parts[parts.length - 1]) == null) {
            LOG.warn("{} is not a valid profile file", profileFile.getAbsolutePath());
            return null;
        }

        FileInputStream profileMetaFileInputStream = null;
        try {
            profileMetaFileInputStream = new FileInputStream(path);
        } catch (Exception e) {
            LOG.warn("open profile file {} failed", path, e);
        }

        return profileMetaFileInputStream;
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

    // For UT
    public void setSummaryProfile(SummaryProfile summaryProfile) {
        this.summaryProfile = summaryProfile;
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
                break;
            }
        }

        if (!hasReportingProfile) {
            // query finished and no flying profile
            // I do want to use TotalTime in summary profile, but it is an encoded string,
            // it is hard to write a parse function.
            long durationMs = this.queryFinishTimestamp - summaryProfile.getQueryBeginTime();
            // time cost of this query is large enough.
            if (durationMs > autoProfileDurationMs) {
                LOG.debug("Query/LoadJob {} costs {} ms, begin {} finish {}, need store its profile",
                        id, durationMs, summaryProfile.getQueryBeginTime(), this.queryFinishTimestamp);
                return true;
            }

            return false;
        }

        if (this.queryFinishTimestamp == Long.MIN_VALUE) {
            LOG.warn("Logical error, query {} has finished, but queryFinishTimestamp is 0,", id);
            return false;
        }

        if (this.queryFinishTimestamp != Long.MIN_VALUE
                    && System.currentTimeMillis() - this.queryFinishTimestamp > 5000) {
            LOG.info("Profile {} should be stored to disk without waiting for incoming profile,"
                    + " since it has been waiting for {} ms, query finished time: {}",
                    id, System.currentTimeMillis() - this.queryFinishTimestamp, this.queryFinishTimestamp);
            return true;
        }

        // query finished, wait a while for reporting profile
        return false;
    }

    public String getProfileStoragePath() {
        return this.profileStoragePath;
    }

    public boolean profileHasBeenStoredToDisk() {
        return !Strings.isNullOrEmpty(profileStoragePath);
    }

    // Profile IO threads races with Coordinator threads.
    public void markisFinished(long queryFinishTime) {
        try {
            if (this.profileHasBeenStoredToDisk()) {
                LOG.error("Logical error, profile {} has already been stored to disk", this.id);
                return;
            }

            this.executionProfiles.forEach(
                    executionProfile -> {
                        executionProfile.setQueryFinishTime(queryFinishTime);
                    });

            this.isFinished = true;
            this.queryFinishTimestamp = System.currentTimeMillis();
        } catch (Throwable t) {
            LOG.warn("mark query finished failed", t);
            throw t;
        }
    }

    // For normal profile, the profile id is a TUniqueId, but for broker load, the profile id is a long.
    public static String[] parseProfileFileName(String profileFileName) {
        String [] timeAndID = profileFileName.split(SEPERATOR);
        if (timeAndID.length != 2) {
            return null;
        }
        TUniqueId thriftId = Util.parseTUniqueIdFromString(timeAndID[1]);
        if (thriftId == null) {
            if (Long.valueOf(timeAndID[1]) == null) {
                return null;
            }
        }
        return timeAndID;
    }

    // read method will only read summary profile, and return a Profile object
    public static Profile read(String path) {
        FileInputStream profileFileInputStream = null;
        try {
            profileFileInputStream = createPorfileFileInputStream(path);
            // Maybe profile path is invalid
            if (profileFileInputStream == null) {
                return null;
            }
            // read method will move the cursor to the end of the summary profile
            DataInput dataInput = new DataInputStream(profileFileInputStream);

            Profile res = new Profile();
            res.summaryProfile = SummaryProfile.read(dataInput);
            res.setId(res.summaryProfile.getProfileId());
            res.profileStoragePath = path;
            res.isFinished = true;
            LOG.debug("Read profile from disk: {}", res.summaryProfile.getProfileId());
            return res;
        } catch (Exception exception) {
            LOG.error("read profile failed", exception);
            return null;
        } finally {
            if (profileFileInputStream != null) {
                try {
                    profileFileInputStream.close();
                } catch (Exception e) {
                    LOG.warn("close profile file {} failed", path, e);
                }
            }
        }
    }

    public void store(String systemProfileStorageDir) {
        if (Strings.isNullOrEmpty(id)) {
            LOG.warn("store profile failed, name is empty");
            return;
        }

        if (!Strings.isNullOrEmpty(profileStoragePath)) {
            LOG.error("Logical error, profile {} has already been stored to disk", id);
            return;
        }

        final String profileId = this.summaryProfile.getProfileId();
        final long tmpProfileStoreTime = System.currentTimeMillis();
        this.profileStoreTimestamp = tmpProfileStoreTime;

        // time_id
        final String profileFilePath = systemProfileStorageDir + File.separator + String.valueOf(tmpProfileStoreTime)
                                    + SEPERATOR + profileId;

        File profileFile = new File(profileFilePath);
        if (profileFile.exists()) {
            LOG.warn("profile directory {} already exists, remove it", profileFile.getAbsolutePath());
            profileFile.delete();
        }

        // File structure of profile:
        /*
         * Integer: n(size of summary profile)
         * String: json of summary profile
         * Integer: m(size of execution profile)
         * String: raw text of execution profile
        */
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(profileFilePath);
            DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
            this.summaryProfile.write(dataOutputStream);

            // store execution profiles as string
            StringBuilder build = new StringBuilder();
            getExecutionProfileContent(build);
            String executionProfileString = build.toString();
            build = null;
            Text.writeString(dataOutputStream, executionProfileString);
        } catch (Exception e) {
            LOG.error("write {} summary profile failed", id, e);
            return;
        } finally {
            try {
                fileOutputStream.close();
            } catch (Exception e) {
                LOG.warn("close profile file {} failed", profileFilePath, e);
            }
        }

        this.profileStoragePath = profileFilePath;
        LOG.debug("Store profile: {}, path {}", id, this.profileStoragePath);
    }

    public void remove() {
        if (!profileHasBeenStoredToDisk()) {
            return;
        }

        File profileFile = new File(getProfileStoragePath());
        if (!profileFile.exists()) {
            LOG.warn("Profile {} does not exist", profileFile.getAbsolutePath());
            return;
        }

        LOG.debug("Remove profile: {}", getProfileStoragePath());

        if (!FileUtils.deleteQuietly(profileFile)) {
            LOG.warn("remove profile {} failed", profileFile.getAbsolutePath());
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
}
