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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.planner.Planner;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

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
    // summaryProfile will be serialized to storage as JSON, and we can recover it from storage
    // recover of SummaryProfile is important, because it contains the meta information of the profile
    // we need it to construct memory index for profile retrieving.
    private SummaryProfile summaryProfile = new SummaryProfile();
    // executionProfiles will be stored to storage as text, when geting profile content, we will read
    // from storage directly.
    private List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    // profileStoragePath will only be assigned when:
    // 1. profile is stored to storage
    // 2. or profile is loaded from storage
    private String profileStoragePath = "";
    // isQueryFinished means the coordinator or stmtexecutor is finished.
    // does not mean the profile report has finished, since the report is async.
    // finish of collection of profile is marked by isCompleted of ExecutionProfiles.
    private boolean isQueryFinished = false;
    // when coordinator finishes, it will mark finish time.
    // we will wait for about 5 seconds to see if all profiles have been reported.
    // if not, we will store the profile to storage, and release the memory,
    // futher report will be ignored.
    // why MAX_VALUE? So that we can use PriorityQueue to sort profile by finish time decreasing order.
    private long queryFinishTimestamp = Long.MAX_VALUE;
    private Map<Integer, String> planNodeMap = Maps.newHashMap();
    private int profileLevel = MergedProfileLevel;
    private long autoProfileDurationMs = -1;
    // Profile size is the size of profile file
    private long profileSize = 0;

    private PhysicalPlan physicalPlan;
    public Map<String, Long> rowsProducedMap = new HashMap<>();
    private List<PhysicalRelation> physicalRelations = new ArrayList<>();

    // Need default constructor for read from storage
    public Profile() {}

    public Profile(boolean isEnable, int profileLevel, long autoProfileDurationMs) {
        this.summaryProfile = new SummaryProfile();
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isQueryFinished = !isEnable;
        this.profileLevel = profileLevel;
        this.autoProfileDurationMs = autoProfileDurationMs;
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

    // For normal profile, the profile id is a TUniqueId, but for broker load, the profile id is a long.
    public static String[] parseProfileFileName(String profileFileName) {
        String [] timeAndID = profileFileName.split(SEPERATOR);
        if (timeAndID.length != 2) {
            return null;
        }

        try {
            DebugUtil.parseTUniqueIdFromString(timeAndID[1]);
        } catch (NumberFormatException e) {
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
            File profileFile = new File(path);
            long fileSize = profileFile.length();
            // read method will move the cursor to the end of the summary profile
            DataInput dataInput = new DataInputStream(profileFileInputStream);
            Profile res = new Profile();
            res.summaryProfile = SummaryProfile.read(dataInput);
            res.setId(res.summaryProfile.getProfileId());
            res.profileStoragePath = path;
            res.isQueryFinished = true;
            res.profileSize = fileSize;
            String[] parts = path.split(File.separator);
            String queryFinishTimeStr = parseProfileFileName(parts[parts.length - 1])[0];
            // queryFinishTime is used for sorting profile by finish time.
            res.queryFinishTimestamp = Long.valueOf(queryFinishTimeStr);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read profile from storage: {}", res.summaryProfile.getProfileId());
            }
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

    // Method to compress a string using Deflater
    public static byte[] compressExecutionProfile(String str) throws IOException {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        deflater.end();
        outputStream.close();
        return outputStream.toByteArray();
    }

    // Method to decompress a byte array using Inflater
    public static String decompressExecutionProfile(byte[] data) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(data, 0, data.length);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            inflater.end();
        } catch (Exception e) {
            throw new IOException("Failed to decompress data", e);
        } finally {
            outputStream.close();
        }

        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }

    // For load task, the profile contains many execution profiles
    public void addExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            LOG.warn("try to set a null excecution profile, it is abnormal", new Exception());
            return;
        }
        executionProfile.setSummaryProfile(summaryProfile);
        this.executionProfiles.add(executionProfile);
    }

    public List<ExecutionProfile> getExecutionProfiles() {
        return this.executionProfiles;
    }

    // This API will also add the profile to ProfileManager, so that we could get the profile from ProfileManager.
    // isFinished ONLY means the coordinator or stmtexecutor is finished.
    public synchronized void updateSummary(Map<String, String> summaryInfo, boolean isFinished,
            Planner planner) {
        try {
            if (this.isQueryFinished) {
                return;
            }

            if (planner instanceof NereidsPlanner) {
                NereidsPlanner nereidsPlanner = ((NereidsPlanner) planner);
                physicalPlan = nereidsPlanner.getPhysicalPlan();
                physicalRelations.addAll(nereidsPlanner.getPhysicalRelations());
                FragmentIdMapping<DistributedPlan> distributedPlans = nereidsPlanner.getDistributedPlans();
                if (distributedPlans != null) {
                    summaryInfo.put(SummaryProfile.DISTRIBUTED_PLAN,
                            DistributedPlan.toString(Lists.newArrayList(distributedPlans.values()))
                                    .replace("\n", "\n     ")
                    );
                }
            }

            summaryProfile.update(summaryInfo);
            this.setId(summaryProfile.getProfileId());

            if (isFinished) {
                this.markQueryFinished(System.currentTimeMillis());
            }
            // Nerids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            ProfileManager.getInstance().pushProfile(this);
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
        // read execution profile from storage or generate it from memory (during query execution)
        getExecutionProfileContent(builder);

        return builder.toString();
    }

    // If the query is already finished, and user wants to get the profile, we should check
    // if BE has reported all profiles, if not, sleep 2s.
    private void waitProfileCompleteIfNeeded() {
        if (!this.isQueryFinished) {
            return;
        }
        boolean allCompleted = true;
        for (ExecutionProfile executionProfile : executionProfiles) {
            if (!executionProfile.isCompleted()) {
                allCompleted = false;
                break;
            }
        }
        if (!allCompleted) {
            try {
                Thread.currentThread().sleep(2000);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }

    private RuntimeProfile composeRootProfile() {
        RuntimeProfile rootProfile = new RuntimeProfile(id);
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

    // Read file if profile has been stored to storage.
    public void getExecutionProfileContent(StringBuilder builder) {
        if (builder == null) {
            builder = new StringBuilder();
        }

        if (profileHasBeenStored()) {
            LOG.info("Profile {} has been stored to storage, reading it from storage", id);

            FileInputStream fileInputStream = null;

            try {
                fileInputStream = createPorfileFileInputStream(profileStoragePath);
                if (fileInputStream == null) {
                    builder.append("Failed to read execution profile from " + profileStoragePath);
                    return;
                }

                DataInputStream dataInput = new DataInputStream(fileInputStream);
                // skip summary profile
                Text.readString(dataInput);
                // read compressed execution profile
                int binarySize = dataInput.readInt();
                byte[] binaryExecutionProfile = new byte[binarySize];
                dataInput.readFully(binaryExecutionProfile, 0, binarySize);
                // decompress binary execution profile
                String textExecutionProfile = decompressExecutionProfile(binaryExecutionProfile);
                builder.append(textExecutionProfile);
                return;
            } catch (Exception e) {
                LOG.error("An error occurred while reading execution profile from storage, profile storage path: {}",
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
        RuntimeProfile mergedProfile = null;
        if (this.profileLevel == MergedProfileLevel && this.executionProfiles.size() == 1) {
            try {
                mergedProfile = this.executionProfiles.get(0).getAggregatedFragmentsProfile(planNodeMap);
                this.rowsProducedMap.putAll(mergedProfile.rowsProducedMap);
                if (physicalPlan != null) {
                    updateActualRowCountOnPhysicalPlan(physicalPlan);
                }
            } catch (Throwable aggProfileException) {
                LOG.warn("build merged simple profile {} failed", this.id, aggProfileException);
            }
        }

        if (physicalPlan != null) {
            builder.append("\nPhysical Plan \n");
            StringBuilder physcialPlanBuilder = new StringBuilder();
            physcialPlanBuilder.append(physicalPlan.treeString());
            physcialPlanBuilder.append("\n");
            for (PhysicalRelation relation : physicalRelations) {
                if (relation.getStats() != null) {
                    physcialPlanBuilder.append(relation).append("\n")
                            .append(relation.getStats().printColumnStats());
                }
            }
            builder.append(
                    physcialPlanBuilder.toString().replace("\n", "\n     "));
        }

        if (this.profileLevel == MergedProfileLevel && this.executionProfiles.size() == 1) {
            builder.append("\nMergedProfile \n");
            if (mergedProfile != null) {
                mergedProfile.prettyPrint(builder, "     ");
            } else {
                builder.append("build merged simple profile failed");
            }
        }

        try {
            // For load task, they will have multiple execution_profiles.
            for (ExecutionProfile executionProfile : executionProfiles) {
                builder.append("\n");
                executionProfile.getRoot().prettyPrint(builder, "");
            }
        } catch (Throwable aggProfileException) {
            LOG.warn("build profile failed", aggProfileException);
            builder.append("build  profile failed");
        }
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

    public boolean shouldStoreToStorage() {
        if (profileHasBeenStored()) {
            return false;
        }

        if (!isQueryFinished) {
            return false;
        }

        // below is the case where query has finished
        boolean hasReportingProfile = false;

        if (this.executionProfiles.isEmpty()) {
            LOG.warn("Profile {} has no execution profile, it is abnormal", id);
            return false;
        }

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
            if (this.queryFinishTimestamp != Long.MAX_VALUE && durationMs
                    > (this.executionProfiles.size() * autoProfileDurationMs)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query/LoadJob {} costs {} ms, begin {} finish {}, need store its profile",
                            id, durationMs, summaryProfile.getQueryBeginTime(), this.queryFinishTimestamp);
                }
                return true;
            }
            return false;
        }

        if (this.queryFinishTimestamp == Long.MAX_VALUE) {
            LOG.warn("Logical error, query {} has finished, but queryFinishTimestamp is not set,", id);
            return false;
        }

        if (this.queryFinishTimestamp != Long.MAX_VALUE
                    && (System.currentTimeMillis() - this.queryFinishTimestamp) > autoProfileDurationMs) {
            LOG.warn("Profile {} should be stored to storage without waiting for incoming profile,"
                    + " since it has been waiting for {} ms, query finished time: {}", id,
                    System.currentTimeMillis() - this.queryFinishTimestamp, this.queryFinishTimestamp);

            this.summaryProfile.setSystemMessage(
                            "This profile is not complete, since its collection does not finish in time."
                            + " Maybe increase auto_profile_threshold_ms current val: "
                            + String.valueOf(autoProfileDurationMs));
            return true;
        }

        // query finished, wait a while for reporting profile
        return false;
    }

    public String getProfileStoragePath() {
        return this.profileStoragePath;
    }

    public boolean profileHasBeenStored() {
        return !Strings.isNullOrEmpty(profileStoragePath);
    }

    // Profile IO threads races with Coordinator threads.
    public void markQueryFinished(long queryFinishTime) {
        try {
            if (this.profileHasBeenStored()) {
                LOG.error("Logical error, profile {} has already been stored to storage", this.id);
                return;
            }

            this.isQueryFinished = true;
            this.queryFinishTimestamp = System.currentTimeMillis();
        } catch (Throwable t) {
            LOG.warn("mark query finished failed", t);
            throw t;
        }
    }

    public void writeToStorage(String systemProfileStorageDir) {
        if (Strings.isNullOrEmpty(id)) {
            LOG.warn("store profile failed, name is empty");
            return;
        }

        if (!Strings.isNullOrEmpty(profileStoragePath)) {
            LOG.error("Logical error, profile {} has already been stored to storage", id);
            return;
        }

        final String profileId = this.summaryProfile.getProfileId();

        // queryFinishTimeStamp_ProfileId
        final String profileFilePath = systemProfileStorageDir + File.separator
                                    + String.valueOf(this.queryFinishTimestamp)
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
         * Integer: m(size of compressed execution profile)
         * String: compressed binary of execution profile
        */
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(profileFilePath);
            DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
            this.summaryProfile.write(dataOutputStream);

            // store execution profiles as string
            StringBuilder build = new StringBuilder();
            getExecutionProfileContent(build);
            byte[] buf = compressExecutionProfile(build.toString());
            dataOutputStream.writeInt(buf.length);
            dataOutputStream.write(buf);
            build = null;
            dataOutputStream.flush();
            this.profileSize = profileFile.length();
        } catch (Exception e) {
            LOG.error("write {} summary profile failed", id, e);
            return;
        } finally {
            try {
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
            } catch (Exception e) {
                LOG.warn("close profile file {} failed", profileFilePath, e);
            }
        }

        this.profileStoragePath = profileFilePath;
    }

    // remove profile from storage
    public void deleteFromStorage() {
        if (!profileHasBeenStored()) {
            return;
        }

        String storagePath = getProfileStoragePath();
        if (Strings.isNullOrEmpty(storagePath)) {
            LOG.warn("remove profile failed, storage path is empty");
            return;
        }

        File profileFile = new File(storagePath);
        if (!profileFile.exists()) {
            LOG.warn("Profile {} does not exist", profileFile.getAbsolutePath());
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Remove profile: {}", getProfileStoragePath());
        }

        if (!FileUtils.deleteQuietly(profileFile)) {
            LOG.warn("remove profile {} failed", profileFile.getAbsolutePath());
        }
    }

    public long getProfileSize() {
        return this.profileSize;
    }

    public boolean shouldBeRemoveFromMemory() {
        if (!this.isQueryFinished) {
            return false;
        }

        if (this.profileHasBeenStored()) {
            return false;
        }

        if (this.queryFinishTimestamp - this.summaryProfile.getQueryBeginTime() >= autoProfileDurationMs) {
            return false;
        }

        return true;
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public void setPhysicalPlan(PhysicalPlan physicalPlan) {
        this.physicalPlan = physicalPlan;
    }

    private void updateActualRowCountOnPhysicalPlan(Plan plan) {
        if (plan == null || rowsProducedMap.isEmpty()) {
            return;
        }
        Long actualRowCount = rowsProducedMap.get(String.valueOf(((AbstractPlan) plan).getId()));
        if (actualRowCount != null) {
            ((AbstractPlan) plan).updateActualRowCount(actualRowCount);
        }
        for (Plan child : plan.children()) {
            updateActualRowCountOnPhysicalPlan(child);
        }
    }
}
