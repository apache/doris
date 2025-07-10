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

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

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
    private static final String PROFILE_ENTRY_SUFFIX = ".profile";

    // summaryProfile will be serialized to storage as JSON, and we can recover it from storage
    // recover of SummaryProfile is important, because it contains the meta information of the profile
    // we need it to construct memory index for profile retrieving.
    private SummaryProfile summaryProfile = new SummaryProfile();
    // executionProfiles will be stored to storage as text, when getting profile content, we will read
    // from storage directly.
    List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    // profileStoragePath will only be assigned when:
    // 1. profile is stored to storage
    // 2. or profile is loaded from storage
    private String profileStoragePath = "";
    // isQueryFinished means the coordinator or stmt executor is finished.
    // does not mean the profile report has finished, since the report is async.
    // finish of collection of profile is marked by isCompleted of ExecutionProfiles.
    boolean isQueryFinished = false;
    // when coordinator finishes, it will mark finish time.
    // we will wait for about 5 seconds to see if all profiles have been reported.
    // if not, we will store the profile to storage, and release the memory,
    // further report will be ignored.
    // why MAX_VALUE? So that we can use PriorityQueue to sort profile by finish time decreasing order.
    private long queryFinishTimestamp = Long.MAX_VALUE;
    private Map<Integer, String> planNodeMap = Maps.newHashMap();
    private int profileLevel = MergedProfileLevel;
    protected long autoProfileDurationMs = -1;
    // Profile size is the size of profile file
    private long profileSize = 0;

    private PhysicalPlan physicalPlan;
    public Map<String, Long> rowsProducedMap = new HashMap<>();
    private Set<PhysicalRelation> physicalRelations = new LinkedHashSet<>();

    private String changedSessionVarCache = "";

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
    static FileInputStream createPorfileFileInputStream(String path) {
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
            LOG.warn("Open profile file {} failed", path, e);
        }

        return profileMetaFileInputStream;
    }

    // For normal profile, the profile id is a TUniqueId, but for broker load, the profile id is a long.
    public static String[] parseProfileFileName(String profileFileName) {
        if (!profileFileName.endsWith(".zip")) {
            LOG.warn("Invalid profile name {}", profileFileName);
            return null;
        } else {
            profileFileName = profileFileName.substring(0, profileFileName.length() - 4);
        }

        String [] timeAndID = profileFileName.split(SEPERATOR);
        if (timeAndID.length != 2) {
            return null;
        }

        try {
            DebugUtil.parseTUniqueIdFromString(timeAndID[1]);
        } catch (NumberFormatException e) {
            try {
                Long.parseLong(timeAndID[1]);
            } catch (NumberFormatException e2) {
                return null;
            }
        }

        return timeAndID;
    }


    public static Profile read(String path) {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(path);
            File profileFile = new File(path);
            long fileSize = profileFile.length();

            ZipInputStream zipIn = new ZipInputStream(fileInputStream);
            ZipEntry entry = zipIn.getNextEntry();
            if (entry == null) {
                LOG.error("Invalid zip profile file, {}", path);
                return null;
            }

            // Read zip entry content into memory
            ByteArrayOutputStream entryContent = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024 * 50];
            int readBytes;
            while ((readBytes = zipIn.read(buffer)) != -1) {
                entryContent.write(buffer, 0, readBytes);
            }

            // Parse profile data using memory stream
            DataInputStream memoryDataInput = new DataInputStream(
                    new ByteArrayInputStream(entryContent.toByteArray()));

            Profile res = new Profile();
            res.summaryProfile = SummaryProfile.read(memoryDataInput);
            res.profileStoragePath = path;
            res.isQueryFinished = true;
            res.profileSize = fileSize;

            String[] parts = path.split(File.separator);
            String filename = parts[parts.length - 1];
            String queryFinishTimeStr = parseProfileFileName(filename)[0];
            res.queryFinishTimestamp = Long.valueOf(queryFinishTimeStr);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Read profile from storage: {}", res.summaryProfile.getProfileId());
            }
            return res;

        } catch (Exception exception) {
            LOG.error("read profile failed", exception);
            return null;
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (Exception e) {
                    LOG.warn("close profile file {} failed", path, e);
                }
            }
        }
    }

    // For load task, the profile contains many execution profiles
    public void addExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            LOG.warn("try to set a null execution profile, it is abnormal", new Exception());
            return;
        }
        executionProfile.setSummaryProfile(summaryProfile);
        this.executionProfiles.add(executionProfile);
    }

    public List<ExecutionProfile> getExecutionProfiles() {
        return this.executionProfiles;
    }

    // This API will also add the profile to ProfileManager, so that we could get the profile from ProfileManager.
    // isFinished ONLY means the coordinator or stmt executor is finished.
    public synchronized void updateSummary(Map<String, String> summaryInfo, boolean isFinished,
            Planner planner) {
        try {
            if (this.isQueryFinished) {
                return;
            }

            if (planner != null && planner instanceof NereidsPlanner) {
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

            if (isFinished) {
                this.markQueryFinished();
                long durationMs = this.queryFinishTimestamp - summaryProfile.getQueryBeginTime();
                // Duration ls less than autoProfileDuration, remove it from memory.
                long durationThreshold = executionProfiles.isEmpty()
                                    ? autoProfileDurationMs : executionProfiles.size() * autoProfileDurationMs;
                if (this.queryFinishTimestamp != Long.MAX_VALUE && durationMs < durationThreshold) {
                    ProfileManager.getInstance().removeProfile(this.getId());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Removed profile {} because it's costs {} is less than {}", this.getId(),
                                durationMs, autoProfileDurationMs * this.executionProfiles.size());
                    }
                    return;
                }
            }

            // Nereids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            ProfileManager.getInstance().pushProfile(this);
        } catch (Throwable t) {
            LOG.warn("update profile {} failed", getId(), t);
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
        getChangedSessionVars(builder);
        getExecutionProfileContent(builder);
        getOnStorageProfile(builder);

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
        RuntimeProfile rootProfile = new RuntimeProfile(getId());
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

    // Return if profile has been stored to storage
    public void getExecutionProfileContent(StringBuilder builder) {
        if (builder == null) {
            builder = new StringBuilder();
        }

        if (profileHasBeenStored()) {
            return;
        }

        // For broker load, if it has more than one execution profile, we will not generate merged profile.
        RuntimeProfile mergedProfile = null;
        if (this.profileLevel == MergedProfileLevel && this.executionProfiles.size() == 1) {
            try {
                mergedProfile = this.executionProfiles.get(0).getAggregatedFragmentsProfile(planNodeMap);
                this.rowsProducedMap.putAll(mergedProfile.rowsProducedMap);
                if (physicalPlan != null) {
                    updateActualRowCountOnPhysicalPlan(physicalPlan);
                }
            } catch (Throwable aggProfileException) {
                LOG.warn("build merged simple profile {} failed", getId(), aggProfileException);
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

    // For UT
    public void setSummaryProfile(SummaryProfile summaryProfile) {
        this.summaryProfile = summaryProfile;
    }

    public void releaseMemory() {
        this.executionProfiles.clear();
        this.changedSessionVarCache = "";
    }

    public boolean shouldStoreToStorage() {
        if (profileHasBeenStored()) {
            return false;
        }

        if (!isQueryFinished) {
            return false;
        }

        if (this.queryFinishTimestamp == Long.MAX_VALUE) {
            LOG.warn("Logical error, query {} has finished, but queryFinishTimestamp is not set,", getId());
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
            return true;
        } else {
            long currentTimeMillis = System.currentTimeMillis();
            if (this.queryFinishTimestamp != Long.MAX_VALUE
                    && (currentTimeMillis - this.queryFinishTimestamp)
                    > Config.profile_waiting_time_for_spill_seconds * 1000) {
                LOG.warn("Profile {} should be stored to storage without waiting for incoming profile,"
                        + " since it has been waiting for {} ms, current time {} query finished time: {}",
                        getId(), currentTimeMillis - this.queryFinishTimestamp, currentTimeMillis,
                        this.queryFinishTimestamp);

                this.summaryProfile.setSystemMessage(
                                "This profile is not complete, since its collection does not finish in time."
                                + " Maybe increase profile_waiting_time_for_spill_secs in fe.conf current val: "
                                + String.valueOf(Config.profile_waiting_time_for_spill_seconds));
                return true;
            }
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
    public void markQueryFinished() {
        try {
            if (this.profileHasBeenStored()) {
                LOG.error("Logical error, profile {} has already been stored to storage", getId());
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
        if (Strings.isNullOrEmpty(getId())) {
            LOG.warn("store profile failed, name is empty");
            return;
        }

        if (!Strings.isNullOrEmpty(profileStoragePath)) {
            LOG.error("Logical error, profile {} has already been stored to storage", getId());
            return;
        }

        final String profileId = this.summaryProfile.getProfileId();
        final String profileFilePath = systemProfileStorageDir + File.separator
                                    + String.valueOf(this.queryFinishTimestamp)
                                    + SEPERATOR + profileId + ".zip";

        File profileFile = new File(profileFilePath);
        if (profileFile.exists()) {
            LOG.warn("profile directory {} already exists, remove it", profileFile.getAbsolutePath());
            profileFile.delete();
        }

        FileOutputStream fileOutputStream = null;
        ZipOutputStream zipOut = null;
        try {
            fileOutputStream = new FileOutputStream(profileFilePath);
            zipOut = new ZipOutputStream(fileOutputStream);

            // First create memory stream to hold all data
            ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
            DataOutputStream memoryDataStream = new DataOutputStream(memoryStream);

            // Write summary profile and execution profile content to memory
            this.summaryProfile.write(memoryDataStream);

            StringBuilder builder = new StringBuilder();
            getChangedSessionVars(builder);
            getExecutionProfileContent(builder);
            byte[] executionProfileBytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            memoryDataStream.writeInt(executionProfileBytes.length);
            memoryDataStream.write(executionProfileBytes);
            memoryDataStream.flush();

            // Create zip entry with profileId based name
            ZipEntry zipEntry = new ZipEntry(profileId + PROFILE_ENTRY_SUFFIX);
            zipOut.putNextEntry(zipEntry);
            zipOut.write(memoryStream.toByteArray());
            zipOut.closeEntry();

            this.profileSize = profileFile.length();
            this.profileStoragePath = profileFilePath;

        } catch (Exception e) {
            LOG.error("write {} summary profile failed", getId(), e);
            return;
        } finally {
            try {
                if (zipOut != null) {
                    zipOut.close();
                }
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
            } catch (Exception e) {
                LOG.warn("close profile file {} failed", profileFilePath, e);
            }
        }
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

    public void setChangedSessionVar(String changedSessionVar) {
        this.changedSessionVarCache = changedSessionVar;
    }

    private void getChangedSessionVars(StringBuilder builder) {
        if (builder == null) {
            builder = new StringBuilder();
        }
        if (profileHasBeenStored()) {
            return;
        }

        builder.append("\nChanged Session Variables:\n");
        builder.append(changedSessionVarCache);
        builder.append("\n");
    }

    void getOnStorageProfile(StringBuilder builder) {
        if (!profileHasBeenStored()) {
            return;
        }

        LOG.info("Profile {} has been stored to storage, reading it from storage", getId());
        FileInputStream fileInputStream = null;
        ZipInputStream zipIn = null;

        try {
            fileInputStream = createPorfileFileInputStream(profileStoragePath);
            if (fileInputStream == null) {
                builder.append("Failed to read profile from " + profileStoragePath);
                return;
            }

            // Directly create ZipInputStream from file input stream
            zipIn = new ZipInputStream(fileInputStream);
            ZipEntry entry = zipIn.getNextEntry();
            String expectedEntryName = summaryProfile.getProfileId() + PROFILE_ENTRY_SUFFIX;
            if (entry == null || !entry.getName().equals(expectedEntryName)) {
                throw new IOException("Invalid zip file format - missing entry: " + expectedEntryName);
            }

            // Read zip entry content into memory
            ByteArrayOutputStream entryContent = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024 * 50];
            int readBytes;
            while ((readBytes = zipIn.read(buffer)) != -1) {
                entryContent.write(buffer, 0, readBytes);
            }

            // Parse profile data using memory stream
            DataInputStream memoryDataInput = new DataInputStream(
                    new ByteArrayInputStream(entryContent.toByteArray()));

            // Skip summary profile data
            Text.readString(memoryDataInput);

            // Read execution profile length and content
            int executionProfileLength = memoryDataInput.readInt();
            byte[] executionProfileBytes = new byte[executionProfileLength];
            memoryDataInput.readFully(executionProfileBytes);

            // Append execution profile content
            builder.append(new String(executionProfileBytes, StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.error("Failed to read profile from storage: {}", profileStoragePath, e);
            builder.append("Failed to read profile from " + profileStoragePath);
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (Exception e) {
                    LOG.warn("Close profile {} failed", profileStoragePath, e);
                }
            }
        }
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

    public String debugInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProfileId:").append(getId()).append("|");
        builder.append("StoragePath:").append(profileStoragePath).append("|");
        builder.append("StartTimeStamp:").append(summaryProfile.getQueryBeginTime()).append("|");
        builder.append("IsFinished:").append(isQueryFinished).append("|");
        builder.append("FinishTimestamp:").append(queryFinishTimestamp).append("|");
        builder.append("AutoProfileDuration: ").append(autoProfileDurationMs).append("|");
        builder.append("ExecutionProfileCnt: ").append(executionProfiles.size()).append("|");
        builder.append("ProfileOnStorageSize:").append(profileSize);
        return builder.toString();
    }

    public void setQueryFinishTimestamp(long l) {
        this.queryFinishTimestamp = l;
    }

    public String getId() {
        return summaryProfile.getProfileId();
    }
}
