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

package org.apache.doris.policy;

import org.apache.doris.analysis.AlterPolicyStmt;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.ShowPolicyStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Management policy and cache it.
 **/
public class PolicyMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(PolicyMgr.class);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "typeToPolicyMap")
    private Map<PolicyTypeEnum, List<Policy>> typeToPolicyMap = Maps.newConcurrentMap();

    /**
     * Cache merge policy for match.
     * key：dbId:tableId-type-user
     **/
    private Map<Long, Map<String, RowPolicy>> dbIdToMergeTablePolicyMap = Maps.newConcurrentMap();

    private Set<String> userPolicySet = Sets.newConcurrentHashSet();

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    /**
     * Create default storage policy used by master.
     **/
    public void createDefaultStoragePolicy() {
        writeLock();
        try {
            Optional<Policy> hasDefault = findPolicy(StoragePolicy.DEFAULT_STORAGE_POLICY_NAME, PolicyTypeEnum.STORAGE);
            if (hasDefault.isPresent()) {
                // already exist default storage policy, just return.
                return;
            }
            long policyId = Env.getCurrentEnv().getNextId();
            StoragePolicy defaultStoragePolicy = new StoragePolicy(policyId, StoragePolicy.DEFAULT_STORAGE_POLICY_NAME);
            unprotectedAdd(defaultStoragePolicy);
            Env.getCurrentEnv().getEditLog().logCreatePolicy(defaultStoragePolicy);
        } finally {
            writeUnlock();
        }
        LOG.info("Create default storage success.");
    }

    /**
     * Create policy through stmt.
     **/
    public void createPolicy(CreatePolicyStmt stmt) throws UserException {
        Policy policy = Policy.fromCreateStmt(stmt);
        writeLock();
        try {
            if (existPolicy(policy)) {
                if (stmt.isIfNotExists()) {
                    return;
                }
                throw new DdlException("the policy " + policy.getPolicyName() + " already create");
            }
            unprotectedAdd(policy);
            Env.getCurrentEnv().getEditLog().logCreatePolicy(policy);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Drop policy through stmt.
     **/
    public void dropPolicy(DropPolicyStmt stmt) throws DdlException, AnalysisException {
        DropPolicyLog dropPolicyLog = DropPolicyLog.fromDropStmt(stmt);
        writeLock();
        try {
            if (!existPolicy(dropPolicyLog)) {
                if (stmt.isIfExists()) {
                    return;
                }
                throw new DdlException("the policy " + dropPolicyLog.getPolicyName() + " not exist");
            }
            unprotectedDrop(dropPolicyLog);
            Env.getCurrentEnv().getEditLog().logDropPolicy(dropPolicyLog);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Check whether this user has policy.
     *
     * @param user user who has policy
     * @return exist or not
     */
    public boolean existPolicy(String user) {
        return userPolicySet.contains(user);
    }

    /**
     * Check whether the policy exist.
     *
     * @param checkedPolicy policy condition to check
     * @return exist or not
     */
    public boolean existPolicy(Policy checkedPolicy) {
        readLock();
        try {
            List<Policy> policies = getPoliciesByType(checkedPolicy.getType());
            return policies.stream().anyMatch(policy -> policy.matchPolicy(checkedPolicy));
        } finally {
            readUnlock();
        }
    }

    /**
     * CCheck whether the policy exist for the DropPolicyLog.
     *
     * @param checkedDropPolicy policy log condition to check
     * @return exist or not
     */
    private boolean existPolicy(DropPolicyLog checkedDropPolicy) {
        readLock();
        try {
            List<Policy> policies = getPoliciesByType(checkedDropPolicy.getType());
            return policies.stream().anyMatch(policy -> policy.matchPolicy(checkedDropPolicy));
        } finally {
            readUnlock();
        }
    }

    /**
     * Get policy by type and name.
     *
     * @param checkedPolicy condition to get policy
     * @return Policy in typeToPolicyMap
     */
    public Policy getPolicy(Policy checkedPolicy) {
        readLock();
        try {
            List<Policy> policies = getPoliciesByType(checkedPolicy.getType());
            for (Policy policy : policies) {
                if (policy.matchPolicy(checkedPolicy)) {
                    return policy;
                }
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    public List<Policy> getCopiedPoliciesByType(PolicyTypeEnum policyType) {
        readLock();
        try {
            return ImmutableList.copyOf(getPoliciesByType(policyType));
        } finally {
            readUnlock();
        }
    }

    private List<Policy> getPoliciesByType(PolicyTypeEnum policyType) {
        if (typeToPolicyMap == null) {
            return new ArrayList<>();
        }
        return typeToPolicyMap.getOrDefault(policyType, new ArrayList<>());
    }

    public void replayCreate(Policy policy) {
        unprotectedAdd(policy);
        LOG.info("replay create policy: {}", policy);
    }

    private void unprotectedAdd(Policy policy) {
        if (policy == null) {
            return;
        }
        List<Policy> dbPolicies = getPoliciesByType(policy.getType());
        dbPolicies.add(policy);
        typeToPolicyMap.put(policy.getType(), dbPolicies);
        updateMergeTablePolicyMap();
    }

    public void replayDrop(DropPolicyLog log) {
        unprotectedDrop(log);
        LOG.info("replay drop policy log: {}", log);
    }

    public void replayStoragePolicyAlter(StoragePolicy log) {
        List<Policy> policies = getPoliciesByType(log.getType());
        policies.removeIf(policy -> log.getPolicyName().equals(policy.getPolicyName()));
        policies.add(log);
        typeToPolicyMap.put(log.getType(), policies);
        LOG.info("replay alter policy log: {}", log);
    }

    private void unprotectedDrop(DropPolicyLog log) {
        List<Policy> policies = getPoliciesByType(log.getType());
        policies.removeIf(policy -> policy.matchPolicy(log));
        typeToPolicyMap.put(log.getType(), policies);
        updateMergeTablePolicyMap();
    }

    /**
     * Match row policy and return it.
     **/
    public RowPolicy getMatchTablePolicy(long dbId, long tableId, String user) {
        readLock();
        try {
            if (!dbIdToMergeTablePolicyMap.containsKey(dbId)) {
                return null;
            }
            String key = Joiner.on("-").join(tableId, PolicyTypeEnum.ROW.name(), user);
            if (!dbIdToMergeTablePolicyMap.get(dbId).containsKey(key)) {
                return null;
            }
            return dbIdToMergeTablePolicyMap.get(dbId).get(key);
        } finally {
            readUnlock();
        }
    }

    /**
     *  Match all row policy and return them.
     **/
    public List<RowPolicy> getMatchRowPolicy(long dbId, long tableId, UserIdentity user) {
        RowPolicy checkedPolicy = new RowPolicy();
        checkedPolicy.setDbId(dbId);
        checkedPolicy.setTableId(tableId);
        checkedPolicy.setUser(user);
        readLock();
        try {
            return getPoliciesByType(PolicyTypeEnum.ROW).stream()
                .filter(p -> p.matchPolicy(checkedPolicy))
                .filter(p -> !p.isInvalid())
                .map(p -> (RowPolicy) p)
                .collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    /**
     * Show policy through stmt.
     **/
    public ShowResultSet showPolicy(ShowPolicyStmt showStmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        long currentDbId = ConnectContext.get().getCurrentDbId();
        Policy checkedPolicy = null;
        switch (showStmt.getType()) {
            case STORAGE:
                checkedPolicy = new StoragePolicy();
                break;
            case ROW:
            default:
                RowPolicy rowPolicy = new RowPolicy();
                if (showStmt.getUser() != null) {
                    rowPolicy.setUser(showStmt.getUser());
                }
                if (currentDbId != -1) {
                    rowPolicy.setDbId(currentDbId);
                }
                checkedPolicy = rowPolicy;
        }
        final Policy finalCheckedPolicy = checkedPolicy;
        readLock();
        try {
            List<Policy> policies = getPoliciesByType(showStmt.getType()).stream()
                    .filter(p -> p.matchPolicy(finalCheckedPolicy)).collect(Collectors.toList());
            for (Policy policy : policies) {
                if (policy.isInvalid()) {
                    continue;
                }

                if (policy instanceof StoragePolicy && ((StoragePolicy) policy).getStorageResource() == null) {
                    // default storage policy not init.
                    continue;
                }

                rows.add(policy.getShowInfo());
            }
            return new ShowResultSet(showStmt.getMetaData(), rows);
        } finally {
            readUnlock();
        }
    }

    /**
     * The merge policy cache needs to be regenerated after the update.
     **/
    private void updateMergeTablePolicyMap() {
        readLock();
        try {
            if (!typeToPolicyMap.containsKey(PolicyTypeEnum.ROW)) {
                return;
            }
            List<Policy> allPolicies = typeToPolicyMap.get(PolicyTypeEnum.ROW);
            Map<Long, List<RowPolicy>> policyMap = new HashMap<>();
            dbIdToMergeTablePolicyMap.clear();
            userPolicySet.clear();
            for (Policy policy : allPolicies) {
                if (!(policy instanceof RowPolicy)) {
                    continue;
                }
                RowPolicy rowPolicy = (RowPolicy) policy;
                if (!policyMap.containsKey(rowPolicy.getDbId())) {
                    policyMap.put(rowPolicy.getDbId(), new ArrayList<>());
                }
                policyMap.get(rowPolicy.getDbId()).add(rowPolicy);
                if (rowPolicy.getUser() != null) {
                    userPolicySet.add(rowPolicy.getUser().getQualifiedUser());
                }
            }
            for (Map.Entry<Long, List<RowPolicy>> entry : policyMap.entrySet()) {
                List<RowPolicy> policies = entry.getValue();
                Map<String, RowPolicy> andMap = new HashMap<>();
                Map<String, RowPolicy> orMap = new HashMap<>();
                for (RowPolicy rowPolicy : policies) {
                    // read from json, need set isAnalyzed
                    rowPolicy.getUser().setIsAnalyzed();
                    String key = Joiner.on("-")
                            .join(rowPolicy.getTableId(), rowPolicy.getType(), rowPolicy.getUser().getQualifiedUser());
                    // merge wherePredicate
                    if (CompoundPredicate.Operator.AND.equals(rowPolicy.getFilterType().getOp())) {
                        RowPolicy frontPolicy = andMap.get(key);
                        if (frontPolicy == null) {
                            andMap.put(key, rowPolicy.clone());
                        } else {
                            frontPolicy.setWherePredicate(new CompoundPredicate(CompoundPredicate.Operator.AND,
                                    frontPolicy.getWherePredicate(), rowPolicy.getWherePredicate()));
                            andMap.put(key, frontPolicy.clone());
                        }
                    } else {
                        RowPolicy frontPolicy = orMap.get(key);
                        if (frontPolicy == null) {
                            orMap.put(key, rowPolicy.clone());
                        } else {
                            frontPolicy.setWherePredicate(new CompoundPredicate(CompoundPredicate.Operator.OR,
                                    frontPolicy.getWherePredicate(), rowPolicy.getWherePredicate()));
                            orMap.put(key, frontPolicy.clone());
                        }
                    }
                }
                Map<String, RowPolicy> mergeMap = new HashMap<>();
                Set<String> policyKeys = new HashSet<>();
                policyKeys.addAll(andMap.keySet());
                policyKeys.addAll(orMap.keySet());
                policyKeys.forEach(key -> {
                    if (andMap.containsKey(key) && orMap.containsKey(key)) {
                        RowPolicy mergePolicy = andMap.get(key).clone();
                        mergePolicy.setWherePredicate(
                                new CompoundPredicate(CompoundPredicate.Operator.AND, mergePolicy.getWherePredicate(),
                                        orMap.get(key).getWherePredicate()));
                        mergeMap.put(key, mergePolicy);
                    }
                    if (!andMap.containsKey(key)) {
                        mergeMap.put(key, orMap.get(key));
                    }
                    if (!orMap.containsKey(key)) {
                        mergeMap.put(key, andMap.get(key));
                    }
                });
                long dbId = entry.getKey();
                dbIdToMergeTablePolicyMap.put(dbId, mergeMap);
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * Read policyMgr from file.
     **/
    public static PolicyMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        PolicyMgr policyMgr = GsonUtils.GSON.fromJson(json, PolicyMgr.class);
        // update merge policy cache and userPolicySet
        policyMgr.updateMergeTablePolicyMap();
        return policyMgr;
    }

    /**
     * Find policy by policy name and type
     **/
    public Optional<Policy> findPolicy(final String policyName, PolicyTypeEnum policyType) {
        readLock();
        try {
            List<Policy> policiesByType = getPoliciesByType(policyType);
            return policiesByType.stream().filter(policy -> policy.getPolicyName().equals(policyName)).findAny();
        } finally {
            readUnlock();
        }
    }

    /**
     * Alter policy by stmt.
     **/
    public void alterPolicy(AlterPolicyStmt stmt) throws DdlException, AnalysisException {
        String storagePolicyName = stmt.getPolicyName();
        Map<String, String> properties = stmt.getProperties();

        if (findPolicy(storagePolicyName, PolicyTypeEnum.ROW).isPresent()) {
            throw new DdlException("Current not support alter row policy");
        }

        Optional<Policy> policy = findPolicy(storagePolicyName, PolicyTypeEnum.STORAGE);
        StoragePolicy storagePolicy = (StoragePolicy) policy.orElseThrow(
                () -> new DdlException("Storage policy(" + storagePolicyName + ") dose not exist."));
        storagePolicy.modifyProperties(properties);

        // log alter
        Env.getCurrentEnv().getEditLog().logAlterStoragePolicy(storagePolicy);
        LOG.info("Alter storage policy success. policy: {}", storagePolicy);
    }

    /**
     * Check storage policy whether exist by policy name.
     **/
    public void checkStoragePolicyExist(String storagePolicyName) throws DdlException {
        if (Strings.isNullOrEmpty(storagePolicyName)) {
            return;
        }
        readLock();
        try {
            List<Policy> policiesByType = Env.getCurrentEnv().getPolicyMgr().getPoliciesByType(PolicyTypeEnum.STORAGE);
            policiesByType.stream().filter(policy -> policy.getPolicyName().equals(storagePolicyName)).findAny()
                    .orElseThrow(() -> new DdlException("Storage policy does not exist. name: " + storagePolicyName));
            Optional<Policy> hasDefaultPolicy = policiesByType.stream()
                    .filter(policy -> policy.getPolicyName().equals(StoragePolicy.DEFAULT_STORAGE_POLICY_NAME))
                    .findAny();
            StoragePolicy.checkDefaultStoragePolicyValid(storagePolicyName, hasDefaultPolicy);
        } finally {
            readUnlock();
        }
    }
}
