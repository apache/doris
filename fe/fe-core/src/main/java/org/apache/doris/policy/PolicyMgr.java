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

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CreateTablePolicyStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.ShowPolicyStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;
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
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Management policy and cache it.
 **/
public class PolicyMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(PolicyMgr.class);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "dbIdToPolicyMap")
    private Map<Long, List<Policy>> dbIdToPolicyMap = Maps.newConcurrentMap();

    /**
     * Cache merge policy for match.
     * key：dbId:tableId-type-user
     **/
    private Map<Long, Map<String, TablePolicy>> dbIdToMergePolicyMap = Maps.newConcurrentMap();

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
     * Create policy through stmt.
     **/
    public void createPolicy(CreateTablePolicyStmt stmt) throws UserException {
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
            Catalog.getCurrentCatalog().getEditLog().logCreatePolicy(policy);
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
            Catalog.getCurrentCatalog().getEditLog().logDropPolicy(dropPolicyLog);
        } finally {
            writeUnlock();
        }
    }

    public boolean existPolicy(String user) {
        return userPolicySet.contains(user);
    }

    private boolean existPolicy(Policy checkedPolicy) {
        List<Policy> policies = getDbPolicies(checkedPolicy.getDbId());
        return policies.stream().anyMatch(policy -> policy.matchPolicy(checkedPolicy));
    }

    private boolean existPolicy(DropPolicyLog checkedDropPolicy) {
        List<Policy> policies = getDbPolicies(checkedDropPolicy.getDbId());
        return policies.stream().anyMatch(policy -> policy.matchPolicy(checkedDropPolicy));
    }

    private List<Policy> getDbPolicies(long dbId) {
        if (dbIdToPolicyMap == null) {
            return new ArrayList<>();
        }
        return dbIdToPolicyMap.getOrDefault(dbId, new ArrayList<>());
    }

    private List<Policy> getDbUserPolicies(long dbId, String user) {
        if (dbIdToPolicyMap == null) {
            return new ArrayList<>();
        }
        return dbIdToPolicyMap.getOrDefault(dbId, new ArrayList<>()).stream()
                .filter(p -> p.getUser().getQualifiedUser().equals(user)).collect(Collectors.toList());
    }

    public void replayCreate(Policy policy) {
        unprotectedAdd(policy);
        LOG.info("replay create policy: {}", policy);
    }

    private void unprotectedAdd(Policy policy) {
        if (policy == null) {
            return;
        }
        long dbId = policy.getDbId();
        List<Policy> dbPolicies = getDbPolicies(dbId);
        dbPolicies.add(policy);
        dbIdToPolicyMap.put(dbId, dbPolicies);
        updateMergePolicyMap(dbId);
        userPolicySet.add(policy.getUser().getQualifiedUser());
    }

    public void replayDrop(DropPolicyLog log) {
        unprotectedDrop(log);
        LOG.info("replay drop policy log: {}", log);
    }

    private void unprotectedDrop(DropPolicyLog log) {
        long dbId = log.getDbId();
        List<Policy> policies = getDbPolicies(dbId);
        policies.removeIf(policy -> policy.matchPolicy(log));
        dbIdToPolicyMap.put(dbId, policies);
        updateMergePolicyMap(dbId);
        if (log.getUser() == null) {
            updateAllUserPolicySet();
        } else {
            String user = log.getUser().getQualifiedUser();
            if (!existUserPolicy(user)) {
                userPolicySet.remove(user);
            }
        }
    }

    /**
     * Match row policy and return it.
     **/
    public TablePolicy getMatchRowPolicy(long dbId, long tableId, String user) {
        readLock();
        try {
            if (!dbIdToMergePolicyMap.containsKey(dbId)) {
                return null;
            }
            String key = Joiner.on("-").join(tableId, PolicyTypeEnum.ROW.name(), user);
            if (!dbIdToMergePolicyMap.get(dbId).containsKey(key)) {
                return null;
            }
            return dbIdToMergePolicyMap.get(dbId).get(key);
        } finally {
            readUnlock();
        }
    }

    /**
     * Show policy through stmt.
     **/
    public ShowResultSet showPolicy(ShowPolicyStmt showStmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        List<Policy> policies;
        long currentDbId = ConnectContext.get().getCurrentDbId();
        if (showStmt.getUser() == null) {
            policies = Catalog.getCurrentCatalog().getPolicyMgr().getDbPolicies(currentDbId);
        } else {
            policies = Catalog.getCurrentCatalog().getPolicyMgr()
                    .getDbUserPolicies(currentDbId, showStmt.getUser().getQualifiedUser());
        }
        for (Policy policy : policies) {
            if (policy.isInvalid()) {
                continue;
            }
            rows.add(policy.getShowInfo());
        }
        return new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void updateAllMergePolicyMap() {
        dbIdToPolicyMap.forEach((dbId, policies) -> updateMergePolicyMap(dbId));
    }

    private void updateAllUserPolicySet() {
        userPolicySet.clear();
        dbIdToPolicyMap.forEach((dbId, policies) ->
                policies.forEach(policy -> userPolicySet.add(policy.getUser().getQualifiedUser())));
    }


    private boolean existUserPolicy(String user) {
        readLock();
        try {
            for (Map.Entry<Long, List<Policy>> entry : dbIdToPolicyMap.entrySet()) {
                for (Policy policy : entry.getValue()) {
                    if (policy.getUser().getQualifiedUser().equals(user)) {
                        return true;
                    }
                }
            }
            return false;
        } finally {
            readUnlock();
        }

    }

    /**
     * The merge policy cache needs to be regenerated after the update.
     **/
    private void updateMergePolicyMap(long dbId) {
        readLock();
        try {
            if (!dbIdToPolicyMap.containsKey(dbId)) {
                return;
            }
            List<Policy> policies = dbIdToPolicyMap.get(dbId);
            Map<String, TablePolicy> andMap = new HashMap<>();
            Map<String, TablePolicy> orMap = new HashMap<>();
            for (Policy policy : policies) {
                if (!(policy instanceof TablePolicy)) {
                    continue;
                }
                TablePolicy tablePolicy = (TablePolicy) policy;
                // read from json, need set isAnalyzed
                policy.getUser().setIsAnalyzed();
                String key =
                        Joiner.on("-").join(tablePolicy.getTableId(), tablePolicy.getType(),
                                tablePolicy.getUser().getQualifiedUser());
                // merge wherePredicate
                if (CompoundPredicate.Operator.AND.equals(tablePolicy.getFilterType().getOp())) {
                    TablePolicy frontPolicy = andMap.get(key);
                    if (frontPolicy == null) {
                        andMap.put(key, tablePolicy.clone());
                    } else {
                        frontPolicy.setWherePredicate(
                                new CompoundPredicate(CompoundPredicate.Operator.AND, frontPolicy.getWherePredicate(),
                                        tablePolicy.getWherePredicate()));
                        andMap.put(key, frontPolicy.clone());
                    }
                } else {
                    TablePolicy frontPolicy = orMap.get(key);
                    if (frontPolicy == null) {
                        orMap.put(key, tablePolicy.clone());
                    } else {
                        frontPolicy.setWherePredicate(
                                new CompoundPredicate(CompoundPredicate.Operator.OR, frontPolicy.getWherePredicate(),
                                        tablePolicy.getWherePredicate()));
                        orMap.put(key, frontPolicy.clone());
                    }
                }
            }
            Map<String, TablePolicy> mergeMap = new HashMap<>();
            Set<String> policyKeys = new HashSet<>();
            policyKeys.addAll(andMap.keySet());
            policyKeys.addAll(orMap.keySet());
            policyKeys.forEach(key -> {
                if (andMap.containsKey(key) && orMap.containsKey(key)) {
                    TablePolicy mergePolicy = andMap.get(key).clone();
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
            dbIdToMergePolicyMap.put(dbId, mergeMap);
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
        // update merge policy cache
        policyMgr.updateAllMergePolicyMap();
        // update user policy cache
        policyMgr.updateAllUserPolicySet();
        return policyMgr;
    }
}
