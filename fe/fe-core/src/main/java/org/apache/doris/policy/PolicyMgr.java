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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class PolicyMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(PolicyMgr.class);
    
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    
    @SerializedName(value = "dbIdToPolicyMap")
    private Map<Long, List<Policy>> dbIdToPolicyMap = Maps.newConcurrentMap();
    
    @SerializedName(value = "userToPolicyMap")
    private Map<String, List<Policy>> userToPolicyMap = Maps.newConcurrentMap();
    
    private void writeLock() {
        lock.writeLock().lock();
    }
    
    private void writeUnlock() {
        lock.writeLock().unlock();
    }
    
    public void createPolicy(CreatePolicyStmt stmt) throws UserException {
        writeLock();
        try {
            Policy policy = Policy.fromCreateStmt(stmt);
            if (existPolicy(policy.getDbId(), policy.getTableId(), policy.getType(), policy.getPolicyName())) {
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
    
    public void dropSqlBlockRule(DropPolicyStmt stmt) throws DdlException {
        writeLock();
        try {
            DropPolicyLog policy = DropPolicyLog.fromDropStmt(stmt);
            if (!existPolicy(policy.getDbId(), policy.getTableId(), policy.getType(), policy.getPolicyName())) {
                if (stmt.isIfExists()) {
                    return;
                }
                throw new DdlException("the policy " + policy.getPolicyName() + " not exist");
            }
            unprotectedDrop(policy);
            Catalog.getCurrentCatalog().getEditLog().logDropPolicy(policy);
        } finally {
            writeUnlock();
        }
    }
    
    private boolean existPolicy(long dbId, long tableId, String type, String policyName) {
        List<Policy> policies = getDbPolicies(dbId);
        return policies.stream().anyMatch(policy -> matchPolicy(policy, type, tableId, policyName));
    }
    
    public List<Policy> getDbPolicies(long dbId) {
        if (dbIdToPolicyMap == null) {
            return new ArrayList<>();
        }
        return dbIdToPolicyMap.getOrDefault(dbId, new ArrayList<>());
    }
    
    public List<Policy> getUserPolicies(String user) {
        if (userToPolicyMap == null) {
            return new ArrayList<>();
        }
        return userToPolicyMap.getOrDefault(user, new ArrayList<>());
    }
    
    public void unprotectedAdd(Policy policy) {
        if (policy == null) {
            return;
        }
        long dbId = policy.getDbId();
        String user = policy.getUser();
        if (dbId == 0 || user == null) {
            LOG.warn("policy={} error", policy);
            return;
        }
        List<Policy> dbPolicies = getDbPolicies(dbId);
        dbPolicies.add(policy);
        dbIdToPolicyMap.put(dbId, dbPolicies);
        List<Policy> userPolicies = getUserPolicies(user);
        userPolicies.add(policy);
        userToPolicyMap.put(user, userPolicies);
    }
    
    public void unprotectedDrop(DropPolicyLog log) {
        List<Policy> policies = getDbPolicies(log.getDbId());
        policies.removeIf(p -> matchPolicy(p, log.getType(), log.getTableId(), log.getPolicyName()));
        dbIdToPolicyMap.put(log.getDbId(), policies);
        userToPolicyMap.forEach((user, userPolicies) -> {
            boolean remove = userPolicies.removeIf(p -> matchPolicy(p, log.getType(), log.getTableId(), log.getPolicyName()));

            if (remove) {
                userToPolicyMap.put(user, userPolicies);
            }
        });
    }
    
    private boolean matchPolicy(Policy policy, String type, long tableId, String policyName) {
        return StringUtils.equals(policy.getType(), type)
            && policy.getTableId() == tableId
            && StringUtils.equals(policy.getPolicyName(), policyName);
    }

    public Policy getMatchPolicy(long dbId, long tableId, String user) {
        if (!dbIdToPolicyMap.containsKey(dbId)) {
            return null;
        }
        List<Policy> policies = dbIdToPolicyMap.get(dbId);
        List<Policy> userPolicies = policies.stream().filter(policy -> policy.getTableId() == tableId && StringUtils.equals(policy.getUser(), user)).collect(Collectors.toList());
        if (userPolicies.isEmpty()) {
            return null;
        }
        // op use last
        Policy lastPolicy = userPolicies.get(userPolicies.size() - 1);
        CompoundPredicate.Operator op = lastPolicy.getFilterType().getOp();
        Policy ret = null;
        for (Policy policy : userPolicies) {
            if (ret == null) {
                ret = policy;
            } else {
                // merge filter
                CompoundPredicate compoundPredicate = new CompoundPredicate(op, ret.getWherePredicate(), policy.getWherePredicate());
                ret.setWherePredicate(compoundPredicate);
            }
        }
        return ret;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
    
    public static PolicyMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PolicyMgr.class);
    }
}
