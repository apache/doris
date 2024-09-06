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
import org.apache.doris.analysis.ShowStoragePolicyUsingStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.PushStoragePolicyTask;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

    // ctlName -> dbName -> tableName -> List<RowPolicy>
    private Map<String, Map<String, Map<String, List<RowPolicy>>>> tablePolicies = Maps.newConcurrentMap();

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
            boolean storagePolicyExists = false;
            if (PolicyTypeEnum.STORAGE == policy.getType()) {
                // The name of the storage policy remains globally unique until it is renamed by user.
                // So we could just compare the policy name to check if there are redundant ones.
                // Otherwise two storage policy share one same name but with different resource name
                // will not be filtered. See github #25025 for more details.
                storagePolicyExists = getPoliciesByType(PolicyTypeEnum.STORAGE)
                        .stream().anyMatch(p -> p.getPolicyName().equals(policy.getPolicyName()));
            }
            if (storagePolicyExists || existPolicy(policy)) {
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
     * Create policy through http api.
     **/
    public void addPolicy(Policy policy) throws UserException {
        writeLock();
        try {
            boolean storagePolicyExists = false;
            if (PolicyTypeEnum.STORAGE == policy.getType()) {
                // The name of the storage policy remains globally unique until it is renamed by user.
                // So we could just compare the policy name to check if there are redundant ones.
                // Otherwise two storage policy share one same name but with different resource name
                // will not be filtered. See github #25025 for more details.
                storagePolicyExists = getPoliciesByType(PolicyTypeEnum.STORAGE)
                        .stream().anyMatch(p -> p.getPolicyName().equals(policy.getPolicyName()));
            }
            if (storagePolicyExists || existPolicy(policy)) {
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
        if (dropPolicyLog.getType() == PolicyTypeEnum.STORAGE) {
            List<Database> databases = Env.getCurrentEnv().getInternalCatalog().getDbs();
            for (Database db : databases) {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    if (table instanceof OlapTable) {
                        OlapTable olapTable = (OlapTable) table;
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        for (Long partitionId : olapTable.getPartitionIds()) {
                            String policyName = partitionInfo.getDataProperty(partitionId).getStoragePolicy();
                            if (policyName.equals(dropPolicyLog.getPolicyName())) {
                                throw new DdlException("the policy " + policyName + " is used by table: "
                                    + table.getName());
                            }
                        }
                    }
                }
            }
        }
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
        // for compatible
        if (policy instanceof RowPolicy) {
            RowPolicy rowPolicy = (RowPolicy) policy;
            if (StringUtils.isEmpty(rowPolicy.getCtlName())) {
                Optional<Database> db = Env.getCurrentEnv().getInternalCatalog().getDb(rowPolicy.getDbId());
                if (!db.isPresent()) {
                    LOG.warn("db may be dropped,ignore CreatePolicyLog. dbId:" + rowPolicy.getDbId());
                    return;
                }
                Optional<Table> table = db.get().getTable(rowPolicy.getTableId());
                if (!table.isPresent()) {
                    LOG.warn("table may be dropped,ignore CreatePolicyLog. tableId:" + rowPolicy.getTableId());
                    return;
                }
                rowPolicy.setCtlName(InternalCatalog.INTERNAL_CATALOG_NAME);
                rowPolicy.setDbName(db.get().getName());
                rowPolicy.setTableName(table.get().getName());
            }
        }
        unprotectedAdd(policy);
        if (policy instanceof StoragePolicy) {
            ((StoragePolicy) policy).addResourceReference();
        }
        LOG.info("replay create policy: {}", policy);
    }

    private void unprotectedAdd(Policy policy) {
        if (policy == null) {
            return;
        }
        List<Policy> dbPolicies = getPoliciesByType(policy.getType());
        dbPolicies.add(policy);
        typeToPolicyMap.put(policy.getType(), dbPolicies);
        if (PolicyTypeEnum.ROW == policy.getType()) {
            addTablePolicies((RowPolicy) policy);
        }

    }

    public void replayDrop(DropPolicyLog log) {
        // for compatible
        if (log.getType() == PolicyTypeEnum.ROW && StringUtils.isEmpty(log.getCtlName())) {
            Optional<Database> db = Env.getCurrentEnv().getInternalCatalog().getDb(log.getDbId());
            if (!db.isPresent()) {
                LOG.warn("db may be dropped,ignore DropPolicyLog. dbId:" + log.getDbId());
                return;
            }
            Optional<Table> table = db.get().getTable(log.getTableId());
            if (!table.isPresent()) {
                LOG.warn("table may be dropped,ignore DropPolicyLog. tableId:" + log.getTableId());
                return;
            }
            log.setCtlName(InternalCatalog.INTERNAL_CATALOG_NAME);
            log.setDbName(db.get().getName());
            log.setTableName(table.get().getName());
        }
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
        policies.removeIf(policy -> {
            if (policy.matchPolicy(log)) {
                if (policy instanceof StoragePolicy) {
                    ((StoragePolicy) policy).removeResourceReference();
                    StoragePolicy storagePolicy = (StoragePolicy) policy;
                    LOG.info("the policy {} with id {} resource {} has been dropped",
                            storagePolicy.getPolicyName(), storagePolicy.getId(), storagePolicy.getStorageResource());
                }
                if (policy instanceof RowPolicy) {
                    dropTablePolicies((RowPolicy) policy);
                }
                return true;
            }
            return false;
        });
        typeToPolicyMap.put(log.getType(), policies);
    }

    /**
     * Match row policy and return it.
     **/
    public RowPolicy getMatchTablePolicy(String ctlName, String dbName, String tableName, UserIdentity user) {
        List<RowPolicy> res = getUserPolicies(ctlName, dbName, tableName, user);
        if (CollectionUtils.isEmpty(res)) {
            return null;
        }
        return mergeRowPolicies(res);
    }

    public List<RowPolicy> getUserPolicies(String ctlName, String dbName, String tableName, UserIdentity user) {
        List<RowPolicy> res = Lists.newArrayList();
        // Make a judgment in advance to reduce the number of times to obtain getRoles
        if (!tablePolicies.containsKey(ctlName) || !tablePolicies.get(ctlName).containsKey(dbName)
                || !tablePolicies.get(ctlName).get(dbName).containsKey(tableName)) {
            return res;
        }
        Set<String> roles = Env.getCurrentEnv().getAccessManager().getAuth().getRolesByUserWithLdap(user).stream()
                .map(role -> ClusterNamespace.getNameFromFullName(role.getRoleName())).collect(Collectors.toSet());
        readLock();
        try {
            // double check in lock,avoid NPE
            if (!tablePolicies.containsKey(ctlName) || !tablePolicies.get(ctlName).containsKey(dbName)
                    || !tablePolicies.get(ctlName).get(dbName).containsKey(tableName)) {
                return res;
            }
            List<RowPolicy> policys = tablePolicies.get(ctlName).get(dbName).get(tableName);
            for (RowPolicy rowPolicy : policys) {
                // on rowPolicy to user
                if ((rowPolicy.getUser() != null && rowPolicy.getUser().getQualifiedUser()
                        .equals(user.getQualifiedUser()))
                        || !StringUtils.isEmpty(rowPolicy.getRoleName()) && roles.contains(rowPolicy.getRoleName())) {
                    res.add(rowPolicy);
                }
            }
            return res;
        } finally {
            readUnlock();
        }
    }

    private RowPolicy mergeRowPolicies(List<RowPolicy> policys) {
        if (CollectionUtils.isEmpty(policys)) {
            return null;
        }
        RowPolicy andPolicy = null;
        RowPolicy orPolicy = null;
        for (RowPolicy rowPolicy : policys) {
            if (CompoundPredicate.Operator.AND.equals(rowPolicy.getFilterType().getOp())) {
                if (andPolicy == null) {
                    andPolicy = rowPolicy.clone();
                } else {
                    andPolicy.setWherePredicate(new CompoundPredicate(CompoundPredicate.Operator.AND,
                            andPolicy.getWherePredicate(), rowPolicy.getWherePredicate()));
                }
            } else {
                if (orPolicy == null) {
                    orPolicy = rowPolicy;
                } else {
                    orPolicy.setWherePredicate(new CompoundPredicate(CompoundPredicate.Operator.OR,
                            orPolicy.getWherePredicate(), rowPolicy.getWherePredicate()));
                }
            }
        }
        if (andPolicy == null) {
            return orPolicy;
        }
        if (orPolicy == null) {
            return andPolicy;
        }
        andPolicy.setWherePredicate(new CompoundPredicate(CompoundPredicate.Operator.AND, andPolicy.getWherePredicate(),
                orPolicy.getWherePredicate()));
        return andPolicy;
    }

    /**
     * Show policy through stmt.
     **/
    public ShowResultSet showPolicy(ShowPolicyStmt showStmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
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
                if (!StringUtils.isEmpty(showStmt.getRoleName())) {
                    rowPolicy.setRoleName(showStmt.getRoleName());
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
     * Show objects which is using the storage policy
     **/
    public ShowResultSet showStoragePolicyUsing(ShowStoragePolicyUsingStmt showStmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        String targetPolicyName = showStmt.getPolicyName();

        readLock();
        try {
            List<Database> databases = Env.getCurrentEnv().getInternalCatalog().getDbs();
            // show for all storage policies
            if (Strings.isNullOrEmpty(targetPolicyName)) {
                for (Database db : databases) {
                    List<Table> tables = db.getTables();
                    for (Table table : tables) {
                        if (!(table instanceof OlapTable)) {
                            continue;
                        }

                        Map<String, List<String>> policyToPartitionsMap = new HashMap<>();
                        OlapTable olapTable = (OlapTable) table;
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        // classify a table's all partitions by storage policy
                        for (Long partitionId : olapTable.getPartitionIds()) {
                            String policyName = partitionInfo.getDataProperty(partitionId).getStoragePolicy();
                            if (StringUtils.isEmpty(policyName)) {
                                continue;
                            }
                            if (policyToPartitionsMap.containsKey(policyName)) {
                                policyToPartitionsMap.get(policyName)
                                        .add(olapTable.getPartition(partitionId).getName());
                            } else {
                                List<String> partitionList = new ArrayList<>();
                                partitionList.add(olapTable.getPartition(partitionId).getName());
                                policyToPartitionsMap.put(policyName, partitionList);
                            }
                        }

                        //output, all partitions with same storage policy in a table will be shown in one line
                        if (policyToPartitionsMap.size() == 1) {
                            String[] policyArray = policyToPartitionsMap.keySet().toArray(new String[0]);
                            List<String> partitionsList = new ArrayList<>(policyToPartitionsMap.values()).get(0);
                            if (partitionsList.size() == olapTable.getPartitionNum()) {
                                List<String> row = Arrays.asList(policyArray[0], db.getName(), olapTable.getName(),
                                        "ALL");
                                rows.add(row);
                            } else {
                                List<String> row = Arrays.asList(policyArray[0], db.getName(), olapTable.getName(),
                                        String.join(",", partitionsList));
                                rows.add(row);
                            }
                        } else {
                            for (Map.Entry<String, List<String>> entry : policyToPartitionsMap.entrySet()) {
                                List<String> row = Arrays.asList(entry.getKey(), db.getName(), olapTable.getName(),
                                        String.join(",", entry.getValue()));
                                rows.add(row);
                            }
                        }
                    }
                }
            } else {
                // show for specific storage policy
                for (Database db : databases) {
                    List<Table> tables = db.getTables();
                    for (Table table : tables) {
                        if (!(table instanceof OlapTable)) {
                            continue;
                        }

                        OlapTable olapTable = (OlapTable) table;
                        int partitionMatchNum = 0;
                        StringBuilder matchPartitionsSB = new StringBuilder();
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        for (Long partitionId : olapTable.getPartitionIds()) {
                            String policyName = partitionInfo.getDataProperty(partitionId).getStoragePolicy();
                            if (policyName.equals(targetPolicyName)) {
                                partitionMatchNum++;
                                matchPartitionsSB.append(olapTable.getPartition(partitionId).getName()).append(",");
                            }
                        }

                        if (partitionMatchNum == 0) {
                            continue;
                        }

                        String matchPartitionsStr = "ALL";
                        if (partitionMatchNum < olapTable.getPartitionNum()) {
                            matchPartitionsStr = matchPartitionsSB.toString();
                            matchPartitionsStr = matchPartitionsStr.substring(0, matchPartitionsStr.length() - 1);
                        }

                        List<String> row = Arrays.asList(targetPolicyName, db.getName(), olapTable.getName(),
                                matchPartitionsStr);
                        rows.add(row);
                    }
                }
            }
            return new ShowResultSet(showStmt.getMetaData(), rows);
        } finally {
            readUnlock();
        }
    }

    private void addTablePolicies(RowPolicy policy) {
        if (policy.getUser() != null) {
            policy.getUser().setIsAnalyzed();
        }
        List<RowPolicy> policys = getOrCreateTblPolicies(policy.getCtlName(), policy.getDbName(),
                policy.getTableName());
        policys.add(policy);
    }

    private void dropTablePolicies(RowPolicy policy) {
        List<RowPolicy> policys = getOrCreateTblPolicies(policy.getCtlName(), policy.getDbName(),
                policy.getTableName());
        policys.removeIf(p -> p.matchPolicy(policy));
    }

    private List<RowPolicy> getOrCreateTblPolicies(String ctlName, String dbName, String tableName) {
        Map<String, List<RowPolicy>> dbPolicyMap = getOrCreateDbPolicyMap(ctlName, dbName);
        if (!dbPolicyMap.containsKey(tableName)) {
            dbPolicyMap.put(tableName, Lists.newArrayList());
        }
        return dbPolicyMap.get(tableName);
    }

    private Map<String, List<RowPolicy>> getOrCreateDbPolicyMap(String ctlName, String dbName) {
        Map<String, Map<String, List<RowPolicy>>> ctlPolicyMap = getOrCreateCtlPolicyMap(ctlName);
        if (!ctlPolicyMap.containsKey(dbName)) {
            ctlPolicyMap.put(dbName, Maps.newConcurrentMap());
        }
        return ctlPolicyMap.get(dbName);
    }

    private Map<String, Map<String, List<RowPolicy>>> getOrCreateCtlPolicyMap(String ctlName) {
        if (!tablePolicies.containsKey(ctlName)) {
            tablePolicies.put(ctlName, Maps.newConcurrentMap());
        }
        return tablePolicies.get(ctlName);
    }

    private void compatible() {
        readLock();
        try {
            if (!typeToPolicyMap.containsKey(PolicyTypeEnum.ROW)) {
                return;
            }
            List<Policy> allPolicies = typeToPolicyMap.get(PolicyTypeEnum.ROW);
            List<Policy> compatiblePolicies = Lists.newArrayList();
            for (Policy policy : allPolicies) {
                RowPolicy rowPolicy = (RowPolicy) policy;
                if (StringUtils.isEmpty(rowPolicy.getCtlName())) {
                    Optional<Database> db = Env.getCurrentEnv().getInternalCatalog().getDb(rowPolicy.getDbId());
                    if (!db.isPresent()) {
                        LOG.warn("db may be dropped,ignore DropPolicyLog. dbId:" + rowPolicy.getDbId());
                        continue;
                    }
                    Optional<Table> table = db.get().getTable(rowPolicy.getTableId());
                    if (!table.isPresent()) {
                        LOG.warn("table may be dropped,ignore DropPolicyLog. tableId:" + rowPolicy.getTableId());
                        continue;
                    }
                    rowPolicy.setCtlName(InternalCatalog.INTERNAL_CATALOG_NAME);
                    rowPolicy.setDbName(db.get().getName());
                    rowPolicy.setTableName(table.get().getName());
                }
                compatiblePolicies.add(rowPolicy);
            }
            typeToPolicyMap.put(PolicyTypeEnum.ROW, compatiblePolicies);
        } finally {
            readUnlock();
        }
    }

    /**
     * The merge policy cache needs to be regenerated after the update.
     **/
    private void updateTablePolicies() {
        readLock();
        try {
            if (!typeToPolicyMap.containsKey(PolicyTypeEnum.ROW)) {
                return;
            }
            List<Policy> allPolicies = typeToPolicyMap.get(PolicyTypeEnum.ROW);
            for (Policy policy : allPolicies) {
                addTablePolicies((RowPolicy) policy);
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
        // for compatible
        policyMgr.compatible();
        // update merge policy cache and userPolicySet
        policyMgr.updateTablePolicies();
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
        AgentBatchTask batchTask = new AgentBatchTask();
        for (long backendId : Env.getCurrentSystemInfo().getAllBackendsByAllCluster().keySet()) {
            PushStoragePolicyTask pushStoragePolicyTask = new PushStoragePolicyTask(backendId,
                    Collections.singletonList(storagePolicy), Collections.emptyList(), Collections.emptyList());
            batchTask.addTask(pushStoragePolicyTask);
        }
        AgentTaskExecutor.submit(batchTask);
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

    public boolean checkStoragePolicyIfSameResource(String policyName, String anotherPolicyName) {
        Optional<Policy> policy = findPolicy(policyName, PolicyTypeEnum.STORAGE);
        Optional<Policy> policy1 = findPolicy(anotherPolicyName, PolicyTypeEnum.STORAGE);
        if (policy1.isPresent() && policy.isPresent()) {
            StoragePolicy storagePolicy = (StoragePolicy) policy.get();
            StoragePolicy storagePolicy1 = (StoragePolicy) policy1.get();
            return storagePolicy1.getStorageResource().equals(storagePolicy.getStorageResource());
        }
        return false;
    }
}
