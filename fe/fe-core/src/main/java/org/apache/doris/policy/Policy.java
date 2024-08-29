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

import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class for Policy.
 **/
@Data
public abstract class Policy implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(Policy.class);

    @SerializedName(value = "id")
    protected long id = -1;

    @SerializedName(value = "type")
    protected PolicyTypeEnum type = null;

    @SerializedName(value = "policyName")
    protected String policyName = null;

    @SerializedName(value = "version")
    protected long version = -1;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    // just for subclass lombok @Data
    public Policy() {
    }

    public Policy(PolicyTypeEnum type) {
        this.type = type;
    }

    /**
     * Base class for Policy.
     *
     * @param type policy type
     * @param policyName policy name
     */
    public Policy(long id, final PolicyTypeEnum type, final String policyName) {
        this.id = id;
        this.type = type;
        this.policyName = policyName;
        this.version = 0;
    }

    /**
     * Trans stmt to Policy.
     **/
    public static Policy fromCreateStmt(CreatePolicyStmt stmt) throws AnalysisException {
        long policyId = Env.getCurrentEnv().getNextId();
        switch (stmt.getType()) {
            case STORAGE:
                StoragePolicy storagePolicy = new StoragePolicy(policyId, stmt.getPolicyName());
                storagePolicy.init(stmt.getProperties(), stmt.isIfNotExists());
                return storagePolicy;
            case ROW:
                // stmt must be analyzed.
                UserIdentity userIdent = stmt.getUser();
                if (userIdent != null) {
                    userIdent.analyze();
                }
                return new RowPolicy(policyId, stmt.getPolicyName(), stmt.getTableName().getCtl(),
                        stmt.getTableName().getDb(), stmt.getTableName().getTbl(), userIdent, stmt.getRoleName(),
                        stmt.getOrigStmt().originStmt, stmt.getOrigStmt().idx, stmt.getFilterType(),
                        stmt.getWherePredicate());
            default:
                throw new AnalysisException("Unknown policy type: " + stmt.getType());
        }
    }

    public void modifyProperties(Map<String, String> properties) throws DdlException, AnalysisException {
    }

    /**
     * Use for SHOW POLICY.
     **/
    public abstract List<String> getShowInfo() throws AnalysisException;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * Read Policy from file.
     **/
    public static Policy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Policy.class);
    }

    protected boolean checkMatched(PolicyTypeEnum type, String policyName) {
        return (type == null || type.equals(this.type))
                && (policyName == null || StringUtils.equals(policyName, this.policyName));
    }

    // it is used to check whether this policy is in PolicyMgr
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        return checkMatched(checkedPolicyCondition.getType(), checkedPolicyCondition.getPolicyName());
    }

    public boolean matchPolicy(DropPolicyLog checkedDropPolicyLogCondition) {
        return checkMatched(checkedDropPolicyLogCondition.getType(), checkedDropPolicyLogCondition.getPolicyName());
    }

    public abstract boolean isInvalid();

}
