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

package org.apache.doris.authentication;

import org.apache.doris.authentication.rolemapping.UnifiedRoleMappingCelEngine;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleMappingCommand;
import org.apache.doris.persist.DropRoleMappingOperationLog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for ROLE MAPPING metadata.
 */
public class RoleMappingMgr implements Writable {
    // Lock ordering across authentication metadata managers:
    // AuthenticationIntegrationMgr.lock -> RoleMappingMgr.lock.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "nTrm")
    private Map<String, RoleMappingMeta> nameToRoleMapping = new LinkedHashMap<>();

    private transient Map<String, String> integrationToMappingName = new LinkedHashMap<>();

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void createRoleMapping(String mappingName, boolean ifNotExists, String integrationName,
            List<CreateRoleMappingCommand.RoleMappingRule> rules, String comment, String createUser)
            throws DdlException {
        Env env = Objects.requireNonNull(Env.getCurrentEnv(), "currentEnv can not be null");
        AuthenticationIntegrationMgr integrationMgr = env.getAuthenticationIntegrationMgr();
        integrationMgr.readLock();
        try {
            writeLock();
            try {
                if (nameToRoleMapping.containsKey(mappingName)) {
                    if (ifNotExists) {
                        return;
                    }
                    throw new DdlException("Role mapping " + mappingName + " already exists");
                }
                if (integrationMgr.getAuthenticationIntegration(integrationName) == null) {
                    throw new DdlException("Authentication integration " + integrationName + " does not exist");
                }
                if (integrationToMappingName.containsKey(integrationName)) {
                    throw new DdlException("Authentication integration " + integrationName
                            + " already has a role mapping");
                }

                List<RoleMappingMeta.RuleMeta> validatedRules = validateAndNormalizeRules(rules, env);
                RoleMappingMeta meta = RoleMappingMeta.fromCreateSql(
                        mappingName, integrationName, validatedRules, comment, createUser);
                validateCompilable(meta);
                putRoleMappingInternal(meta);
                env.getEditLog().logCreateRoleMapping(meta);
            } finally {
                writeUnlock();
            }
        } finally {
            integrationMgr.readUnlock();
        }
    }

    public void dropRoleMapping(String mappingName, boolean ifExists) throws DdlException {
        Env env = Objects.requireNonNull(Env.getCurrentEnv(), "currentEnv can not be null");
        writeLock();
        try {
            if (!nameToRoleMapping.containsKey(mappingName)) {
                if (ifExists) {
                    return;
                }
                throw new DdlException("Role mapping " + mappingName + " does not exist");
            }
            removeRoleMappingInternal(mappingName);
            env.getEditLog().logDropRoleMapping(new DropRoleMappingOperationLog(mappingName));
        } finally {
            writeUnlock();
        }
    }

    public void replayCreateRoleMapping(RoleMappingMeta meta) {
        writeLock();
        try {
            putRoleMappingInternal(meta);
        } finally {
            writeUnlock();
        }
    }

    public void replayDropRoleMapping(DropRoleMappingOperationLog log) {
        writeLock();
        try {
            removeRoleMappingInternal(log.getMappingName());
        } finally {
            writeUnlock();
        }
    }

    public RoleMappingMeta getRoleMapping(String mappingName) {
        readLock();
        try {
            return nameToRoleMapping.get(mappingName);
        } finally {
            readUnlock();
        }
    }

    public RoleMappingMeta getRoleMappingByIntegration(String integrationName) {
        readLock();
        try {
            String mappingName = integrationToMappingName.get(integrationName);
            return mappingName == null ? null : nameToRoleMapping.get(mappingName);
        } finally {
            readUnlock();
        }
    }

    public boolean hasRoleMapping(String integrationName) {
        readLock();
        try {
            return integrationToMappingName.containsKey(integrationName);
        } finally {
            readUnlock();
        }
    }

    public Map<String, RoleMappingMeta> getRoleMappings() {
        readLock();
        try {
            return Collections.unmodifiableMap(new LinkedHashMap<>(nameToRoleMapping));
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static RoleMappingMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        RoleMappingMgr mgr = GsonUtils.GSON.fromJson(json, RoleMappingMgr.class);
        if (mgr.nameToRoleMapping == null) {
            mgr.nameToRoleMapping = new LinkedHashMap<>();
        }
        mgr.integrationToMappingName = new LinkedHashMap<>();
        mgr.rebuildIntegrationIndex();
        return mgr;
    }

    private List<RoleMappingMeta.RuleMeta> validateAndNormalizeRules(
            List<CreateRoleMappingCommand.RoleMappingRule> rules, Env env) throws DdlException {
        Objects.requireNonNull(rules, "rules can not be null");
        if (rules.isEmpty()) {
            throw new DdlException("CREATE ROLE MAPPING should contain at least one RULE");
        }

        List<RoleMappingMeta.RuleMeta> result = new ArrayList<>(rules.size());
        for (CreateRoleMappingCommand.RoleMappingRule rule : rules) {
            String condition = normalizeCondition(rule.getCondition());
            Set<String> grantedRoles = normalizeGrantedRoles(rule.getGrantedRoles(), env);
            result.add(new RoleMappingMeta.RuleMeta(condition, grantedRoles));
        }
        return result;
    }

    private String normalizeCondition(String condition) throws DdlException {
        String normalized = Objects.requireNonNull(condition, "condition can not be null").trim();
        if (normalized.isEmpty()) {
            throw new DdlException("Role mapping rule condition must not be empty");
        }
        return normalized;
    }

    private Set<String> normalizeGrantedRoles(Set<String> grantedRoles, Env env) throws DdlException {
        Objects.requireNonNull(grantedRoles, "grantedRoles can not be null");
        if (grantedRoles.isEmpty()) {
            throw new DdlException("Role mapping rule must grant at least one role");
        }
        LinkedHashSet<String> normalizedRoles = new LinkedHashSet<>();
        for (String grantedRole : grantedRoles) {
            String normalizedRole = Objects.requireNonNull(grantedRole, "grantedRole can not be null").trim();
            if (normalizedRole.isEmpty()) {
                throw new DdlException("Role mapping rule must grant at least one role");
            }
            if (!env.getAuth().doesRoleExist(normalizedRole)) {
                throw new DdlException("Role " + normalizedRole + " does not exist");
            }
            normalizedRoles.add(normalizedRole);
        }
        return Collections.unmodifiableSet(normalizedRoles);
    }

    private void validateCompilable(RoleMappingMeta meta) throws DdlException {
        try {
            new UnifiedRoleMappingCelEngine(meta.toDefinition().toEngineRules());
        } catch (IllegalArgumentException e) {
            throw new DdlException("Invalid role mapping condition in " + meta.getName() + ": "
                    + e.getMessage());
        }
    }

    private void putRoleMappingInternal(RoleMappingMeta meta) {
        RoleMappingMeta previousMeta = nameToRoleMapping.put(meta.getName(), meta);
        if (previousMeta != null) {
            integrationToMappingName.remove(previousMeta.getIntegrationName());
        }
        String previousMappingName = integrationToMappingName.put(meta.getIntegrationName(), meta.getName());
        if (previousMappingName != null && !previousMappingName.equals(meta.getName())) {
            throw new IllegalStateException("Authentication integration " + meta.getIntegrationName()
                    + " maps to multiple role mappings");
        }
    }

    private void removeRoleMappingInternal(String mappingName) {
        RoleMappingMeta removed = nameToRoleMapping.remove(mappingName);
        if (removed != null) {
            integrationToMappingName.remove(removed.getIntegrationName());
        }
    }

    private void rebuildIntegrationIndex() {
        integrationToMappingName.clear();
        for (RoleMappingMeta meta : nameToRoleMapping.values()) {
            String previousMappingName = integrationToMappingName.put(meta.getIntegrationName(), meta.getName());
            if (previousMappingName != null && !previousMappingName.equals(meta.getName())) {
                throw new IllegalStateException("Authentication integration " + meta.getIntegrationName()
                        + " maps to multiple role mappings");
            }
        }
    }
}
