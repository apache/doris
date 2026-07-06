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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.DropAuthenticationIntegrationOperationLog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for AUTHENTICATION INTEGRATION metadata.
 */
public class AuthenticationIntegrationMgr implements Writable {
    private static final String CHECK_VALIDATION_PROPERTY = "check_validation";

    private static final class ValidationConfig {
        private final Map<String, String> properties;
        private final boolean checkValidation;

        private ValidationConfig(Map<String, String> properties, boolean checkValidation) {
            this.properties = properties;
            this.checkValidation = checkValidation;
        }
    }

    private static final class UnsetValidationConfig {
        private final Set<String> propertiesToUnset;
        private final boolean checkValidation;

        private UnsetValidationConfig(Set<String> propertiesToUnset, boolean checkValidation) {
            this.propertiesToUnset = propertiesToUnset;
            this.checkValidation = checkValidation;
        }
    }

    // Lock ordering across authentication metadata managers:
    // AuthenticationIntegrationMgr.lock -> RoleMappingMgr.lock.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "nTi")
    private Map<String, AuthenticationIntegrationMeta> nameToIntegration = new LinkedHashMap<>();

    void readLock() {
        lock.readLock().lock();
    }

    void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void createAuthenticationIntegration(
            String integrationName, boolean ifNotExists, Map<String, String> properties, String comment,
            String createUser)
            throws DdlException {
        ValidationConfig validationConfig = extractValidationConfig(properties);
        writeLock();
        try {
            if (nameToIntegration.containsKey(integrationName)) {
                if (ifNotExists) {
                    return;
                }
                throw new DdlException("Authentication integration " + integrationName + " already exists");
            }
            AuthenticationIntegrationMeta meta = AuthenticationIntegrationMeta.fromCreateSql(
                    integrationName, validationConfig.properties, comment, createUser);
            validateAuthenticationIntegration(meta, validationConfig.checkValidation);
            nameToIntegration.put(integrationName, meta);
            Env.getCurrentEnv().getEditLog().logCreateAuthenticationIntegration(meta);
        } finally {
            writeUnlock();
        }
    }

    public void alterAuthenticationIntegrationProperties(
            String integrationName, Map<String, String> properties, String alterUser) throws DdlException {
        ValidationConfig validationConfig = extractValidationConfig(properties);
        writeLock();
        try {
            AuthenticationIntegrationMeta current = getOrThrow(integrationName);
            AuthenticationIntegrationMeta updated =
                    current.withAlterProperties(validationConfig.properties, alterUser);
            validateAuthenticationIntegration(updated, validationConfig.checkValidation);
            nameToIntegration.put(integrationName, updated);
            Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
            Env.getCurrentEnv().getAuthenticationIntegrationRuntime().markAuthenticationIntegrationDirty(
                    integrationName);
        } finally {
            writeUnlock();
        }
    }

    public void alterAuthenticationIntegrationUnsetProperties(
            String integrationName, Set<String> propertiesToUnset, String alterUser) throws DdlException {
        UnsetValidationConfig validationConfig = extractUnsetValidationConfig(propertiesToUnset);
        writeLock();
        try {
            AuthenticationIntegrationMeta current = getOrThrow(integrationName);
            AuthenticationIntegrationMeta updated =
                    current.withUnsetProperties(validationConfig.propertiesToUnset, alterUser);
            validateAuthenticationIntegration(updated, validationConfig.checkValidation);
            nameToIntegration.put(integrationName, updated);
            Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
            Env.getCurrentEnv().getAuthenticationIntegrationRuntime().markAuthenticationIntegrationDirty(
                    integrationName);
        } finally {
            writeUnlock();
        }
    }

    public void alterAuthenticationIntegrationComment(String integrationName, String comment, String alterUser)
            throws DdlException {
        writeLock();
        try {
            AuthenticationIntegrationMeta current = getOrThrow(integrationName);
            AuthenticationIntegrationMeta updated = current.withComment(comment, alterUser);
            nameToIntegration.put(integrationName, updated);
            Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
        } finally {
            writeUnlock();
        }
    }

    public void dropAuthenticationIntegration(String integrationName, boolean ifExists) throws DdlException {
        writeLock();
        try {
            if (!nameToIntegration.containsKey(integrationName)) {
                if (ifExists) {
                    return;
                }
                throw new DdlException("Authentication integration " + integrationName + " does not exist");
            }
            if (Env.getCurrentEnv().getRoleMappingMgr().hasRoleMapping(integrationName)) {
                throw new DdlException("Authentication integration " + integrationName + " still has role mapping");
            }
            nameToIntegration.remove(integrationName);
            Env.getCurrentEnv().getEditLog().logDropAuthenticationIntegration(
                    new DropAuthenticationIntegrationOperationLog(integrationName));
            Env.getCurrentEnv().getAuthenticationIntegrationRuntime()
                    .removeAuthenticationIntegration(integrationName);
        } finally {
            writeUnlock();
        }
    }

    public void replayCreateAuthenticationIntegration(AuthenticationIntegrationMeta meta) {
        writeLock();
        try {
            nameToIntegration.put(meta.getName(), meta);
        } finally {
            writeUnlock();
        }
    }

    public void replayAlterAuthenticationIntegration(AuthenticationIntegrationMeta meta) {
        writeLock();
        try {
            nameToIntegration.put(meta.getName(), meta);
        } finally {
            writeUnlock();
        }
    }

    public void replayDropAuthenticationIntegration(DropAuthenticationIntegrationOperationLog log) {
        writeLock();
        try {
            nameToIntegration.remove(log.getIntegrationName());
        } finally {
            writeUnlock();
        }
    }

    public AuthenticationIntegrationMeta getAuthenticationIntegration(String integrationName) {
        readLock();
        try {
            return nameToIntegration.get(integrationName);
        } finally {
            readUnlock();
        }
    }

    public Map<String, AuthenticationIntegrationMeta> getAuthenticationIntegrations() {
        readLock();
        try {
            return Collections.unmodifiableMap(new LinkedHashMap<>(nameToIntegration));
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AuthenticationIntegrationMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        AuthenticationIntegrationMgr mgr = GsonUtils.GSON.fromJson(json, AuthenticationIntegrationMgr.class);
        if (mgr.nameToIntegration == null) {
            mgr.nameToIntegration = new LinkedHashMap<>();
        }
        return mgr;
    }

    private AuthenticationIntegrationMeta getOrThrow(String integrationName) throws DdlException {
        AuthenticationIntegrationMeta meta = nameToIntegration.get(integrationName);
        if (meta == null) {
            throw new DdlException("Authentication integration " + integrationName + " does not exist");
        }
        return meta;
    }

    private void validateAuthenticationIntegration(AuthenticationIntegrationMeta meta, boolean checkValidation)
            throws DdlException {
        if (!checkValidation) {
            return;
        }
        try {
            Env.getCurrentEnv().getAuthenticationIntegrationRuntime().validateAuthenticationIntegration(meta);
        } catch (AuthenticationException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private ValidationConfig extractValidationConfig(Map<String, String> properties) throws DdlException {
        Map<String, String> sanitizedProperties = new LinkedHashMap<>();
        boolean checkValidation = true;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (CHECK_VALIDATION_PROPERTY.equalsIgnoreCase(key)) {
                String value = entry.getValue();
                if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                    throw new DdlException(
                            "Property '" + CHECK_VALIDATION_PROPERTY + "' must be 'true' or 'false'");
                }
                checkValidation = Boolean.parseBoolean(value);
                continue;
            }
            sanitizedProperties.put(key, entry.getValue());
        }
        return new ValidationConfig(sanitizedProperties, checkValidation);
    }

    private UnsetValidationConfig extractUnsetValidationConfig(Set<String> propertiesToUnset) {
        Set<String> sanitizedPropertiesToUnset = new java.util.LinkedHashSet<>();
        boolean checkValidation = true;
        for (String key : propertiesToUnset) {
            if (CHECK_VALIDATION_PROPERTY.equalsIgnoreCase(key)) {
                checkValidation = false;
                continue;
            }
            sanitizedPropertiesToUnset.add(key);
        }
        return new UnsetValidationConfig(sanitizedPropertiesToUnset, checkValidation);
    }
}
