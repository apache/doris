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
    private static final String INITIALIZE_IMMEDIATELY_PROPERTY = "plugin.initialize_immediately";

    @FunctionalInterface
    private interface MetadataTransformer {
        AuthenticationIntegrationMeta transform(AuthenticationIntegrationMeta current) throws DdlException;
    }

    private static final class DdlProperties {
        private final Map<String, String> metadataProperties;
        private final boolean initializeImmediately;

        private DdlProperties(Map<String, String> metadataProperties, boolean initializeImmediately) {
            this.metadataProperties = metadataProperties;
            this.initializeImmediately = initializeImmediately;
        }
    }

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "nTi")
    private Map<String, AuthenticationIntegrationMeta> nameToIntegration = new LinkedHashMap<>();

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

    public void createAuthenticationIntegration(
            String integrationName, boolean ifNotExists, Map<String, String> properties, String comment,
            String createUser)
            throws DdlException {
        DdlProperties ddlProperties = splitDdlProperties(properties, "CREATE AUTHENTICATION INTEGRATION");
        AuthenticationIntegrationMeta meta =
                AuthenticationIntegrationMeta.fromCreateSql(integrationName, ddlProperties.metadataProperties,
                        comment, createUser);
        if (authenticationIntegrationExists(integrationName)) {
            if (ifNotExists) {
                return;
            }
            throw new DdlException("Authentication integration " + integrationName + " already exists");
        }
        if (!ddlProperties.initializeImmediately) {
            writeLock();
            try {
                if (nameToIntegration.containsKey(integrationName)) {
                    if (ifNotExists) {
                        return;
                    }
                    throw new DdlException("Authentication integration " + integrationName + " already exists");
                }
                nameToIntegration.put(integrationName, meta);
            } finally {
                writeUnlock();
            }
            return;
        }
        AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration prepared =
                prepareAuthenticationIntegration(meta);
        try {
            writeLock();
            try {
                if (nameToIntegration.containsKey(integrationName)) {
                    if (ifNotExists) {
                        return;
                    }
                    throw new DdlException("Authentication integration " + integrationName + " already exists");
                }
                nameToIntegration.put(integrationName, meta);
                // TODO(authentication-integration): Re-enable edit log persistence
                // when authentication integration is fully integrated.
                // Env.getCurrentEnv().getEditLog().logCreateAuthenticationIntegration(meta);
                try {
                    getRuntime().activatePreparedAuthenticationIntegration(prepared);
                    prepared = null;
                } catch (RuntimeException e) {
                    nameToIntegration.remove(integrationName);
                    throw e;
                }
            } finally {
                writeUnlock();
            }
        } finally {
            discardPreparedAuthenticationIntegration(prepared);
        }
    }

    public void alterAuthenticationIntegrationProperties(
            String integrationName, Map<String, String> properties, String alterUser) throws DdlException {
        DdlProperties ddlProperties = splitDdlProperties(properties, "ALTER AUTHENTICATION INTEGRATION");
        updateAuthenticationIntegration(integrationName, current -> {
            if (ddlProperties.metadataProperties.isEmpty()) {
                if (ddlProperties.initializeImmediately) {
                    return current;
                }
                throw new DdlException("ALTER AUTHENTICATION INTEGRATION should contain at least one property");
            }
            return current.withAlterProperties(ddlProperties.metadataProperties, alterUser);
        }, ddlProperties.initializeImmediately);
    }

    public void alterAuthenticationIntegrationUnsetProperties(
            String integrationName, Set<String> propertiesToUnset, String alterUser) throws DdlException {
        updateAuthenticationIntegration(integrationName, current -> current.withUnsetProperties(propertiesToUnset,
                alterUser), false);
    }

    public void alterAuthenticationIntegrationComment(String integrationName, String comment, String alterUser)
            throws DdlException {
        writeLock();
        try {
            AuthenticationIntegrationMeta current = getOrThrowWithoutLock(integrationName);
            AuthenticationIntegrationMeta updated = current.withComment(comment, alterUser);
            nameToIntegration.put(integrationName, updated);
            // TODO(authentication-integration): Re-enable edit log persistence
            // when authentication integration is fully integrated.
            // Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
        } finally {
            writeUnlock();
        }
    }

    public void dropAuthenticationIntegration(String integrationName, boolean ifExists) throws DdlException {
        writeLock();
        try {
            AuthenticationIntegrationMeta current = nameToIntegration.get(integrationName);
            if (current == null) {
                if (ifExists) {
                    return;
                }
                throw new DdlException("Authentication integration " + integrationName + " does not exist");
            }
            nameToIntegration.remove(integrationName);
            // TODO(authentication-integration): Re-enable edit log persistence
            // when authentication integration is fully integrated.
            // Env.getCurrentEnv().getEditLog().logDropAuthenticationIntegration(
            //         new DropAuthenticationIntegrationOperationLog(integrationName));
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

    private void updateAuthenticationIntegration(String integrationName, MetadataTransformer transformer,
            boolean initializeImmediately)
            throws DdlException {
        while (true) {
            AuthenticationIntegrationMeta current = readAuthenticationIntegrationOrThrow(integrationName);
            AuthenticationIntegrationMeta updated = transformer.transform(current);
            if (!initializeImmediately) {
                writeLock();
                try {
                    AuthenticationIntegrationMeta latest = getOrThrowWithoutLock(integrationName);
                    if (latest != current) {
                        continue;
                    }
                    nameToIntegration.put(integrationName, updated);
                    getRuntime().markAuthenticationIntegrationDirty(integrationName);
                    return;
                } finally {
                    writeUnlock();
                }
            }
            AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration prepared =
                    prepareAuthenticationIntegration(updated);
            boolean shouldRetry = false;
            try {
                writeLock();
                try {
                    AuthenticationIntegrationMeta latest = getOrThrowWithoutLock(integrationName);
                    if (latest != current) {
                        shouldRetry = true;
                        continue;
                    }
                    nameToIntegration.put(integrationName, updated);
                    // TODO(authentication-integration): Re-enable edit log persistence
                    // when authentication integration is fully integrated.
                    // Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
                    try {
                        getRuntime().activatePreparedAuthenticationIntegration(prepared);
                        prepared = null;
                        return;
                    } catch (RuntimeException e) {
                        nameToIntegration.put(integrationName, current);
                        throw e;
                    }
                } finally {
                    writeUnlock();
                }
            } finally {
                discardPreparedAuthenticationIntegration(prepared);
            }
            if (!shouldRetry) {
                return;
            }
        }
    }

    private DdlProperties splitDdlProperties(Map<String, String> properties, String statement) throws DdlException {
        Map<String, String> metadataProperties = new LinkedHashMap<>();
        boolean initializeImmediately = false;
        boolean seenInitializeImmediately = false;
        if (properties == null) {
            return new DdlProperties(metadataProperties, false);
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (INITIALIZE_IMMEDIATELY_PROPERTY.equalsIgnoreCase(key)) {
                if (seenInitializeImmediately) {
                    throw new DdlException("Property '" + INITIALIZE_IMMEDIATELY_PROPERTY
                            + "' is duplicated in " + statement);
                }
                initializeImmediately = parseBooleanProperty(entry.getValue(), INITIALIZE_IMMEDIATELY_PROPERTY);
                seenInitializeImmediately = true;
                continue;
            }
            metadataProperties.put(key, entry.getValue());
        }
        return new DdlProperties(metadataProperties, initializeImmediately);
    }

    private static boolean parseBooleanProperty(String value, String propertyName) throws DdlException {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new DdlException("Property '" + propertyName + "' must be either 'true' or 'false'");
    }

    private AuthenticationIntegrationMeta readAuthenticationIntegrationOrThrow(String integrationName)
            throws DdlException {
        readLock();
        try {
            return getOrThrowWithoutLock(integrationName);
        } finally {
            readUnlock();
        }
    }

    private AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration prepareAuthenticationIntegration(
            AuthenticationIntegrationMeta meta) throws DdlException {
        try {
            return getRuntime().prepareAuthenticationIntegration(meta);
        } catch (AuthenticationException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private void discardPreparedAuthenticationIntegration(
            AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration prepared) {
        if (prepared != null) {
            getRuntime().discardPreparedAuthenticationIntegration(prepared);
        }
    }

    private AuthenticationIntegrationRuntime getRuntime() {
        return Env.getCurrentEnv().getAuthenticationIntegrationRuntime();
    }

    private boolean authenticationIntegrationExists(String integrationName) {
        readLock();
        try {
            return nameToIntegration.containsKey(integrationName);
        } finally {
            readUnlock();
        }
    }

    private AuthenticationIntegrationMeta getOrThrowWithoutLock(String integrationName) throws DdlException {
        AuthenticationIntegrationMeta meta = nameToIntegration.get(integrationName);
        if (meta == null) {
            throw new DdlException("Authentication integration " + integrationName + " does not exist");
        }
        return meta;
    }
}
