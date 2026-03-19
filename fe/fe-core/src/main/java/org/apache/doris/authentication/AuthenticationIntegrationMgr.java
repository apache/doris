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
        AuthenticationIntegrationMeta meta =
                AuthenticationIntegrationMeta.fromCreateSql(integrationName, properties, comment, createUser);
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
        } finally {
            writeUnlock();
        }
    }

    public void alterAuthenticationIntegrationProperties(
            String integrationName, Map<String, String> properties, String alterUser) throws DdlException {
        writeLock();
        try {
            AuthenticationIntegrationMeta current = getOrThrow(integrationName);
            AuthenticationIntegrationMeta updated = current.withAlterProperties(properties, alterUser);
            nameToIntegration.put(integrationName, updated);
            // TODO(authentication-integration): Re-enable edit log persistence
            // when authentication integration is fully integrated.
            // Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
            Env.getCurrentEnv().getAuthenticationIntegrationRuntime().markAuthenticationIntegrationDirty(
                    integrationName);
        } finally {
            writeUnlock();
        }
    }

    public void alterAuthenticationIntegrationUnsetProperties(
            String integrationName, Set<String> propertiesToUnset, String alterUser) throws DdlException {
        writeLock();
        try {
            AuthenticationIntegrationMeta current = getOrThrow(integrationName);
            AuthenticationIntegrationMeta updated = current.withUnsetProperties(propertiesToUnset, alterUser);
            nameToIntegration.put(integrationName, updated);
            // TODO(authentication-integration): Re-enable edit log persistence
            // when authentication integration is fully integrated.
            // Env.getCurrentEnv().getEditLog().logAlterAuthenticationIntegration(updated);
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
            if (!nameToIntegration.containsKey(integrationName)) {
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

    private AuthenticationIntegrationMeta getOrThrow(String integrationName) throws DdlException {
        AuthenticationIntegrationMeta meta = nameToIntegration.get(integrationName);
        if (meta == null) {
            throw new DdlException("Authentication integration " + integrationName + " does not exist");
        }
        return meta;
    }
}
