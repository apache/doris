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

package org.apache.doris.analysis;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;


/**
 * Describe storage properties
 * The structure diagram is divided into three levels:
 *            StorageDesc
 *          /            \
 *    BrokerDesc        The other StorageBackend.StorageType desc
 *        |
 *  The broker's StorageBackend.StorageType desc
 */
public class StorageDesc extends ResourceDesc implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(StorageDesc.class);


    @Deprecated
    @SerializedName("st")
    protected StorageBackend.StorageType storageType;

    /** SPI facade binding; lazily bound from the raw properties. Not Gson-serialized. */
    protected StorageAdapter storageAdapter;

    public StorageDesc() {
    }

    public StorageDesc(String name, StorageBackend.StorageType storageType, Map<String, String> properties) {
        this.name = name;
        this.storageType = storageType;
        this.properties = properties;
        initStorageAdapter();
    }

    protected void initStorageAdapter() {
        if (storageAdapter != null) {
            return;
        }
        if (properties == null) {
            properties = new HashMap<>();
        }
        if (null != storageType && storageType.equals(StorageBackend.StorageType.BROKER)) {
            this.storageAdapter = StorageAdapter.ofBroker(name, properties);
            return;
        }
        if (!properties.isEmpty()) {
            this.storageAdapter = StorageAdapter.of(properties);
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setStorageType(StorageBackend.StorageType storageType) {
        this.storageType = storageType;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public StorageBackend.StorageType getStorageType() {
        return storageType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getBackendConfigProperties() {
        initStorageAdapter();
        if (null == storageAdapter) {
            return properties;
        }
        return storageAdapter.getBackendConfigProperties();
    }

    public StorageAdapter getStorageAdapter() {
        initStorageAdapter();
        return storageAdapter;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // Binding runs plugin code: bindPrimary probes every loaded provider and bind() belongs to
        // the one that claims the map. This method runs at image load and journal replay for every
        // persisted load and export job, so with that provider absent - a filesystem plugin that
        // failed to load - a throw here would take the image load down, or kill a serving follower
        // at the next OP_CREATE_LOAD_JOB. Leave the adapter unbound instead: every getter re-runs
        // initStorageAdapter() lazily, so the job binds at use and fails there with a Status.
        try {
            initStorageAdapter();
        } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
            LOG.warn("Storage descriptor (name={}, type={}) could not bind its filesystem provider at"
                    + " load; the binding is retried at use: {}", name, storageType, e.getMessage());
        }
    }
}
