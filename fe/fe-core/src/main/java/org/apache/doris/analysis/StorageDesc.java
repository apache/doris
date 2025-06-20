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

import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;

import java.util.Map;


/**
 * Describe storage properties
 * The structure diagram is divided into three levels:
 *            StorageDesc
 *          /            \
 *    BrokerDesc        The other StorageBackend.StorageType desc
 *        |
 *  The broker's StorageBackend.StorageType desc
 */
public class StorageDesc extends ResourceDesc {

    @Deprecated
    @SerializedName("st")
    protected StorageBackend.StorageType storageType;

    @Getter
    protected StorageProperties storageProperties;

    public StorageDesc() {
    }

    public StorageDesc(String name, StorageBackend.StorageType storageType, Map<String, String> properties) {
        this.name = name;
        this.storageType = storageType;
        this.properties = properties;
        initStorageProperties();
    }

    private void initStorageProperties() {
        if (null != storageType && storageType.equals(StorageBackend.StorageType.BROKER)) {
            this.storageProperties = BrokerProperties.of(name, properties);
        } else {
            this.storageProperties = StorageProperties.createPrimary(properties);
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
        if (null == storageProperties) {
            return properties;
        }
        return storageProperties.getBackendConfigProperties();
    }

    public StorageProperties getStorageProperties() {
        return storageProperties;
    }
}
