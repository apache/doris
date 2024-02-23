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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * DatabaseProperty contains additional information about a database.
 *
 * Different properties are recognized by prefix, such as `iceberg`
 * If there is different type property is added, write a method,
 * such as `checkAndBuildIcebergProperty` to check and build it.
 */
public class DatabaseProperty implements Writable {

    @SerializedName(value = "properties")
    private Map<String, String> properties = Maps.newHashMap();

    public DatabaseProperty() {

    }

    public DatabaseProperty(Map<String, String> properties) {
        this.properties = properties;
    }

    public void put(String key, String val) {
        properties.put(key, val);
    }

    public String get(String key) {
        return properties.get(key);
    }

    public String getOrDefault(String key, String defaultVal) {
        return properties.getOrDefault(key, defaultVal);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public BinlogConfig getBinlogConfig() {
        BinlogConfig binlogConfig = new BinlogConfig();
        binlogConfig.mergeFromProperties(properties);
        return binlogConfig;
    }

    public void updateProperties(Map<String, String> newProperties) {
        properties.putAll(newProperties);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DatabaseProperty read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DatabaseProperty.class);
    }
}
