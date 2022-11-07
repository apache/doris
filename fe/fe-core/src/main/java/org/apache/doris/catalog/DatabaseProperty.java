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

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.external.iceberg.IcebergCatalog;
import org.apache.doris.external.iceberg.IcebergCatalogMgr;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DatabaseProperty contains additional information about a database.
 *
 * Different properties are recognized by prefix, such as `iceberg`
 * If there is different type property is added, write a method,
 * such as `checkAndBuildIcebergProperty` to check and build it.
 */
public class DatabaseProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(DatabaseProperty.class);

    public static final String ICEBERG_PROPERTY_PREFIX = "iceberg";

    @SerializedName(value = "properties")
    private Map<String, String> properties = Maps.newHashMap();

    // the following variables are built from "properties"
    private IcebergProperty icebergProperty = new IcebergProperty(Maps.newHashMap());

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

    public IcebergProperty getIcebergProperty() {
        return icebergProperty;
    }

    public DatabaseProperty checkAndBuildProperties() throws DdlException {
        Map<String, String> icebergProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            if (entry.getKey().startsWith(ICEBERG_PROPERTY_PREFIX)) {
                if (Config.disable_iceberg_hudi_table) {
                    throw new DdlException(
                            "database for iceberg is no longer supported. Use multi catalog feature instead."
                                    + ". Or you can temporarily set 'disable_iceberg_hudi_table=false'"
                                    + " in fe.conf to reopen this feature.");
                } else {
                    icebergProperties.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (icebergProperties.size() > 0) {
            checkAndBuildIcebergProperty(icebergProperties);
        }
        return this;
    }

    private void checkAndBuildIcebergProperty(Map<String, String> properties) throws DdlException {
        IcebergCatalogMgr.validateProperties(properties, false);
        icebergProperty = new IcebergProperty(properties);
        String icebergDb = icebergProperty.getDatabase();
        IcebergCatalog icebergCatalog = IcebergCatalogMgr.getCatalog(icebergProperty);
        // check database exists
        if (!icebergCatalog.databaseExists(icebergDb)) {
            throw new DdlException("Database [" + icebergDb + "] dose not exist in Iceberg.");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DatabaseProperty read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DatabaseProperty.class);
    }
}
