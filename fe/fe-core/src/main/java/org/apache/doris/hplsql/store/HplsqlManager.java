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

package org.apache.doris.hplsql.store;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class HplsqlManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(HplsqlManager.class);

    @SerializedName(value = "nameToStoredProcedures")
    Map<StoredKey, StoredProcedure> nameToStoredProcedures = Maps.newConcurrentMap();

    @SerializedName(value = "nameToPackages")
    Map<StoredKey, HplsqlPackage> nameToPackages = Maps.newConcurrentMap();

    public HplsqlManager() {
    }

    public StoredProcedure getStoredProcedure(StoredKey storedKey) {
        return nameToStoredProcedures.get(storedKey);
    }

    public void addStoredProcedure(StoredProcedure procedure, boolean isForce) {
        StoredKey storedKey = new StoredKey(procedure.getName(), procedure.getCatalogName(), procedure.getDbName());
        if (isForce) {
            nameToStoredProcedures.put(storedKey, procedure);
        } else if (nameToStoredProcedures.putIfAbsent(storedKey, procedure) != null) {
            throw new RuntimeException(storedKey + ", stored procedure already exist.");
        }
        Env.getCurrentEnv().getEditLog().logAddStoredProcedure(procedure);
        LOG.info("Add stored procedure success: {}", storedKey);
    }

    public void replayAddStoredProcedure(StoredProcedure procedure) {
        StoredKey storedKey = new StoredKey(procedure.getName(), procedure.getCatalogName(), procedure.getDbName());
        nameToStoredProcedures.put(storedKey, procedure);
        LOG.info("Replay add stored procedure success: {}", storedKey);
    }

    public void dropStoredProcedure(StoredKey storedKey) {
        nameToStoredProcedures.remove(storedKey);
        Env.getCurrentEnv().getEditLog().logDropStoredProcedure(storedKey);
        LOG.info("Drop stored procedure success: {}", storedKey);
    }

    public void replayDropStoredProcedure(StoredKey storedKey) {
        nameToStoredProcedures.remove(storedKey);
        LOG.info("Replay drop stored procedure success: {}", storedKey);
    }

    public HplsqlPackage getPackage(StoredKey storedKey) {
        return nameToPackages.get(storedKey);
    }

    public void addPackage(HplsqlPackage pkg, boolean isForce) {
        StoredKey storedKey = new StoredKey(pkg.getName(), pkg.getCatalogName(), pkg.getDbName());
        nameToPackages.put(storedKey, pkg);
        if (isForce) {
            nameToPackages.put(storedKey, pkg);
        } else if (nameToPackages.putIfAbsent(storedKey, pkg) != null) {
            throw new RuntimeException(storedKey + ", package already exist.");
        }
        Env.getCurrentEnv().getEditLog().logAddHplsqlPackage(pkg);
        LOG.info("Add hplsql package success: {}", storedKey);
    }

    public void replayAddPackage(HplsqlPackage pkg) {
        StoredKey storedKey = new StoredKey(pkg.getName(), pkg.getCatalogName(), pkg.getDbName());
        nameToPackages.put(storedKey, pkg);
        LOG.info("Replay add hplsql package success: {}", storedKey);
    }

    public void dropPackage(StoredKey storedKey) {
        nameToPackages.remove(storedKey);
        Env.getCurrentEnv().getEditLog().logDropHplsqlPackage(storedKey);
        LOG.info("Drop hplsql package success: {}", storedKey);
    }

    public void replayDropPackage(StoredKey storedKey) {
        nameToPackages.remove(storedKey);
        LOG.info("Replay drop hplsql package success: {}", storedKey);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static HplsqlManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, HplsqlManager.class);
    }
}
