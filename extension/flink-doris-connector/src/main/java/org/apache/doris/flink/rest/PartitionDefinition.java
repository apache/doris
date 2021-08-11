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

package org.apache.doris.flink.rest;

import org.apache.doris.flink.cfg.DorisOptions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Doris  partition info.
 */
public class PartitionDefinition implements Serializable, Comparable<PartitionDefinition> {
    private final String database;
    private final String table;

    private final String beAddress;
    private final Set<Long> tabletIds;
    private final String queryPlan;
    private final String serializedSettings;

    public PartitionDefinition(String database, String table,
                               DorisOptions settings, String beAddress, Set<Long> tabletIds, String queryPlan)
            throws IllegalArgumentException {
        if (settings != null) {
            this.serializedSettings = settings.save();
        } else {
            this.serializedSettings = null;
        }
        this.database = database;
        this.table = table;
        this.beAddress = beAddress;
        this.tabletIds = tabletIds;
        this.queryPlan = queryPlan;
    }

    public String getBeAddress() {
        return beAddress;
    }

    public Set<Long> getTabletIds() {
        return tabletIds;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getQueryPlan() {
        return queryPlan;
    }


    @Override
    public int compareTo(PartitionDefinition o) {
        int cmp = database.compareTo(o.database);
        if (cmp != 0) {
            return cmp;
        }
        cmp = table.compareTo(o.table);
        if (cmp != 0) {
            return cmp;
        }
        cmp = beAddress.compareTo(o.beAddress);
        if (cmp != 0) {
            return cmp;
        }
        cmp = queryPlan.compareTo(o.queryPlan);
        if (cmp != 0) {
            return cmp;
        }

        cmp = tabletIds.size() - o.tabletIds.size();
        if (cmp != 0) {
            return cmp;
        }

        Set<Long> similar = new HashSet<>(tabletIds);
        Set<Long> diffSelf = new HashSet<>(tabletIds);
        Set<Long> diffOther = new HashSet<>(o.tabletIds);
        similar.retainAll(o.tabletIds);
        diffSelf.removeAll(similar);
        diffOther.removeAll(similar);
        if (diffSelf.size() == 0) {
            return 0;
        }
        long diff = Collections.min(diffSelf) - Collections.min(diffOther);
        return diff < 0 ? -1 : 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionDefinition that = (PartitionDefinition) o;
        return Objects.equals(database, that.database) &&
                Objects.equals(table, that.table) &&
                Objects.equals(beAddress, that.beAddress) &&
                Objects.equals(tabletIds, that.tabletIds) &&
                Objects.equals(queryPlan, that.queryPlan) &&
                Objects.equals(serializedSettings, that.serializedSettings);
    }

    @Override
    public int hashCode() {
        int result = database.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + beAddress.hashCode();
        result = 31 * result + queryPlan.hashCode();
        result = 31 * result + tabletIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PartitionDefinition{" +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", beAddress='" + beAddress + '\'' +
                ", tabletIds=" + tabletIds +
                ", queryPlan='" + queryPlan + '\'' +
                '}';
    }
}
