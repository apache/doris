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

/**
 * Snapshot read for time travel
 * supports external iceberg/hudi table
 */
public class TableSnapshot {

    public enum VersionType {
        TIME, VERSION,
        // ISO SQL 2011 standard syntax: FOR SYSTEM_TIME AS OF '<timestamp>'
        // Explicitly routes to the metadata-driven MVCC time travel path
        // (FDB in cloud mode, EditLog in coupled mode when implemented).
        // Use this when a table has both enable_time_travel and binlog enabled.
        SYSTEM_TIME
    }

    private final VersionType type;
    private String value;

    public TableSnapshot(TableSnapshot other) {
        this.type = other.type;
        this.value = other.value;
    }

    public TableSnapshot(String value, VersionType type) {
        this.value = value;
        this.type = type;
    }

    public static TableSnapshot timeOf(String time) {
        return new TableSnapshot(time, VersionType.TIME);
    }

    public static TableSnapshot systemTimeOf(String time) {
        return new TableSnapshot(time, VersionType.SYSTEM_TIME);
    }

    public static TableSnapshot versionOf(String time) {
        return new TableSnapshot(time, VersionType.VERSION);
    }

    public String getValue() {
        return value;
    }

    public VersionType getType() {
        return type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        if (this.type == VersionType.VERSION) {
            return " FOR VERSION AS OF " + value;
        } else if (this.type == VersionType.SYSTEM_TIME) {
            return " FOR SYSTEM_TIME AS OF '" + value + "'";
        } else {
            return " FOR TIME AS OF '" + value + "'";
        }
    }

    public String toDigest() {
        if (this.type == VersionType.VERSION) {
            return " FOR VERSION AS OF " + '?';
        } else if (this.type == VersionType.SYSTEM_TIME) {
            return " FOR SYSTEM_TIME AS OF '" + '?' + "'";
        } else {
            return " FOR TIME AS OF '" + '?' + "'";
        }
    }
}
