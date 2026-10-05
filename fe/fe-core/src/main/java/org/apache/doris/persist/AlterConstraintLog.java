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

package org.apache.doris.persist;

import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Edit log entry for constraint add/drop operations.
 * Uses TableNameInfo (name-based) for persistence.
 * Supports backward compatibility with old TableIdentifier (ID-based) format
 * via GsonPostProcessable migration.
 */
public class AlterConstraintLog implements Writable, GsonPostProcessable {
    @SerializedName("ct")
    final Constraint constraint;

    // Old format: ID-based table identifier (kept for backward compat deserialization)
    @SerializedName("tid")
    final TableIdentifier tableIdentifier;

    // New format: name-based table info
    @SerializedName("tni")
    private TableNameInfo tableNameInfo;

    public AlterConstraintLog(Constraint constraint, TableNameInfo tableNameInfo) {
        this.constraint = constraint;
        this.tableNameInfo = tableNameInfo;
        this.tableIdentifier = null;
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterConstraintLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterConstraintLog.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // Migrate from old ID-based format to name-based format
        if (tableNameInfo == null && tableIdentifier != null) {
            try {
                String qualifiedName = tableIdentifier.toQualifiedName();
                if (qualifiedName != null) {
                    tableNameInfo = new TableNameInfo(qualifiedName);
                }
            } catch (Exception ignored) {
                // Old table may no longer exist; tableNameInfo stays null
            }
        }
    }
}
