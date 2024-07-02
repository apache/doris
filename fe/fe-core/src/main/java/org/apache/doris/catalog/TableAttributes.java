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

import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TableAttributes contains additional information about all table
 */
public class TableAttributes {
    public static final long TABLE_INIT_VERSION = 1L;

    @SerializedName(value = "constraints")
    private final Map<String, Constraint> constraintsMap = new HashMap<>();
    @SerializedName(value = "visibleVersion")
    private long visibleVersion;
    @SerializedName(value = "visibleVersionTime")
    private long visibleVersionTime;

    public TableAttributes() {
        this.visibleVersion = TABLE_INIT_VERSION;
        this.visibleVersionTime = System.currentTimeMillis();
    }

    public Map<String, Constraint> getConstraintsMap() {
        return constraintsMap;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }

    public long getVisibleVersionTime() {
        return visibleVersionTime;
    }

    public void updateVisibleVersionAndTime(long visibleVersion, long visibleVersionTime) {
        // To be compatible with previous versions
        if (visibleVersion <= TABLE_INIT_VERSION) {
            return;
        }
        this.visibleVersion = visibleVersion;
        this.visibleVersionTime = visibleVersionTime;
    }

    public long getNextVersion() {
        return visibleVersion + 1;
    }

    public TableAttributes read(DataInput in) throws IOException {
        TableAttributes tableAttributes = GsonUtils.GSON.fromJson(Text.readString(in), TableAttributes.class);
        // To be compatible with previous versions
        if (tableAttributes.getVisibleVersion() < TABLE_INIT_VERSION) {
            tableAttributes.visibleVersion = TABLE_INIT_VERSION;
            tableAttributes.visibleVersionTime = System.currentTimeMillis();
        }
        return tableAttributes;
    }
}
