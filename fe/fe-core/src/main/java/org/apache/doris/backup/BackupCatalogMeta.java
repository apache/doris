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

package org.apache.doris.backup;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class BackupCatalogMeta implements Writable {
    @SerializedName(value = "catalogName")
    String catalogName;
    @SerializedName(value = "resource")
    String resource;
    @SerializedName(value = "properties")
    Map<String, String> properties;
    @SerializedName(value = "comment")
    String comment;

    public BackupCatalogMeta(String catalogName, String resource, Map<String, String> properties,
                             String comment) {
        this.catalogName = catalogName;
        this.resource = Strings.nullToEmpty(resource);
        this.comment = Strings.nullToEmpty(comment);
        this.properties = Maps.newHashMap(properties);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getResource() {
        return resource;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BackupCatalogMeta read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BackupCatalogMeta.class);
    }
}
