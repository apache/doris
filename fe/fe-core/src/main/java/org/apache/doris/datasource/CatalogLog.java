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

package org.apache.doris.datasource;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * A union metadata log for all the catalog operator include create,drop and alter.
 */
@NoArgsConstructor
@Getter
@Data
public class CatalogLog implements Writable {
    @SerializedName(value = "catalogId")
    private long catalogId;

    @SerializedName(value = "catalogName")
    private String catalogName;

    @SerializedName(value = "props")
    private Map<String, String> props;

    @SerializedName(value = "newCatalogName")
    private String newCatalogName;

    @SerializedName(value = "newProps")
    private Map<String, String> newProps;

    @SerializedName(value = "invalidCache")
    private boolean invalidCache;

    @SerializedName(value = "resource")
    private String resource;

    @SerializedName(value = "comment")
    private String comment;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CatalogLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CatalogLog.class);
    }
}
