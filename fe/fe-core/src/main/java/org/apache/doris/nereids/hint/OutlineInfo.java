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

package org.apache.doris.nereids.hint;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * outline information which outline should have
 */
public class OutlineInfo implements Writable {
    @SerializedName("outlineName")
    private final String outlineName;
    @SerializedName("visibleSignature")
    private final String visibleSignature;
    @SerializedName("sqlId")
    private final String sqlId;
    @SerializedName("sqlText")
    private final String sqlText;
    @SerializedName("outlineTarget")
    private final String outlineTarget;
    @SerializedName("outlineData")
    private final String outlineData;

    /**
     * outline information which outline should have
     * @param outlineName name of outline, also as a key in outline map
     * @param visibleSignature sql we want to add outline after change constant to param
     * @param sqlId sql id use visible signature as hash key
     * @param sqlText sql we want to add outline before change constant to param
     * @param outlineTarget sql we want to add outline with syntax on
     * @param outlineData outline data which we need to add to matched sqls
     */
    public OutlineInfo(String outlineName, String visibleSignature, String sqlId, String sqlText, String outlineTarget,
                        String outlineData) {
        this.outlineName = outlineName;
        this.visibleSignature = visibleSignature;
        this.sqlId = sqlId;
        this.sqlText = sqlText;
        this.outlineTarget = outlineTarget;
        this.outlineData = outlineData;
    }

    public String getOutlineName() {
        return outlineName;
    }

    public String getVisibleSignature() {
        return visibleSignature;
    }

    public String getOutlineData() {
        return outlineData;
    }

    public String getOutlineTarget() {
        return outlineTarget;
    }

    public String getSqlId() {
        return sqlId;
    }

    public String getSqlText() {
        return sqlText;
    }

    public static OutlineInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), OutlineInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

}
