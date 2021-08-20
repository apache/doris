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

package org.apache.doris.stack.model.activity;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.sql.Timestamp;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActivityViewInfoResp {

    private Integer userId;

    private Integer modelId;

    private String model;

    private int cnt;

    private Timestamp maxTs;

    private ModelObject modelObject;

    @JSONField(name = "user_id")
    @JsonProperty("user_id")
    public Integer getUserId() {
        return userId;
    }

    @JSONField(name = "user_id")
    @JsonProperty("user_id")
    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    @JSONField(name = "model_id")
    @JsonProperty("model_id")
    public Integer getModelId() {
        return modelId;
    }

    @JSONField(name = "model_id")
    @JsonProperty("model_id")
    public void setModelId(Integer modelId) {
        this.modelId = modelId;
    }

    @JSONField(name = "max_ts")
    @JsonProperty("max_ts")
    public Timestamp getMaxTs() {
        return maxTs;
    }

    @JSONField(name = "max_ts")
    @JsonProperty("max_ts")
    public void setMaxTs(Timestamp maxTs) {
        this.maxTs = maxTs;
    }

    @JSONField(name = "model_object")
    @JsonProperty("model_object")
    public ModelObject getModelObject() {
        return modelObject;
    }

    @JSONField(name = "model_object")
    @JsonProperty("model_object")
    public void setModelObject(ModelObject modelObject) {
        this.modelObject = modelObject;
    }

}
