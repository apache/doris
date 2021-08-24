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

package org.apache.doris.stack.model.response.config;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class GeoInfo {
    private String name;

    private String url;

    // region_key
    private String regionKey;

    // region_name
    private String regionName;

    private boolean builtin;

    @JSONField(name = "region_key")
    @JsonProperty("region_key")
    public String getRegionKey() {
        return regionKey;
    }

    @JSONField(name = "region_key")
    @JsonProperty("region_key")
    public void setRegionKey(String regionKey) {
        this.regionKey = regionKey;
    }

    @JSONField(name = "region_name")
    @JsonProperty("region_name")
    public String getRegionName() {
        return regionName;
    }

    @JSONField(name = "region_name")
    @JsonProperty("region_name")
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}
