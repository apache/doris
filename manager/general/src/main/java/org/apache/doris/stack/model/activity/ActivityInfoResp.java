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
import org.apache.doris.stack.model.meta.DataBaseResp;
import org.apache.doris.stack.model.meta.TableResp;
import org.apache.doris.stack.model.response.user.UserInfo;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActivityInfoResp {

    private int id;

    private Integer tableId;

    private TableResp table;

    private Integer databaseId;

    private DataBaseResp database;

    private boolean modelExists;

    private String topic;

    private String customId;

    private Details details;

    private Integer modelId;

    private String model;

    private Integer userId;

    private UserInfo user;

    private Timestamp timestamp;

    @JSONField(name = "table_id")
    @JsonProperty("table_id")
    public Integer getTableId() {
        return tableId;
    }

    @JSONField(name = "table_id")
    @JsonProperty("table_id")
    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    @JSONField(name = "database_id")
    @JsonProperty("database_id")
    public Integer getDatabaseId() {
        return databaseId;
    }

    @JSONField(name = "database_id")
    @JsonProperty("database_id")
    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }

    @JSONField(name = "model_exists")
    @JsonProperty("model_exists")
    public boolean isModelExists() {
        return modelExists;
    }

    @JSONField(name = "model_exists")
    @JsonProperty("model_exists")
    public void setModelExists(boolean modelExists) {
        this.modelExists = modelExists;
    }

    @JSONField(name = "custom_id")
    @JsonProperty("custom_id")
    public String getCustomId() {
        return customId;
    }

    @JSONField(name = "custom_id")
    @JsonProperty("custom_id")
    public void setCustomId(String customId) {
        this.customId = customId;
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

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Details {
        private String name;

        private String description;

        private List<Dashcard> dashcards;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Dashcard {
        private String name;

        private String description;

        private int id;

        private int cardId;

        private boolean exists;

        @JSONField(name = "card_id")
        @JsonProperty("card_id")
        public int getCardId() {
            return cardId;
        }

        @JSONField(name = "card_id")
        @JsonProperty("card_id")
        public void setCardId(int cardId) {
            this.cardId = cardId;
        }
    }

}
