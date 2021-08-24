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

package org.apache.doris.stack.entity;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.activity.ActivityInfoResp;
import org.apache.doris.stack.model.activity.ActivityViewInfoResp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Entity
@Table(name = "activity")
@Data
@NoArgsConstructor
public class ActivityEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(length = 32, nullable = false)
    private String topic;

    @Column(name = "timestamp", nullable = false)
    private Timestamp timestamp;

    @Column(length = 16)
    private String model;

    @Column(name = "model_id")
    private Integer modelId;

    @Column(name = "user_id")
    private Integer userId;

    @Column(name = "database_id")
    private Integer databaseId;

    @Column(name = "table_id")
    private Integer tableId;

    @Column(name = "custom_id", length = 48)
    private String customId;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String details;

    public ActivityEntity(String topic, String model, String details) {
        this.timestamp = new Timestamp(System.currentTimeMillis());

        this.topic = topic;
        this.model = model;
        this.details = details;
    }

    public ActivityEntity(String topic, String model, String details, int modelId, int userId) {
        this.timestamp = new Timestamp(System.currentTimeMillis());

        this.topic = topic;
        this.model = model;
        this.modelId = modelId;
        this.details = details;
        this.userId = userId;
    }

    public ActivityEntity(String topic, String model, String details, int modelId, int userId,
                          int databaseId, int tableId) {
        this.timestamp = new Timestamp(System.currentTimeMillis());

        this.topic = topic;
        this.model = model;
        this.modelId = modelId;
        this.details = details;
        this.userId = userId;
        this.databaseId = databaseId;
        this.tableId = tableId;
    }

    public ActivityInfoResp transToModel() {
        ActivityInfoResp resp = new ActivityInfoResp();
        resp.setId(this.id);
        resp.setTableId(this.tableId);
        resp.setDatabaseId(this.databaseId);
        resp.setTopic(this.topic);
        resp.setCustomId(this.customId);
        resp.setDetails(JSON.parseObject(this.details, ActivityInfoResp.Details.class));
        resp.setModel(this.model);
        resp.setModelId(this.modelId);
        resp.setUserId(this.userId);
        resp.setTimestamp(this.timestamp);
        return resp;
    }

    public ActivityViewInfoResp transToViewModel() {
        ActivityViewInfoResp resp = new ActivityViewInfoResp();
        resp.setModel(this.model);
        resp.setModelId(this.modelId);
        resp.setUserId(this.userId);
        resp.setMaxTs(this.timestamp);
        return resp;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ModelActivityEntity {
        private int id;

        private String model;

        private Integer modelId;

        private Integer userId;
    }

}
