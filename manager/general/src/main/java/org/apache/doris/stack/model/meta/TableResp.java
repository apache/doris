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

package org.apache.doris.stack.model.meta;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableResp {

    private boolean active;

    private String caveats;

    private String description;

    private int id;

    private String name;

    private String schema;

    private DataBaseResp db;

    private Detail details;

    private Timestamp createdAt;

    private int dbId;

    private String displayName;

    private String entityName;

    private String entityType;

    private String fieldOrder;

    private String pointsOfInterest;

    private boolean showInGettingStarted;

    private Timestamp updatedAt;

    private String visibilityType;

    @JSONField(name = "created_at")
    @JsonProperty("created_at")
    public Timestamp getCreatedAt() {
        return createdAt;
    }

    @JSONField(name = "created_at")
    @JsonProperty("created_at")
    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    @JSONField(name = "db_id")
    @JsonProperty("db_id")
    public int getDbId() {
        return dbId;
    }

    @JSONField(name = "db_id")
    @JsonProperty("db_id")
    public void setDbId(int dbId) {
        this.dbId = dbId;
    }

    @JSONField(name = "display_name")
    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

    @JSONField(name = "display_name")
    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JSONField(name = "entity_name")
    @JsonProperty("entity_name")
    public String getEntityName() {
        return entityName;
    }

    @JSONField(name = "entity_name")
    @JsonProperty("entity_name")
    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    @JSONField(name = "entity_type")
    @JsonProperty("entity_type")
    public String getEntityType() {
        return entityType;
    }

    @JSONField(name = "entity_type")
    @JsonProperty("entity_type")
    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    @JSONField(name = "field_order")
    @JsonProperty("field_order")
    public String getFieldOrder() {
        return fieldOrder;
    }

    @JSONField(name = "field_order")
    @JsonProperty("field_order")
    public void setFieldOrder(String fieldOrder) {
        this.fieldOrder = fieldOrder;
    }

    @JSONField(name = "points_of_interest")
    @JsonProperty("points_of_interest")
    public String getPointsOfInterest() {
        return pointsOfInterest;
    }

    @JSONField(name = "points_of_interest")
    @JsonProperty("points_of_interest")
    public void setPointsOfInterest(String pointsOfInterest) {
        this.pointsOfInterest = pointsOfInterest;
    }

    @JSONField(name = "show_in_getting_started")
    @JsonProperty("show_in_getting_started")
    public boolean isShowInGettingStarted() {
        return showInGettingStarted;
    }

    @JSONField(name = "show_in_getting_started")
    @JsonProperty("show_in_getting_started")
    public void setShowInGettingStarted(boolean showInGettingStarted) {
        this.showInGettingStarted = showInGettingStarted;
    }

    @JSONField(name = "updated_at")
    @JsonProperty("updated_at")
    public Timestamp getUpdatedAt() {
        return updatedAt;
    }

    @JSONField(name = "updated_at")
    @JsonProperty("updated_at")
    public void setUpdatedAt(Timestamp updatedAt) {
        this.updatedAt = updatedAt;
    }

    @JSONField(name = "visibility_type")
    @JsonProperty("visibility_type")
    public String getVisibilityType() {
        return visibilityType;
    }

    @JSONField(name = "visibility_type")
    @JsonProperty("visibility_type")
    public void setVisibilityType(String visibilityType) {
        this.visibilityType = visibilityType;
    }

    /**
     * Detail
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class Detail {

        private static final String DB_NAME = "dbname";

        private static final String HOST = "host";

        private static final String PASSWORD = "password";

        private static final String PORT = "port";

        private static final String USER = "user";

        private String dbName;

        private String host;

        private String password;

        private int port;

        private String user;

        public Detail(String detail) {
            if (!StringUtils.isEmpty(detail)) {
                JSONObject jsonObject = JSONObject.parseObject(detail);
                this.dbName = jsonObject.getString(DB_NAME);
                this.host = jsonObject.getString(HOST);
                this.password = jsonObject.getString(PASSWORD);
                this.port = jsonObject.getInteger(PORT);
                this.user = jsonObject.getString(USER);
            }
        }
    }

    /**
     * Database
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class DataBase {

        private String description;

        private String timezone;

        private String engine;

        private String caveats;

        private String name;

        private String options;

        private List<String> features;

        private DataBaseResp.Detail details;

        private List<DataBaseResp.Table> tables;

        private String updatedAt;

        private String pointsOfInterest;

        private String nativePermissions;

        private String metadataSyncSchedule;

        private boolean autoRunQueries;

        private String cacheFieldValuesSchedule;

        private boolean isFullSync;

        private boolean isOnDemand;

        private boolean isSample;

        @JSONField(name = "updated_at")
        @JsonProperty("updated_at")
        public String getUpdatedAt() {
            return updatedAt;
        }

        @JSONField(name = "updated_at")
        @JsonProperty("updated_at")
        public void setUpdatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
        }

        @JSONField(name = "points_of_interest")
        @JsonProperty("points_of_interest")
        public String getPointsOfInterest() {
            return pointsOfInterest;
        }

        @JSONField(name = "points_of_interest")
        @JsonProperty("points_of_interest")
        public void setPointsOfInterest(String pointsOfInterest) {
            this.pointsOfInterest = pointsOfInterest;
        }

        @JSONField(name = "native_permissions")
        @JsonProperty("native_permissions")
        public String getNativePermissions() {
            return nativePermissions;
        }

        @JSONField(name = "native_permissions")
        @JsonProperty("native_permissions")
        public void setNativePermissions(String nativePermissions) {
            this.nativePermissions = nativePermissions;
        }

        @JSONField(name = "metadata_sync_schedule")
        @JsonProperty("metadata_sync_schedule")
        public String getMetadataSyncSchedule() {
            return metadataSyncSchedule;
        }

        @JSONField(name = "metadata_sync_schedule")
        @JsonProperty("metadata_sync_schedule")
        public void setMetadataSyncSchedule(String metadataSyncSchedule) {
            this.metadataSyncSchedule = metadataSyncSchedule;
        }

        @JSONField(name = "auto_run_queries")
        @JsonProperty("auto_run_queries")
        public boolean isAutoRunQueries() {
            return autoRunQueries;
        }

        @JSONField(name = "auto_run_queries")
        @JsonProperty("auto_run_queries")
        public void setAutoRunQueries(boolean autoRunQueries) {
            this.autoRunQueries = autoRunQueries;
        }

        @JSONField(name = "cache_field_values_schedule")
        @JsonProperty("cache_field_values_schedule")
        public String getCacheFieldValuesSchedule() {
            return cacheFieldValuesSchedule;
        }

        @JSONField(name = "cache_field_values_schedule")
        @JsonProperty("cache_field_values_schedule")
        public void setCacheFieldValuesSchedule(String cacheFieldValuesSchedule) {
            this.cacheFieldValuesSchedule = cacheFieldValuesSchedule;
        }

        @JSONField(name = "is_full_sync")
        @JsonProperty("is_full_sync")
        public boolean isFullSync() {
            return isFullSync;
        }

        @JSONField(name = "is_full_sync")
        @JsonProperty("is_full_sync")
        public void setFullSync(boolean fullSync) {
            isFullSync = fullSync;
        }

        @JSONField(name = "is_on_demand")
        @JsonProperty("is_on_demand")
        public boolean isOnDemand() {
            return isOnDemand;
        }

        @JSONField(name = "is_on_demand")
        @JsonProperty("is_on_demand")
        public void setOnDemand(boolean onDemand) {
            isOnDemand = onDemand;
        }

        @JSONField(name = "is_sample")
        @JsonProperty("is_sample")
        public boolean isSample() {
            return isSample;
        }

        @JSONField(name = "is_sample")
        @JsonProperty("is_sample")
        public void setSample(boolean sample) {
            isSample = sample;
        }

        {
            this.features = new ArrayList<>();
            this.features.add("basic-aggregations");
            this.features.add("standard-deviation-aggregations");
            this.features.add("expression-aggregations");
            this.features.add("foreign-keys");
            this.features.add("right-join");
            this.features.add("left-join");
            this.features.add("native-parameters");
            this.features.add("nested-queries");
            this.features.add("expressions");
            this.features.add("set-timezone");
            this.features.add("binning");
            this.features.add("inner-join");
            this.features.add("advanced-math-expressions");
        }
    }
}
